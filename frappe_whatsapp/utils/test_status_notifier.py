"""Tests for frappe_whatsapp.utils.status_notifier.

Strategy
--------
- Use real DB records for WhatsApp Client App and WhatsApp Status Webhook
  Log (the two types this subsystem writes to directly).
- Use ``frappe._dict`` mock docs for WhatsApp Message arguments — this
  avoids triggering the outbound Meta API call in before_insert and bypasses
  read-only field restrictions while still exercising all notifier logic.
- Mock ``requests.post`` for all HTTP delivery assertions.
- Do NOT commit in tests; ``enqueue_after_commit=True`` jobs are never
  submitted to Redis, so we verify observable state (log records in DB)
  rather than enqueue invocations.
"""
import json
from typing import TYPE_CHECKING, cast
from unittest.mock import MagicMock, call, patch

import frappe
from frappe.tests.utils import FrappeTestCase
from frappe.utils import add_to_date, now_datetime

if TYPE_CHECKING:
    # Only used by pyright for attribute-access checking; not imported at
    # runtime to avoid import-order issues in the bench test runner.
    from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_client_app.whatsapp_client_app import (  # noqa: E501
        WhatsAppClientApp,
    )
    from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_status_webhook_log.whatsapp_status_webhook_log import (  # noqa: E501
        WhatsAppStatusWebhookLog,
    )
from frappe_whatsapp.utils.status_notifier import (
    MAX_RETRY_ATTEMPTS,
    STATUS_WEBHOOK_LOG_DOCTYPE,
    _build_event_id,
    _create_log_if_new,
    _is_material_change,
    _normalize_status,
    deliver_status_notification,
    maybe_enqueue_status_notification,
    on_whatsapp_message_after_insert,
    on_whatsapp_message_on_update,
    retry_failed_status_notifications,
)


# ── Fixtures ───────────────────────────────────────────────────────────────


def _make_client_app(
    *,
    status_webhook_url: str = "https://example.com/status",
    enabled: int = 1,
):
    suffix = frappe.generate_hash(length=8)
    return frappe.get_doc(
        {
            "doctype": "WhatsApp Client App",
            "app_id": f"status-test-app-{suffix}",
            "enabled": enabled,
            "inbound_webhook_url": "https://example.com/incoming",
            "status_webhook_url": status_webhook_url,
        }
    ).insert(ignore_permissions=True)


def _mock_msg(
    *,
    name: str | None = None,
    type: str = "Outgoing",
    status: str | None = "Success",
    source_app: str | None = None,
    message_id: str | None = None,
    external_reference: str | None = None,
    to: str = "+15551234567",
    whatsapp_account: str = "test-account",
    content_type: str = "text",
    conversation_id: str | None = None,
    status_error_code: str | None = None,
    status_error_title: str | None = None,
    status_error_message: str | None = None,
    status_error_details: str | None = None,
    status_error_href: str | None = None,
) -> frappe._dict:
    """Return a frappe._dict that looks like a WhatsApp Message document."""
    return frappe._dict(
        {
            "name": name or f"TEST-MSG-{frappe.generate_hash(length=8)}",
            "doctype": "WhatsApp Message",
            "type": type,
            "status": status,
            "source_app": source_app,
            "message_id": (message_id or
                           f"wamid.{frappe.generate_hash(length=16)}"),
            "external_reference": external_reference,
            "to": to,
            "whatsapp_account": whatsapp_account,
            "content_type": content_type,
            "conversation_id": conversation_id,
            "status_error_code": status_error_code,
            "status_error_title": status_error_title,
            "status_error_message": status_error_message,
            "status_error_details": status_error_details,
            "status_error_href": status_error_href,
        }
    )


def _make_log(
    *,
    message_name: str,
    source_app: str,
    current_status: str = "Success",
    previous_status: str = "",
    delivery_status: str = "Pending",
    attempts: int = 0,
    event_id: str | None = None,
    next_retry_at=None,
    claim_expires_at=None,
) -> "WhatsAppStatusWebhookLog":
    """Insert a WhatsApp Status Webhook Log directly
    for delivery/retry tests."""
    eid = event_id or _build_event_id(message_name, current_status)
    payload = {
        "event": "whatsapp.message_status",
        "event_id": eid,
        "occurred_at": str(now_datetime()),
        "app_id": "test-app",
        "message": {
            "name": message_name,
            "message_id": "",
            "external_reference": "",
            "source_app": source_app,
            "to": "",
            "whatsapp_account": "",
            "previous_status": previous_status,
            "current_status": current_status,
            "normalized_status": _normalize_status(current_status),
            "conversation_id": "",
            "content_type": "",
            "type": "Outgoing",
        },
    }
    return cast(
        "WhatsAppStatusWebhookLog",
        frappe.get_doc(
            {
                "doctype": STATUS_WEBHOOK_LOG_DOCTYPE,
                "message_name": message_name,
                "source_app": source_app,
                "event_id": eid,
                "delivery_status": delivery_status,
                "current_status": current_status,
                "previous_status": previous_status,
                "payload": json.dumps(payload),
                "attempts": attempts,
                "next_retry_at": next_retry_at,
                "claim_expires_at": claim_expires_at,
            }
        ).insert(ignore_permissions=True, ignore_links=True),
    )


def _count_logs(message_name: str, current_status: str | None = None) -> int:
    filters: dict = {"message_name": message_name}
    if current_status is not None:
        filters["current_status"] = current_status
    return frappe.db.count(STATUS_WEBHOOK_LOG_DOCTYPE, filters)


# ── Unit tests: helpers ────────────────────────────────────────────────────


class TestStatusNormalization(FrappeTestCase):
    def test_success_maps_to_accepted(self):
        self.assertEqual(_normalize_status("Success"), "accepted")

    def test_failed_maps_to_failed(self):
        self.assertEqual(_normalize_status("Failed"), "failed")

    def test_meta_statuses_pass_through(self):
        self.assertEqual(_normalize_status("sent"), "sent")
        self.assertEqual(_normalize_status("delivered"), "delivered")
        self.assertEqual(_normalize_status("read"), "read")

    def test_marked_as_read_maps_to_read(self):
        self.assertEqual(_normalize_status("marked as read"), "read")

    def test_none_maps_to_unknown(self):
        self.assertEqual(_normalize_status(None), "unknown")

    def test_unknown_value_lowercased(self):
        self.assertEqual(_normalize_status(
            "SomeWeirdStatus"), "someweirdstatus")


class TestMaterialChange(FrappeTestCase):
    def test_initial_insert_with_status_is_material(self):
        doc = _mock_msg(status="Success")
        changed, prev = _is_material_change(doc, None)
        self.assertTrue(changed)
        self.assertIsNone(prev)

    def test_initial_insert_without_status_is_not_material(self):
        doc = _mock_msg(status=None)
        changed, prev = _is_material_change(doc, None)
        self.assertFalse(changed)
        self.assertIsNone(prev)

    def test_status_change_is_material(self):
        prev = _mock_msg(status="sent")
        doc = _mock_msg(status="delivered")
        changed, prev_status = _is_material_change(doc, prev)
        self.assertTrue(changed)
        self.assertEqual(prev_status, "sent")

    def test_no_change_is_not_material(self):
        prev = _mock_msg(status="delivered")
        doc = _mock_msg(status="delivered")
        changed, _ = _is_material_change(doc, prev)
        self.assertFalse(changed)

    def test_failed_with_new_error_code_is_material(self):
        prev = _mock_msg(status="failed", status_error_code="131")
        doc = _mock_msg(status="failed", status_error_code="132")
        changed, _ = _is_material_change(doc, prev)
        self.assertTrue(changed)

    def test_failed_with_same_error_is_not_material(self):
        prev = _mock_msg(
            status="failed",
            status_error_code="131",
            status_error_message="msg",
            status_error_title="title",
            status_error_details="details",
            status_error_href="https://example.com/err",
        )
        doc = _mock_msg(
            status="failed",
            status_error_code="131",
            status_error_message="msg",
            status_error_title="title",
            status_error_details="details",
            status_error_href="https://example.com/err",
        )
        changed, _ = _is_material_change(doc, prev)
        self.assertFalse(changed)

    def test_failed_with_new_error_title_only_is_material(self):
        prev = _mock_msg(
            status="failed",
            status_error_code="131",
            status_error_title="Original",
        )
        doc = _mock_msg(
            status="failed",
            status_error_code="131",
            status_error_title="Enriched",
        )
        changed, _ = _is_material_change(doc, prev)
        self.assertTrue(changed)

    def test_failed_with_new_error_details_is_material(self):
        prev = _mock_msg(status="failed", status_error_code="131")
        doc = _mock_msg(
            status="failed",
            status_error_code="131",
            status_error_details="Additional context",
        )
        changed, _ = _is_material_change(doc, prev)
        self.assertTrue(changed)

    def test_failed_with_new_error_href_is_material(self):
        prev = _mock_msg(status="failed", status_error_code="131")
        doc = _mock_msg(
            status="failed",
            status_error_code="131",
            status_error_href="https://developers.facebook.com/131",
        )
        changed, _ = _is_material_change(doc, prev)
        self.assertTrue(changed)

    def test_incoming_type_is_ignored(self):
        """_is_material_change doesn't gate on type; callers do."""
        doc = _mock_msg(type="Incoming", status="read")
        changed, _ = _is_material_change(doc, None)
        self.assertTrue(changed)  # still detected, but callers filter type


# ── Tests: notification enqueueing / log creation ─────────────────────────


class TestMaybeEnqueueStatusNotification(FrappeTestCase):
    """Test outbox log creation via maybe_enqueue_status_notification and
    the two doc_events handler functions."""

    def setUp(self):
        self.app = _make_client_app()

    # ── Basic routing ──────────────────────────────────────────────────

    def test_initial_success_creates_pending_log(self):
        doc = _mock_msg(source_app=str(self.app.name), status="Success")
        maybe_enqueue_status_notification(doc, previous_status=None)

        log_name = frappe.db.get_value(
            STATUS_WEBHOOK_LOG_DOCTYPE,
            {"message_name": doc.name, "current_status": "Success"},
            "name",
        )
        self.assertIsNotNone(log_name,
                             "Expected a Pending log after initial insert")
        log = cast("WhatsAppStatusWebhookLog",
                   frappe.get_doc(STATUS_WEBHOOK_LOG_DOCTYPE, str(log_name)))
        self.assertEqual(log.delivery_status, "Pending")
        self.assertEqual(log.source_app, self.app.name)
        self.assertEqual(log.previous_status, "")

    def test_meta_status_update_creates_new_log(self):
        doc = _mock_msg(source_app=str(self.app.name), status="delivered")
        maybe_enqueue_status_notification(doc, previous_status="sent")

        log_name = frappe.db.get_value(
            STATUS_WEBHOOK_LOG_DOCTYPE,
            {"message_name": doc.name, "current_status": "delivered"},
            "name",
        )
        self.assertIsNotNone(log_name)
        log = cast("WhatsAppStatusWebhookLog",
                   frappe.get_doc(STATUS_WEBHOOK_LOG_DOCTYPE, str(log_name)))
        self.assertEqual(log.previous_status, "sent")

    # ── Skip conditions ────────────────────────────────────────────────

    def test_no_notification_when_source_app_missing(self):
        doc = _mock_msg(source_app=None, status="Success")
        before = frappe.db.count(STATUS_WEBHOOK_LOG_DOCTYPE)
        maybe_enqueue_status_notification(doc, previous_status=None)
        self.assertEqual(frappe.db.count(STATUS_WEBHOOK_LOG_DOCTYPE), before)

    def test_no_notification_when_status_webhook_url_empty(self):
        app_no_url = _make_client_app(status_webhook_url="")
        doc = _mock_msg(source_app=str(app_no_url.name), status="Success")
        before = frappe.db.count(STATUS_WEBHOOK_LOG_DOCTYPE)
        maybe_enqueue_status_notification(doc, previous_status=None)
        self.assertEqual(frappe.db.count(STATUS_WEBHOOK_LOG_DOCTYPE), before)

    def test_no_notification_when_app_disabled(self):
        disabled = _make_client_app(enabled=0)
        doc = _mock_msg(source_app=str(disabled.name), status="Success")
        before = frappe.db.count(STATUS_WEBHOOK_LOG_DOCTYPE)
        maybe_enqueue_status_notification(doc, previous_status=None)
        self.assertEqual(frappe.db.count(STATUS_WEBHOOK_LOG_DOCTYPE), before)

    def test_no_notification_for_incoming_type(self):
        doc = _mock_msg(type="Incoming", source_app=str(self.app.name),
                        status="read")
        before = frappe.db.count(STATUS_WEBHOOK_LOG_DOCTYPE)
        on_whatsapp_message_after_insert(doc)
        self.assertEqual(frappe.db.count(STATUS_WEBHOOK_LOG_DOCTYPE), before)

    def test_no_notification_when_status_absent(self):
        doc = _mock_msg(source_app=str(self.app.name), status=None)
        before = frappe.db.count(STATUS_WEBHOOK_LOG_DOCTYPE)
        on_whatsapp_message_after_insert(doc)
        self.assertEqual(frappe.db.count(STATUS_WEBHOOK_LOG_DOCTYPE), before)

    # ── Deduplication ──────────────────────────────────────────────────

    def test_duplicate_call_same_status_creates_only_one_log(self):
        doc = _mock_msg(source_app=str(self.app.name), status="sent")
        maybe_enqueue_status_notification(doc, previous_status=None)
        # duplicate
        maybe_enqueue_status_notification(doc, previous_status=None)

        self.assertEqual(_count_logs(str(doc.name), "sent"), 1)

    def test_different_status_creates_second_log(self):
        doc = _mock_msg(source_app=str(self.app.name), status="sent")
        maybe_enqueue_status_notification(doc, previous_status=None)

        doc.status = "delivered"
        maybe_enqueue_status_notification(doc, previous_status="sent")

        self.assertEqual(_count_logs(str(doc.name), "sent"), 1)
        self.assertEqual(_count_logs(str(doc.name), "delivered"), 1)

    # ── on_update skips initial insert ─────────────────────────────────

    def test_on_update_skips_when_previous_doc_is_none(self):
        """If previous_doc is None (initial insert), on_update is a no-op."""
        doc = _mock_msg(source_app=str(self.app.name), status="Success")
        doc.get_doc_before_save = lambda: None  # simulate initial insert

        before = frappe.db.count(STATUS_WEBHOOK_LOG_DOCTYPE)
        on_whatsapp_message_on_update(doc)
        self.assertEqual(frappe.db.count(STATUS_WEBHOOK_LOG_DOCTYPE), before)

    def test_on_update_creates_log_on_status_change(self):
        prev_doc = _mock_msg(status="sent")
        doc = _mock_msg(
            source_app=str(self.app.name),
            status="delivered",
        )
        doc.get_doc_before_save = lambda: prev_doc

        on_whatsapp_message_on_update(doc)

        log_name = frappe.db.get_value(
            STATUS_WEBHOOK_LOG_DOCTYPE,
            {"message_name": doc.name, "current_status": "delivered"},
            "name",
        )
        self.assertIsNotNone(log_name)

    def test_on_update_skips_when_status_unchanged(self):
        prev_doc = _mock_msg(status="delivered")
        doc = _mock_msg(source_app=str(self.app.name), status="delivered")
        doc.get_doc_before_save = lambda: prev_doc

        before = frappe.db.count(STATUS_WEBHOOK_LOG_DOCTYPE)
        on_whatsapp_message_on_update(doc)
        self.assertEqual(frappe.db.count(STATUS_WEBHOOK_LOG_DOCTYPE), before)

    # ── Error details in payload ───────────────────────────────────────

    def test_failed_status_payload_includes_error_block(self):
        doc = _mock_msg(
            source_app=str(self.app.name),
            status="failed",
            status_error_code="131026",
            status_error_title="Message Undeliverable",
            status_error_message=("Message failed to send because"
                                  " the recipient opted out."),
        )
        maybe_enqueue_status_notification(doc, previous_status="sent")

        log_name = frappe.db.get_value(
            STATUS_WEBHOOK_LOG_DOCTYPE,
            {"message_name": doc.name, "current_status": "failed"},
            "name",
        )
        self.assertIsNotNone(log_name)
        log = cast("WhatsAppStatusWebhookLog",
                   frappe.get_doc(STATUS_WEBHOOK_LOG_DOCTYPE, str(log_name)))
        payload = json.loads(str(log.payload))

        self.assertIn("error", payload)
        self.assertEqual(payload["error"]["code"], "131026")
        self.assertEqual(payload["error"]["title"], "Message Undeliverable")
        self.assertIn("opted out", payload["error"]["message"])

    def test_enriched_failed_error_creates_new_log(self):
        """A new error code on an already-failed message → new log entry."""
        doc = _mock_msg(
            source_app=str(self.app.name),
            status="failed",
            status_error_code="131026",
        )
        maybe_enqueue_status_notification(doc, previous_status="sent")

        # Simulate enriched callback with different error code.
        doc.status_error_code = "131047"
        doc.status_error_title = "Re-engagement Message"
        maybe_enqueue_status_notification(doc, previous_status="failed")

        # Two distinct event_ids → two log entries.
        self.assertEqual(_count_logs(str(doc.name), "failed"), 2)

    def test_same_error_code_new_title_creates_new_log(self):
        """Same error_code + new error_title → distinct event → new log."""
        doc = _mock_msg(
            source_app=str(self.app.name),
            status="failed",
            status_error_code="131026",
            status_error_title="Original Title",
        )
        maybe_enqueue_status_notification(doc, previous_status="sent")

        doc.status_error_title = "Enriched Title"
        maybe_enqueue_status_notification(doc, previous_status="failed")

        self.assertEqual(_count_logs(str(doc.name), "failed"), 2)

    def test_same_error_code_new_details_creates_new_log(self):
        """Same code/title/message + new error_details → new log."""
        doc = _mock_msg(
            source_app=str(self.app.name),
            status="failed",
            status_error_code="131026",
            status_error_title="Title",
            status_error_message="Msg",
        )
        maybe_enqueue_status_notification(doc, previous_status="sent")

        doc.status_error_details = "Supplementary details"
        maybe_enqueue_status_notification(doc, previous_status="failed")

        self.assertEqual(_count_logs(str(doc.name), "failed"), 2)

    def test_true_duplicate_failed_callback_dedupes(self):
        """Identical failed callbacks — all error fields same → one log."""
        doc = _mock_msg(
            source_app=str(self.app.name),
            status="failed",
            status_error_code="131026",
            status_error_title="Title",
            status_error_message="Msg",
            status_error_details="Details",
            status_error_href="https://developers.facebook.com/131026",
        )
        maybe_enqueue_status_notification(doc, previous_status="sent")
        # Exact same callback replayed — must dedupe.
        maybe_enqueue_status_notification(doc, previous_status="failed")

        self.assertEqual(_count_logs(str(doc.name), "failed"), 1)

    def test_duplicate_event_collision_is_clean_noop(self):
        """Race-safe: concurrent duplicate insert is a no-op, not an error.

        Simulates the scenario where two workers both pass any fast-path
        check and race to insert the same event_id.  The unique constraint
        must absorb the collision without raising to the caller.
        """
        doc = _mock_msg(source_app=str(self.app.name), status="sent")
        app_doc = frappe.get_doc("WhatsApp Client App", str(self.app.name))

        # First insert — must succeed and return a log name.
        log_name_1 = _create_log_if_new(doc, None, app_doc)
        self.assertIsNotNone(log_name_1)

        # Second insert with identical args — constraint collision.
        # Must return None cleanly, not raise.
        log_name_2 = _create_log_if_new(doc, None, app_doc)
        self.assertIsNone(log_name_2)

        # Exactly one log row exists.
        self.assertEqual(_count_logs(str(doc.name), "sent"), 1)

    def test_payload_normalized_status_is_stable(self):
        for raw, expected in [
            ("Success", "accepted"),
            ("Failed", "failed"),
            ("sent", "sent"),
            ("delivered", "delivered"),
            ("read", "read"),
            ("marked as read", "read"),
        ]:
            doc = _mock_msg(
                source_app=str(self.app.name),
                status=raw,
            )
            maybe_enqueue_status_notification(doc, previous_status=None)
            log_name = frappe.db.get_value(
                STATUS_WEBHOOK_LOG_DOCTYPE,
                {"message_name": doc.name, "current_status": raw},
                "name",
            )
            self.assertIsNotNone(log_name, f"No log for status={raw!r}")
            log = cast(
                "WhatsAppStatusWebhookLog",
                frappe.get_doc(STATUS_WEBHOOK_LOG_DOCTYPE, str(log_name)),
            )
            payload = json.loads(str(log.payload))
            self.assertEqual(
                payload["message"]["normalized_status"],
                expected,
                f"Expected normalized_status={expected!r} for raw={raw!r}",
            )

    def test_payload_includes_external_reference(self):
        doc = _mock_msg(
            source_app=str(self.app.name),
            status="sent",
            external_reference="client-ref-abc123",
        )
        maybe_enqueue_status_notification(doc, previous_status=None)

        log_name = frappe.db.get_value(
            STATUS_WEBHOOK_LOG_DOCTYPE,
            {"message_name": doc.name},
            "name",
        )
        payload = json.loads(str(
            cast(
                "WhatsAppStatusWebhookLog",
                frappe.get_doc(STATUS_WEBHOOK_LOG_DOCTYPE, str(log_name)),
            ).payload
        ))
        self.assertEqual(
            payload["message"]["external_reference"], "client-ref-abc123"
        )


# ── Tests: webhook HTTP delivery ──────────────────────────────────────────


class TestDeliverStatusNotification(FrappeTestCase):
    """Test the HTTP delivery step using mocked requests.post."""

    def setUp(self):
        self.app = _make_client_app()
        self.msg_name = f"DLVR-MSG-{frappe.generate_hash(length=8)}"

    def tearDown(self):
        # Remove any Status Webhook Logs that may have been committed to DB
        # (frappe.db.set_value inside deliver_status_notification may commit).
        frappe.db.delete(
            STATUS_WEBHOOK_LOG_DOCTYPE, {"message_name": self.msg_name}
        )
        frappe.db.commit()

    def _log(self, **kwargs) -> "WhatsAppStatusWebhookLog":
        return _make_log(
            message_name=self.msg_name,
            source_app=str(self.app.name),
            **kwargs,
        )

    @patch("frappe_whatsapp.utils.status_notifier.requests.post")
    def test_successful_delivery_marks_log_delivered(self, mock_post):
        mock_post.return_value = MagicMock(
            ok=True, status_code=200, text='{"ok":true}')

        log = self._log()
        deliver_status_notification(str(log.name))

        log.reload()
        self.assertEqual(log.delivery_status, "Delivered")
        self.assertEqual(log.response_code, "200")
        self.assertEqual(log.attempts, 1)
        self.assertFalse(log.error)
        self.assertIsNone(log.next_retry_at)

    @patch("frappe_whatsapp.utils.status_notifier.requests.post")
    def test_failed_http_marks_log_failed(self, mock_post):
        mock_post.return_value = MagicMock(
            ok=False, status_code=500, text="Internal Server Error"
        )

        log = self._log()
        deliver_status_notification(str(log.name))

        log.reload()
        self.assertEqual(log.delivery_status, "Failed")
        self.assertEqual(log.response_code, "500")
        self.assertEqual(log.attempts, 1)
        self.assertIn("500", str(log.error))
        self.assertIsNotNone(log.next_retry_at)

    @patch("frappe_whatsapp.utils.status_notifier.requests.post")
    def test_network_exception_marks_log_failed(self, mock_post):
        mock_post.side_effect = ConnectionError("unreachable")

        log = self._log()
        deliver_status_notification(str(log.name))

        log.reload()
        self.assertEqual(log.delivery_status, "Failed")
        self.assertEqual(log.attempts, 1)
        self.assertTrue(log.error)
        self.assertIsNotNone(log.next_retry_at)

    @patch("frappe_whatsapp.utils.status_notifier.requests.post")
    def test_posts_to_status_webhook_url_with_correct_headers(self, mock_post):
        mock_post.return_value = MagicMock(ok=True, status_code=200, text="")

        log = self._log()
        deliver_status_notification(str(log.name))

        mock_post.assert_called_once()
        typed_app = cast("WhatsAppClientApp", self.app)
        call = mock_post.call_args
        self.assertEqual(call.args[0], typed_app.status_webhook_url)
        headers = call.kwargs["headers"]
        self.assertEqual(headers["X-WhatsApp-App-ID"], typed_app.app_id)
        self.assertEqual(headers["X-Event-ID"], log.event_id)
        self.assertEqual(headers["Content-Type"], "application/json")

    @patch("frappe_whatsapp.utils.status_notifier.requests.post")
    def test_already_delivered_is_idempotent_noop(self, mock_post):
        mock_post.return_value = MagicMock(ok=True, status_code=200, text="")

        log = self._log(delivery_status="Delivered", attempts=1)
        deliver_status_notification(str(log.name))

        mock_post.assert_not_called()

    @patch("frappe_whatsapp.utils.status_notifier.requests.post")
    def test_skips_when_app_disabled_after_log_creation(self, mock_post):
        frappe.db.set_value(
            "WhatsApp Client App", self.app.name, "enabled", 0
        )

        log = self._log()
        deliver_status_notification(str(log.name))

        log.reload()
        self.assertEqual(log.delivery_status, "Skipped")
        mock_post.assert_not_called()

    @patch("frappe_whatsapp.utils.status_notifier.requests.post")
    def test_payload_shape_matches_contract(self, mock_post):
        mock_post.return_value = MagicMock(ok=True, status_code=200, text="")

        log = self._log(current_status="delivered", previous_status="sent")
        deliver_status_notification(str(log.name))

        body = mock_post.call_args.kwargs["json"]
        self.assertEqual(body["event"], "whatsapp.message_status")
        self.assertIn("event_id", body)
        self.assertIn("occurred_at", body)
        self.assertIn("app_id", body)
        msg = body["message"]
        self.assertEqual(msg["name"], self.msg_name)
        self.assertEqual(msg["type"], "Outgoing")
        self.assertIn("normalized_status", msg)
        self.assertIn("current_status", msg)
        self.assertIn("previous_status", msg)

    @patch("frappe_whatsapp.utils.status_notifier.requests.post")
    def test_increments_attempts_on_retry(self, mock_post):
        mock_post.return_value = MagicMock(
            ok=False, status_code=503, text="Service Unavailable"
        )

        log = self._log(delivery_status="Failed", attempts=1)
        deliver_status_notification(str(log.name))

        log.reload()
        self.assertEqual(log.attempts, 2)
        self.assertEqual(log.delivery_status, "Failed")

    @patch("frappe_whatsapp.utils.status_notifier.requests.post")
    def test_processing_claim_prevents_duplicate_post(self, mock_post):
        """A log with a valid (future) claim is not re-posted."""
        future = add_to_date(now_datetime(), minutes=4)
        log = self._log(
            delivery_status="Processing",
            attempts=0,
            claim_expires_at=future,
        )
        deliver_status_notification(str(log.name))

        mock_post.assert_not_called()

    @patch("frappe_whatsapp.utils.status_notifier.requests.post")
    def test_null_claim_processing_is_reclaimable(self, mock_post):
        """A Processing log with NULL claim_expires_at is re-claimed and
        delivered (NULL is treated as an immediately-expired lease)."""
        mock_post.return_value = MagicMock(ok=True, status_code=200, text="")
        log = self._log(delivery_status="Processing", attempts=0)
        deliver_status_notification(str(log.name))

        mock_post.assert_called_once()
        log.reload()
        self.assertEqual(log.delivery_status, "Delivered")

    @patch("frappe_whatsapp.utils.status_notifier.requests.post")
    def test_recovery_reenqueue_after_success_is_noop(self, mock_post):
        """Recovery re-enqueue overlapping a completed delivery posts once.

        Simulates: initial job delivers successfully, then the scheduler
        re-enqueues the same log (it was still Pending when the scheduler
        ran).  The second deliver call must skip — the row is Delivered.
        """
        mock_post.return_value = MagicMock(ok=True, status_code=200, text="")

        log = self._log()
        deliver_status_notification(str(log.name))  # initial — succeeds
        deliver_status_notification(str(log.name))  # recovery — must skip

        mock_post.assert_called_once()

    @patch("frappe_whatsapp.utils.status_notifier.requests.post")
    def test_fresh_processing_claim_blocks_concurrent_delivery(
        self, mock_post
    ):
        """A log with a valid (future) claim cannot be re-claimed."""
        future = add_to_date(now_datetime(), minutes=4)
        log = self._log(
            delivery_status="Processing",
            attempts=0,
            claim_expires_at=future,
        )
        deliver_status_notification(str(log.name))

        mock_post.assert_not_called()

    @patch("frappe_whatsapp.utils.status_notifier.requests.post")
    def test_stale_processing_is_reclaimed_and_delivered(self, mock_post):
        """A log with an expired claim is re-claimed and delivered."""
        mock_post.return_value = MagicMock(ok=True, status_code=200, text="")
        past = add_to_date(now_datetime(), minutes=-10)
        log = self._log(
            delivery_status="Processing",
            attempts=0,
            claim_expires_at=past,
        )
        deliver_status_notification(str(log.name))

        mock_post.assert_called_once()
        log.reload()
        self.assertEqual(log.delivery_status, "Delivered")


# ── Tests: retry scheduler ────────────────────────────────────────────────


class TestRetryScheduler(FrappeTestCase):
    def setUp(self):
        # Purge stale logs left by earlier runs so the global retry query
        # starts from a clean slate (Failed, Pending, and Processing rows
        # are all picked up or excluded by the scheduler).
        for status in ("Failed", "Pending", "Processing"):
            frappe.db.delete(
                STATUS_WEBHOOK_LOG_DOCTYPE, {"delivery_status": status}
            )
        frappe.db.commit()
        self.app = _make_client_app()
        self.msg_name = f"RETRY-MSG-{frappe.generate_hash(length=8)}"

    def tearDown(self):
        frappe.db.delete(
            STATUS_WEBHOOK_LOG_DOCTYPE, {"message_name": self.msg_name}
        )
        frappe.db.commit()

    @patch("frappe_whatsapp.utils.status_notifier.frappe.enqueue")
    def test_retry_enqueues_due_failed_logs(self, mock_enqueue):
        log = _make_log(
            message_name=self.msg_name,
            source_app=str(self.app.name),
            delivery_status="Failed",
            attempts=1,
            # next_retry_at=None → due immediately
        )
        retry_failed_status_notifications()

        mock_enqueue.assert_called_once_with(
            ("frappe_whatsapp.utils.status_notifier."
             "deliver_status_notification"),
            queue="short",
            log_name=log.name,
        )

    @patch("frappe_whatsapp.utils.status_notifier.frappe.enqueue")
    def test_retry_skips_logs_not_yet_due(self, mock_enqueue):
        future = add_to_date(now_datetime(), hours=2)
        _make_log(
            message_name=self.msg_name,
            source_app=str(self.app.name),
            delivery_status="Failed",
            attempts=1,
            next_retry_at=future,
        )
        retry_failed_status_notifications()

        mock_enqueue.assert_not_called()

    @patch("frappe_whatsapp.utils.status_notifier.frappe.enqueue")
    def test_retry_skips_exhausted_logs(self, mock_enqueue):
        _make_log(
            message_name=self.msg_name,
            source_app=str(self.app.name),
            delivery_status="Failed",
            attempts=MAX_RETRY_ATTEMPTS,  # at or above ceiling
        )
        retry_failed_status_notifications()

        mock_enqueue.assert_not_called()

    @patch("frappe_whatsapp.utils.status_notifier.frappe.enqueue")
    def test_retry_skips_delivered_logs(self, mock_enqueue):
        _make_log(
            message_name=self.msg_name,
            source_app=str(self.app.name),
            delivery_status="Delivered",
            attempts=1,
        )
        retry_failed_status_notifications()

        mock_enqueue.assert_not_called()

    @patch("frappe_whatsapp.utils.status_notifier.frappe.enqueue")
    def test_retry_recovers_stale_pending_log(self, mock_enqueue):
        """A Pending log past its next_retry_at deadline is re-enqueued."""
        past = add_to_date(now_datetime(), minutes=-20)
        log = _make_log(
            message_name=self.msg_name,
            source_app=str(self.app.name),
            delivery_status="Pending",
            attempts=0,
            next_retry_at=past,
        )
        retry_failed_status_notifications()

        mock_enqueue.assert_called_once_with(
            (
                "frappe_whatsapp.utils.status_notifier."
                "deliver_status_notification"
            ),
            queue="short",
            log_name=log.name,
        )

    @patch("frappe_whatsapp.utils.status_notifier.frappe.enqueue")
    def test_retry_skips_pending_not_yet_due(self, mock_enqueue):
        """A Pending log whose next_retry_at is in the future is skipped."""
        future = add_to_date(now_datetime(), hours=1)
        _make_log(
            message_name=self.msg_name,
            source_app=str(self.app.name),
            delivery_status="Pending",
            attempts=0,
            next_retry_at=future,
        )
        retry_failed_status_notifications()

        mock_enqueue.assert_not_called()

    @patch("frappe_whatsapp.utils.status_notifier.frappe.enqueue")
    def test_retry_recovers_stale_processing_log(self, mock_enqueue):
        """A Processing log with an expired claim is re-enqueued."""
        past = add_to_date(now_datetime(), minutes=-10)
        log = _make_log(
            message_name=self.msg_name,
            source_app=str(self.app.name),
            delivery_status="Processing",
            attempts=0,
            claim_expires_at=past,
        )
        retry_failed_status_notifications()

        mock_enqueue.assert_called_once_with(
            (
                "frappe_whatsapp.utils.status_notifier."
                "deliver_status_notification"
            ),
            queue="short",
            log_name=log.name,
        )

    @patch("frappe_whatsapp.utils.status_notifier.frappe.enqueue")
    def test_retry_recovers_null_claim_processing_log(self, mock_enqueue):
        """A Processing log with NULL claim_expires_at is treated as stale
        and re-enqueued by the scheduler."""
        log = _make_log(
            message_name=self.msg_name,
            source_app=str(self.app.name),
            delivery_status="Processing",
            attempts=0,
            # claim_expires_at=None (default) — simulates a row stuck in
            # Processing without a lease stamp
        )
        retry_failed_status_notifications()

        mock_enqueue.assert_called_once_with(
            (
                "frappe_whatsapp.utils.status_notifier."
                "deliver_status_notification"
            ),
            queue="short",
            log_name=log.name,
        )

    @patch("frappe_whatsapp.utils.status_notifier.frappe.enqueue")
    def test_retry_skips_fresh_processing_log(self, mock_enqueue):
        """A Processing log with a valid (future) claim is not enqueued."""
        future = add_to_date(now_datetime(), minutes=3)
        _make_log(
            message_name=self.msg_name,
            source_app=str(self.app.name),
            delivery_status="Processing",
            attempts=0,
            claim_expires_at=future,
        )
        retry_failed_status_notifications()

        mock_enqueue.assert_not_called()


# ── Tests: index maintenance ───────────────────────────────────────────────


class TestEnsureStatusLogIndexes(FrappeTestCase):
    @patch("frappe_whatsapp.utils.status_notifier.frappe.db.add_index")
    @patch("frappe_whatsapp.utils.status_notifier.frappe.db.has_index")
    def test_ensure_indexes_uses_db_helper_for_missing_indexes(
        self,
        mock_has_index,
        mock_add_index,
    ):
        """Missing indexes are created via frappe.db.add_index()."""
        from frappe_whatsapp.utils.status_notifier import (
            ensure_status_log_indexes,
        )

        table = f"tab{STATUS_WEBHOOK_LOG_DOCTYPE}"
        mock_has_index.side_effect = [False, False]

        ensure_status_log_indexes()

        self.assertEqual(
            mock_has_index.call_args_list,
            [
                call(table, "idx_status_retry_scan"),
                call(table, "idx_status_claim_scan"),
            ],
        )
        self.assertEqual(
            mock_add_index.call_args_list,
            [
                call(
                    STATUS_WEBHOOK_LOG_DOCTYPE,
                    ["delivery_status", "attempts", "next_retry_at"],
                    index_name="idx_status_retry_scan",
                ),
                call(
                    STATUS_WEBHOOK_LOG_DOCTYPE,
                    ["delivery_status", "attempts", "claim_expires_at"],
                    index_name="idx_status_claim_scan",
                ),
            ],
        )

    def test_ensure_indexes_is_idempotent(self):
        """ensure_status_log_indexes() is a no-op when indexes exist, and
        both indexes have the correct column definitions."""
        from frappe_whatsapp.utils.status_notifier import (
            ensure_status_log_indexes,
        )

        # Should not raise on first or repeated calls.
        ensure_status_log_indexes()
        ensure_status_log_indexes()

        table = f"tab{STATUS_WEBHOOK_LOG_DOCTYPE}"
        expected = {
            "idx_status_retry_scan": [
                "delivery_status", "attempts", "next_retry_at"
            ],
            "idx_status_claim_scan": [
                "delivery_status", "attempts", "claim_expires_at"
            ],
        }
        for index_name, expected_cols in expected.items():
            # SHOW INDEX columns (0-based): 2=Key_name, 3=Seq_in_index,
            # 4=Column_name.  ORDER BY is not supported by MariaDB's SHOW
            # INDEX syntax, so sort the result tuples in Python.
            rows = frappe.db.sql(
                "SHOW INDEX FROM `{0}`"
                " WHERE Key_name = '{1}'".format(table, index_name),
            )
            rows = sorted(rows, key=lambda r: r[3])
            self.assertGreater(
                len(rows), 0,
                f"Expected index {index_name!r} to exist",
            )
            actual_cols = [row[4] for row in rows]
            self.assertEqual(
                actual_cols,
                expected_cols,
                f"Index {index_name!r} column order mismatch",
            )
