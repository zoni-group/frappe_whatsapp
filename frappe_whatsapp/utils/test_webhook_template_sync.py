"""Tests for automatic template sync triggered by Meta webhook events.

Covers five layers:
  1. update_status() unit tests — correct dispatch and enqueue args.
  2. Deduplication contract — deduplicate=True + stable job_id required.
  3. _is_trusted_waba_id() unit tests — correct per-ID trust decisions.
  4. process_webhook_payload() integration — both list-shaped and dict-shaped
     entry payloads, trusted and untrusted WABA IDs, and the critical check
     that a trusted *later* entry cannot authorize an untrusted *first* entry.
  5. Non-template paths (message status) must not enqueue sync.

Meta webhook field coverage note
---------------------------------
  message_template_status_update  — APPROVED/REJECTED/PENDING after edits
  message_template_quality_update — quality-score changes (HIGH/MEDIUM/LOW)
  template_category_update        — Meta category reclassification

When an operator edits template content in Business Manager, Meta sets the
status to PENDING and emits message_template_status_update.  There is no
separate "content_changed" field.  All three fields above are handled.

Payload shape note
------------------
Meta sends:
  list-shaped: data["entry"] = [{"id": "...", "changes": [...]}, ...]
  dict-shaped: data["entry"] = {"id": "...", "changes": [...]}
Both shapes are normalized to a list internally and must work identically.
"""

from unittest.mock import patch

import frappe
from frappe.tests.utils import FrappeTestCase

from frappe_whatsapp.utils.webhook import (
    _is_trusted_waba_id,
    process_webhook_payload,
    update_status,
)

# ---------------------------------------------------------------------------
# Shared constants
# ---------------------------------------------------------------------------

_WABA_ID = "123456789"
_UNKNOWN_WABA_ID = "evil-unknown-999"

# ---------------------------------------------------------------------------
# Fixtures — list-shaped entry (standard Meta shape)
# ---------------------------------------------------------------------------

_TEMPLATE_STATUS_PAYLOAD = {
    "object": "whatsapp_business_account",
    "entry": [{
        "id": _WABA_ID,
        "changes": [{
            "value": {
                "event": "APPROVED",
                "message_template_id": 594425479261596,
                "message_template_name": "my_sample_template",
                "message_template_language": "en_US",
                "reason": "None",
            },
            "field": "message_template_status_update",
        }],
    }],
}

_TEMPLATE_QUALITY_PAYLOAD = {
    "object": "whatsapp_business_account",
    "entry": [{
        "id": _WABA_ID,
        "changes": [{
            "value": {
                "message_template_id": 594425479261596,
                "message_template_quality": "HIGH",
            },
            "field": "message_template_quality_update",
        }],
    }],
}

_TEMPLATE_CATEGORY_PAYLOAD = {
    "object": "whatsapp_business_account",
    "entry": [{
        "id": _WABA_ID,
        "changes": [{
            "value": {
                "message_template_id": 594425479261596,
                "previous_category": "UTILITY",
                "new_category": "MARKETING",
            },
            "field": "template_category_update",
        }],
    }],
}

_MESSAGE_STATUS_PAYLOAD = {
    "object": "whatsapp_business_account",
    "entry": [{
        "id": _WABA_ID,
        "changes": [{
            "value": {
                "messaging_product": "whatsapp",
                "metadata": {
                    "display_phone_number": "15551234567",
                    "phone_number_id": "987654321",
                },
                "statuses": [{
                    "id": "wamid.abc123",
                    "status": "delivered",
                    "timestamp": "1234567890",
                    "recipient_id": "15559876543",
                }],
            },
            "field": "messages",
        }],
    }],
}

# ---------------------------------------------------------------------------
# Fixtures — dict-shaped entry (alternative Meta shape)
# ---------------------------------------------------------------------------

_TEMPLATE_STATUS_DICT_PAYLOAD = {
    "object": "whatsapp_business_account",
    "entry": {                          # dict, not list
        "id": _WABA_ID,
        "changes": [{
            "value": {
                "event": "APPROVED",
                "message_template_id": 594425479261596,
                "message_template_name": "my_sample_template",
                "message_template_language": "en_US",
                "reason": "None",
            },
            "field": "message_template_status_update",
        }],
    },
}

_TEMPLATE_STATUS_DICT_UNTRUSTED_PAYLOAD = {
    "object": "whatsapp_business_account",
    "entry": {
        "id": _UNKNOWN_WABA_ID,
        "changes": [{
            "value": {
                "event": "APPROVED",
                "message_template_id": 999,
            },
            "field": "message_template_status_update",
        }],
    },
}

# ---------------------------------------------------------------------------
# Helper assertion
# ---------------------------------------------------------------------------

def _assert_sync_enqueue(test_case, mock_enqueue):
    """Assert exactly one sync enqueue with the required dedup args."""
    mock_enqueue.assert_called_once()
    kwargs = mock_enqueue.call_args.kwargs
    test_case.assertEqual(kwargs["job_id"], "whatsapp_template_sync")
    test_case.assertTrue(
        kwargs.get("deduplicate"),
        "deduplicate=True must be passed so RQ suppresses redundant jobs",
    )
    test_case.assertEqual(
        mock_enqueue.call_args.args[0],
        "frappe_whatsapp.frappe_whatsapp.doctype."
        "whatsapp_templates.whatsapp_templates.fetch",
    )


# ===========================================================================
# 1. update_status() dispatch — unit tests
# ===========================================================================

class TestUpdateStatusDispatch(FrappeTestCase):

    @patch("frappe_whatsapp.utils.webhook.frappe.enqueue")
    @patch("frappe_whatsapp.utils.webhook.update_template_status")
    def test_template_status_update_calls_local_update_and_enqueues_sync(
        self, mock_update_template_status, mock_enqueue
    ):
        changes = _TEMPLATE_STATUS_PAYLOAD["entry"][0]["changes"][0]
        update_status(changes)
        mock_update_template_status.assert_called_once_with(changes["value"])
        _assert_sync_enqueue(self, mock_enqueue)

    @patch("frappe_whatsapp.utils.webhook.frappe.enqueue")
    def test_template_quality_update_enqueues_sync(self, mock_enqueue):
        update_status(_TEMPLATE_QUALITY_PAYLOAD["entry"][0]["changes"][0])
        _assert_sync_enqueue(self, mock_enqueue)

    @patch("frappe_whatsapp.utils.webhook.frappe.enqueue")
    def test_template_category_update_enqueues_sync(self, mock_enqueue):
        """template_category_update must trigger sync so the local record
        reflects the new category."""
        update_status(_TEMPLATE_CATEGORY_PAYLOAD["entry"][0]["changes"][0])
        _assert_sync_enqueue(self, mock_enqueue)

    @patch("frappe_whatsapp.utils.webhook.frappe.enqueue")
    @patch("frappe_whatsapp.utils.webhook.update_message_status")
    def test_message_field_does_not_enqueue_sync(
        self, mock_update_message_status, mock_enqueue
    ):
        update_status(_MESSAGE_STATUS_PAYLOAD["entry"][0]["changes"][0])
        mock_update_message_status.assert_called_once()
        mock_enqueue.assert_not_called()

    @patch("frappe_whatsapp.utils.webhook.frappe.enqueue")
    def test_unknown_field_does_not_enqueue_sync(self, mock_enqueue):
        update_status({"field": "account_alerts", "value": {"x": 1}})
        mock_enqueue.assert_not_called()


# ===========================================================================
# 2. Deduplication contract
# ===========================================================================

class TestTemplateSyncDeduplication(FrappeTestCase):

    @patch("frappe_whatsapp.utils.webhook.frappe.enqueue")
    @patch("frappe_whatsapp.utils.webhook.update_template_status")
    def test_burst_all_calls_carry_deduplicate_true(
        self, _mock_update_template_status, mock_enqueue
    ):
        changes = _TEMPLATE_STATUS_PAYLOAD["entry"][0]["changes"][0]
        for _ in range(5):
            update_status(changes)
        self.assertEqual(mock_enqueue.call_count, 5)
        for c in mock_enqueue.call_args_list:
            self.assertEqual(c.kwargs["job_id"], "whatsapp_template_sync")
            self.assertTrue(c.kwargs.get("deduplicate"))

    @patch("frappe_whatsapp.utils.webhook.frappe.enqueue")
    @patch("frappe_whatsapp.utils.webhook.update_template_status")
    def test_stable_job_id_across_all_template_fields(
        self, _mock_update_template_status, mock_enqueue
    ):
        for payload in (
            _TEMPLATE_STATUS_PAYLOAD,
            _TEMPLATE_QUALITY_PAYLOAD,
            _TEMPLATE_CATEGORY_PAYLOAD,
        ):
            update_status(payload["entry"][0]["changes"][0])
        job_ids = {c.kwargs["job_id"] for c in mock_enqueue.call_args_list}
        self.assertEqual(job_ids, {"whatsapp_template_sync"})


# ===========================================================================
# 3. _is_trusted_waba_id() unit tests
# ===========================================================================

class TestWabaTrustCheck(FrappeTestCase):

    @patch("frappe_whatsapp.utils.webhook.frappe.db.exists", return_value=True)
    def test_known_waba_id_is_trusted(self, mock_exists):
        self.assertTrue(_is_trusted_waba_id(_WABA_ID))
        mock_exists.assert_called_once_with(
            "WhatsApp Account", {"business_id": _WABA_ID}
        )

    @patch("frappe_whatsapp.utils.webhook.frappe.db.exists", return_value=False)
    def test_unknown_waba_id_is_not_trusted(self, _mock_exists):
        self.assertFalse(_is_trusted_waba_id(_UNKNOWN_WABA_ID))

    def test_empty_string_is_not_trusted(self):
        """An empty WABA ID (missing entry id field) must be rejected without
        hitting the database."""
        self.assertFalse(_is_trusted_waba_id(""))

    @patch("frappe_whatsapp.utils.webhook.frappe.db.exists")
    def test_only_the_supplied_id_is_checked(self, mock_exists):
        """The function must check exactly the provided WABA ID — not any
        other entries — so the caller controls the scope."""
        mock_exists.side_effect = lambda dt, f: f["business_id"] == "good-id"
        self.assertTrue(_is_trusted_waba_id("good-id"))
        self.assertFalse(_is_trusted_waba_id("bad-id"))
        # db.exists must be called with exactly what was passed in
        calls = [c.args[1]["business_id"] for c in mock_exists.call_args_list]
        self.assertIn("good-id", calls)
        self.assertIn("bad-id", calls)


# ===========================================================================
# 4. process_webhook_payload() end-to-end
# ===========================================================================

class TestProcessWebhookPayloadTemplatePath(FrappeTestCase):
    """Template events must pass through process_webhook_payload() correctly
    for both payload shapes and both trusted/untrusted WABA IDs."""

    # --- trusted, list-shaped entry ---

    @patch("frappe_whatsapp.utils.webhook._is_trusted_waba_id", return_value=True)
    @patch("frappe_whatsapp.utils.webhook.frappe.enqueue")
    @patch("frappe_whatsapp.utils.webhook.update_template_status")
    def test_trusted_list_template_status_enqueues_sync(
        self, mock_update_template_status, mock_enqueue, _mock_trusted
    ):
        """Trusted list-shaped template-status payload: status updated locally
        and sync enqueued despite no phone_number_id."""
        process_webhook_payload(_TEMPLATE_STATUS_PAYLOAD)
        expected_value = _TEMPLATE_STATUS_PAYLOAD["entry"][0]["changes"][0]["value"]
        mock_update_template_status.assert_called_once_with(expected_value)
        _assert_sync_enqueue(self, mock_enqueue)

    @patch("frappe_whatsapp.utils.webhook._is_trusted_waba_id", return_value=True)
    @patch("frappe_whatsapp.utils.webhook.frappe.enqueue")
    def test_trusted_list_template_quality_enqueues_sync(
        self, mock_enqueue, _mock_trusted
    ):
        process_webhook_payload(_TEMPLATE_QUALITY_PAYLOAD)
        _assert_sync_enqueue(self, mock_enqueue)

    @patch("frappe_whatsapp.utils.webhook._is_trusted_waba_id", return_value=True)
    @patch("frappe_whatsapp.utils.webhook.frappe.enqueue")
    def test_trusted_list_template_category_enqueues_sync(
        self, mock_enqueue, _mock_trusted
    ):
        process_webhook_payload(_TEMPLATE_CATEGORY_PAYLOAD)
        _assert_sync_enqueue(self, mock_enqueue)

    # --- trusted, dict-shaped entry ---

    @patch("frappe_whatsapp.utils.webhook._is_trusted_waba_id", return_value=True)
    @patch("frappe_whatsapp.utils.webhook.frappe.enqueue")
    @patch("frappe_whatsapp.utils.webhook.update_template_status")
    def test_trusted_dict_template_status_enqueues_sync(
        self, mock_update_template_status, mock_enqueue, _mock_trusted
    ):
        """Dict-shaped entry payload must work identically to the list shape."""
        process_webhook_payload(_TEMPLATE_STATUS_DICT_PAYLOAD)
        expected_value = _TEMPLATE_STATUS_DICT_PAYLOAD["entry"]["changes"][0]["value"]
        mock_update_template_status.assert_called_once_with(expected_value)
        _assert_sync_enqueue(self, mock_enqueue)

    # --- untrusted, list-shaped entry ---

    @patch("frappe_whatsapp.utils.webhook.frappe.log_error")
    @patch("frappe_whatsapp.utils.webhook._is_trusted_waba_id", return_value=False)
    @patch("frappe_whatsapp.utils.webhook.frappe.enqueue")
    @patch("frappe_whatsapp.utils.webhook.update_template_status")
    def test_untrusted_list_does_not_enqueue_sync(
        self, mock_update_template_status, mock_enqueue,
        _mock_not_trusted, _mock_log
    ):
        process_webhook_payload(_TEMPLATE_STATUS_PAYLOAD)
        mock_enqueue.assert_not_called()
        mock_update_template_status.assert_not_called()

    @patch("frappe_whatsapp.utils.webhook._is_trusted_waba_id", return_value=False)
    @patch("frappe_whatsapp.utils.webhook.frappe.enqueue")
    def test_untrusted_list_logs_warning(self, _mock_enqueue, _mock_not_trusted):
        with patch("frappe_whatsapp.utils.webhook.frappe.log_error") as mock_log:
            process_webhook_payload(_TEMPLATE_STATUS_PAYLOAD)
            mock_log.assert_called_once()
            title = mock_log.call_args.args[1]
            self.assertIn("untrusted", title.lower())

    # --- untrusted, dict-shaped entry ---

    @patch("frappe_whatsapp.utils.webhook.frappe.log_error")
    @patch("frappe_whatsapp.utils.webhook.frappe.enqueue")
    @patch("frappe_whatsapp.utils.webhook.update_template_status")
    @patch("frappe_whatsapp.utils.webhook.frappe.db.exists", return_value=False)
    def test_untrusted_dict_does_not_enqueue_sync(
        self, _mock_exists, mock_update_template_status,
        mock_enqueue, _mock_log
    ):
        """Dict-shaped untrusted payload must also be rejected cleanly
        without crashing the log message."""
        process_webhook_payload(_TEMPLATE_STATUS_DICT_UNTRUSTED_PAYLOAD)
        mock_enqueue.assert_not_called()
        mock_update_template_status.assert_not_called()

    @patch("frappe_whatsapp.utils.webhook.frappe.enqueue")
    @patch("frappe_whatsapp.utils.webhook.frappe.db.exists", return_value=False)
    def test_untrusted_dict_logs_warning_without_crash(
        self, _mock_exists, _mock_enqueue
    ):
        """Untrusted dict-shaped payload must produce a log message that
        includes the WABA ID (not crash trying to iterate a dict as a list)."""
        with patch("frappe_whatsapp.utils.webhook.frappe.log_error") as mock_log:
            process_webhook_payload(_TEMPLATE_STATUS_DICT_UNTRUSTED_PAYLOAD)
            mock_log.assert_called_once()
            log_msg = mock_log.call_args.args[0]
            # The specific WABA ID must appear in the log so operators can act
            self.assertIn(_UNKNOWN_WABA_ID, log_msg)

    # --- trust scope: first entry's ID is what matters ---

    @patch("frappe_whatsapp.utils.webhook.frappe.log_error")
    @patch("frappe_whatsapp.utils.webhook.frappe.enqueue")
    @patch("frappe_whatsapp.utils.webhook.update_template_status")
    @patch("frappe_whatsapp.utils.webhook._is_trusted_waba_id")
    def test_trusted_second_entry_does_not_authorize_first_entry_change(
        self, mock_is_trusted, mock_update_template_status,
        mock_enqueue, _mock_log
    ):
        """The trust check must use only the WABA ID of the entry whose change
        is being processed (entries[0]).  A trusted second entry must not
        authorize a rejected first entry's change."""
        # First entry is untrusted, second is trusted
        mock_is_trusted.side_effect = lambda wid: wid == "trusted-waba"

        payload = {
            "object": "whatsapp_business_account",
            "entry": [
                {
                    "id": "untrusted-waba",
                    "changes": [{
                        "value": {
                            "event": "APPROVED",
                            "message_template_id": 123,
                        },
                        "field": "message_template_status_update",
                    }],
                },
                {
                    "id": "trusted-waba",
                    "changes": [{
                        "value": {"statuses": []},
                        "field": "messages",
                    }],
                },
            ],
        }

        process_webhook_payload(payload)

        # Must be called with exactly the FIRST entry's WABA ID
        mock_is_trusted.assert_called_once_with("untrusted-waba")
        # First entry is untrusted → nothing must be enqueued or updated
        mock_enqueue.assert_not_called()
        mock_update_template_status.assert_not_called()

    # --- message-status path must not enqueue sync ---

    @patch("frappe_whatsapp.utils.webhook.frappe.enqueue")
    @patch("frappe_whatsapp.utils.webhook.get_whatsapp_account",
           return_value=None)
    def test_message_status_payload_does_not_enqueue_sync(
        self, _mock_get_account, mock_enqueue
    ):
        """A message-status payload that cannot resolve an account must not
        enqueue a template sync."""
        process_webhook_payload(_MESSAGE_STATUS_PAYLOAD)
        mock_enqueue.assert_not_called()
