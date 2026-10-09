import json
from typing import Any, cast
from unittest.mock import patch

import frappe
from frappe.tests.utils import FrappeTestCase

from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_conversation_route.whatsapp_conversation_route import (
    WhatsAppConversationRoute,
)
from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_message.whatsapp_message import (
    WhatsAppMessage,
)
from frappe_whatsapp.utils.routing import (
    forward_incoming_to_app,
    forward_incoming_to_app_async,
    recover_unqueued_incoming_events,
    resolve_incoming_routed_app,
    serialize_incoming_message_for_forwarding,
)
from frappe_whatsapp.utils.webhook import (
    _format_shared_contacts,
    _process_incoming_message,
    get_media_file_extension,
    normalize_media_mime_type,
    process_webhook_payload,
)


def _get_message_doc(name: Any) -> WhatsAppMessage:
    assert isinstance(name, str)
    return cast(
        WhatsAppMessage,
        frappe.get_doc("WhatsApp Message", name),
    )


class TestRouting(FrappeTestCase):
    @patch(
        "frappe_whatsapp.utils.routing.forward_incoming_to_app_by_name"
    )
    @patch("frappe_whatsapp.utils.routing.frappe.get_all")
    @patch("frappe_whatsapp.utils.routing.frappe.db.has_column")
    def test_recover_unqueued_incoming_events_waits_for_media(
        self,
        mock_has_column,
        mock_get_all,
        mock_forward,
    ):
        mock_has_column.return_value = True
        mock_get_all.return_value = [
            frappe._dict({
                "name": "MSG-TEXT",
                "content_type": "text",
                "attach": None,
            }),
            frappe._dict({
                "name": "MSG-MEDIA-PENDING",
                "content_type": "image",
                "attach": None,
            }),
            frappe._dict({
                "name": "MSG-MEDIA-READY",
                "content_type": "image",
                "attach": "/private/files/photo.jpg",
            }),
        ]

        recover_unqueued_incoming_events()

        self.assertEqual(
            [call.kwargs["incoming_message_name"]
             for call in mock_forward.call_args_list],
            ["MSG-TEXT", "MSG-MEDIA-READY"],
        )
        mock_get_all.assert_called_once_with(
            "WhatsApp Message",
            filters={
                "type": "Incoming",
                "client_event_queued": 0,
                "routed_app": ["is", "set"],
            },
            fields=["name", "content_type", "attach"],
            order_by="creation asc",
            limit=100,
        )

    def test_forward_queue_handles_missing_and_available_referral_column(self):
        for has_column, source_type, expected_queue in (
            (False, None, "short"),
            (True, None, "short"),
            (True, "ad", "default"),
            (True, "post", "short"),
        ):
            with (
                self.subTest(has_column=has_column, source_type=source_type),
                patch.object(frappe.db, "has_column", return_value=has_column),
                patch.object(
                    frappe.db, "get_value", return_value=source_type,
                ) as get_value,
                patch("frappe.enqueue") as enqueue,
            ):
                forward_incoming_to_app_async(incoming_message_name="TEST-MSG")
                enqueue.assert_called_once_with(
                    "frappe_whatsapp.utils.routing."
                    "forward_incoming_to_app_by_name",
                    queue=expected_queue,
                    incoming_message_name="TEST-MSG",
                    enqueue_after_commit=True,
                )
                if not has_column:
                    get_value.assert_not_called()

    def test_forward_queue_propagates_database_errors(self):
        error = frappe.db.OperationalError(2003, "connection unavailable")
        with (
            patch.object(frappe.db, "has_column", side_effect=error),
            patch("frappe.enqueue") as enqueue,
            self.assertRaises(frappe.db.OperationalError),
        ):
            forward_incoming_to_app_async(incoming_message_name="TEST-MSG")
        enqueue.assert_not_called()

    def test_system_event_does_not_block_next_message_or_change_identity(self):
        app = self._create_client_app()
        account = self._create_account(whatsapp_client_app=app.name)
        suffix = frappe.generate_hash(length=8)
        system_id, text_id = f"wamid.system.{suffix}", f"wamid.text.{suffix}"
        system = {
            "from": "15551230000", "id": system_id, "type": "system",
            "system": {
                "type": "user_changed_number", "wa_id": "15551239999",
                "body": "User changed from 15551230000 to 15551239999",
            },
        }
        payload = {"entry": [{"changes": [{"field": "messages", "value": {
            "metadata": {"phone_number_id": "test-phone-id"},
            "messages": [system, {
                "from": "15551230001", "id": text_id, "type": "text",
                "text": {"body": "Hello"},
            }],
        }}]}]}
        with (
            patch(
                "frappe_whatsapp.utils.webhook.get_whatsapp_account",
                return_value=account,
            ),
            patch(
                "frappe_whatsapp.utils.webhook.is_contact_blocked",
                return_value=False,
            ),
            patch(
                "frappe_whatsapp.utils.webhook.resolve_incoming_routed_app",
                return_value=app.name,
            ) as route,
            patch(
                "frappe_whatsapp.utils.webhook._handle_consent_keywords",
            ) as consent,
            patch(
                "frappe_whatsapp.utils.webhook._enqueue_language_detection",
            ) as language,
            patch(
                "frappe_whatsapp.utils.webhook.forward_incoming_to_app_async",
            ) as forward,
            patch("frappe_whatsapp.utils.webhook.frappe.logger") as logger,
        ):
            # The system event alone must never reach any document write.
            with patch("frappe.get_doc") as get_doc:
                _process_incoming_message(
                    message=system, whatsapp_account=account,
                    sender_profile_name=None,
                )
                get_doc.assert_not_called()
            for downstream in (route, consent, language, forward):
                downstream.assert_not_called()
            logger.reset_mock()
            process_webhook_payload(payload)

        self.assertFalse(frappe.db.exists(
            "WhatsApp Message", {"message_id": system_id},
        ))
        message_name = frappe.db.get_value(
            "WhatsApp Message", {"message_id": text_id}, "name",
        )
        self.assertTrue(message_name)
        route.assert_called_once()
        route_kwargs = route.call_args.kwargs
        self.assertEqual(route_kwargs["whatsapp_account"], account.name)
        self.assertEqual(route_kwargs["contact_number"], "15551230001")
        self.assertEqual(
            route_kwargs["contact_profile"],
            frappe.db.get_value(
                "WhatsApp Message", message_name, "contact_profile"
            ),
        )
        consent.assert_called_once()
        language.assert_called_once()
        forward.assert_called_once_with(incoming_message_name=str(message_name))
        warning = logger.return_value.warning.call_args.args[0]
        self.assertIn(system_id, warning)
        self.assertNotIn("15551230000", warning)
        self.assertNotIn("15551239999", warning)

    def _create_client_app(
        self,
        *,
        enabled: int = 1,
        inbound_webhook_url: str = "https://example.com/incoming",
    ):
        suffix = frappe.generate_hash(length=8)
        return frappe.get_doc(
            {
                "doctype": "WhatsApp Client App",
                "app_id": f"client-app-{suffix}",
                "enabled": enabled,
                "inbound_webhook_url": inbound_webhook_url,
            }
        ).insert(ignore_permissions=True)

    def _create_account(self, *, whatsapp_client_app: str | None = None):
        suffix = frappe.generate_hash(length=8)
        return frappe.get_doc(
            {
                "doctype": "WhatsApp Account",
                "account_name": f"Test Account {suffix}",
                "status": "Active",
                "whatsapp_client_app": whatsapp_client_app,
            }
        ).insert(ignore_permissions=True)

    def test_serialize_incoming_message_for_forwarding_includes_profile_name(
        self,
    ):
        incoming_message_doc = cast(
            WhatsAppMessage,
            frappe._dict(
                {
                    "name": "MSG-0001",
                    "doctype": "WhatsApp Message",
                    "from": "15551234567",
                    "to": "15557654321",
                    "profile_name": "Jane Sender",
                    "whatsapp_account": "Test Account",
                    "content_type": "text",
                    "message": "Hello there",
                    "message_id": "wamid.123",
                    "creation": "2026-03-17 10:00:00",
                    "attach": None,
                }
            ),
        )

        payload = serialize_incoming_message_for_forwarding(
            incoming_message_doc=incoming_message_doc
        )

        self.assertEqual(payload["profile_name"], "Jane Sender")
        self.assertIsNone(payload["referral"])

    def test_serialize_incoming_message_includes_campaign_referral(self):
        incoming_message_doc = cast(
            WhatsAppMessage,
            frappe._dict(
                {
                    "name": "MSG-ATTRIBUTION-1",
                    "doctype": "WhatsApp Message",
                    "from": "15551234567",
                    "to": "15557654321",
                    "profile_name": "Jane Sender",
                    "whatsapp_account": "Test Account",
                    "content_type": "text",
                    "message": "Hello from an ad",
                    "message_id": "wamid.attribution",
                    "creation": "2026-09-01 10:00:00",
                    "attach": None,
                    "referral_source_type": "ad",
                    "referral_source_id": "ad-123",
                    "referral_payload": json.dumps({
                        "source_type": "ad",
                        "source_id": "ad-123",
                        "ctwa_clid": "click-123",
                    }),
                    "meta_ad_name": "Enrollment Ad",
                    "meta_ad_account_id": "act-123",
                    "meta_adset_id": "adset-123",
                    "meta_adset_name": "Prospects",
                    "meta_campaign_id": "campaign-123",
                    "meta_campaign_name": "Fall Enrollment",
                    "attribution_status": "Resolved",
                }
            ),
        )

        payload = serialize_incoming_message_for_forwarding(
            incoming_message_doc=incoming_message_doc
        )

        self.assertEqual(payload["referral"]["ctwa_clid"], "click-123")
        self.assertEqual(
            payload["referral"]["campaign"],
            {"id": "campaign-123", "name": "Fall Enrollment"},
        )

    def test_format_shared_contacts_includes_readable_structured_details(self):
        summary = _format_shared_contacts([
            {
                "name": {
                    "formatted_name": "Ana Vera Mamá ❤️",
                    "first_name": "Ana",
                    "last_name": "Vera",
                },
                "phones": [
                    {
                        "phone": "+1 (201) 970-9401",
                        "wa_id": "12019709401",
                        "type": "CELL",
                    },
                    {
                        "phone": "+1 (201) 970-9401",
                        "type": "WORK",
                    },
                    {"wa_id": "573115631239"},
                ],
                "emails": [
                    {"email": "ana@example.com", "type": "WORK"},
                    {"email": "ana@example.com", "type": "HOME"},
                ],
                "org": {
                    "company": "Zoni",
                    "department": "Admissions",
                    "title": "Advisor",
                },
                "addresses": [
                    {
                        "street": "123 Main St",
                        "city": "Miami",
                        "state": "FL",
                        "zip": "33101",
                        "country": "United States",
                        "type": "WORK",
                    }
                ],
                "urls": [{"url": "https://example.com", "type": "WORK"}],
                "birthday": "1990-01-02",
                "vcard": "base64-data-that-must-not-be-rendered",
                "origin": "other",
            },
            {
                "name": {
                    "prefix": "Dr.",
                    "first_name": "José",
                    "middle_name": "Luis",
                    "last_name": "Pérez",
                    "suffix": "Jr.",
                },
                "phones": [{"phone": "+57 311 5631239"}],
            },
        ])

        self.assertEqual(
            summary,
            (
                "Shared contacts (2)\n\n"
                "Contact 1\n"
                "Name: Ana Vera Mamá ❤️\n"
                "Phone (CELL): +1 (201) 970-9401\n"
                "Phone: 573115631239\n"
                "Email (WORK): ana@example.com\n"
                "Organization: Zoni — Admissions — Advisor\n"
                "Address (WORK): 123 Main St, Miami, FL, 33101, "
                "United States\n"
                "URL (WORK): https://example.com\n"
                "Birthday: 1990-01-02\n\n"
                "Contact 2\n"
                "Name: Dr. José Luis Pérez Jr.\n"
                "Phone: +57 311 5631239"
            ),
        )
        self.assertNotIn("base64-data", summary)
        self.assertNotIn("origin", summary)

    def test_format_shared_contacts_handles_empty_and_partial_payloads(self):
        fallback = (
            "A contact was shared, but no readable contact details "
            "were provided."
        )
        self.assertEqual(_format_shared_contacts(None), fallback)
        self.assertEqual(_format_shared_contacts([]), fallback)
        self.assertEqual(_format_shared_contacts(["invalid"]), fallback)
        self.assertEqual(
            _format_shared_contacts([{"name": None, "phones": "invalid"}]),
            "Shared contact\n\nName: Unnamed contact",
        )

    @patch("frappe_whatsapp.utils.client_delivery.queue_client_event")
    @patch("frappe_whatsapp.utils.routing.frappe.get_doc")
    def test_forward_incoming_to_app_queues_profile_name_in_payload(
        self,
        mock_get_doc,
        mock_queue_client_event,
    ):
        mock_get_doc.return_value = frappe._dict(
            {
                "name": "Test Client App",
                "enabled": 1,
                "inbound_webhook_url": "https://example.com/incoming",
                "app_id": "client-app-1",
            }
        )
        incoming_message_doc = frappe._dict(
            {
                "name": "MSG-0001",
                "doctype": "WhatsApp Message",
                "routed_app": "Test Client App",
                "from": "15551234567",
                "to": "15557654321",
                "profile_name": "Jane Sender",
                "whatsapp_account": "Test Account",
                "content_type": "text",
                "message": "Hello there",
                "message_id": "wamid.123",
                "creation": "2026-03-17 10:00:00",
                "attach": None,
            }
        )

        forward_incoming_to_app(incoming_message_doc=incoming_message_doc)

        mock_queue_client_event.assert_called_once()
        queue_kwargs = mock_queue_client_event.call_args.kwargs
        self.assertEqual(queue_kwargs["event_type"], "whatsapp.incoming")
        payload = queue_kwargs["payload"]
        self.assertEqual(payload["message"]["profile_name"], "Jane Sender")
        self.assertEqual(payload["message"]["whatsapp_account"], "Test Account")

    def test_resolve_incoming_routed_app_seeds_default_account_route(self):
        app = self._create_client_app()
        account = self._create_account(whatsapp_client_app=app.name)

        routed_app = resolve_incoming_routed_app(
            whatsapp_account=str(account.name),
            contact_number="+15551234567",
        )

        self.assertEqual(routed_app, app.name)
        route = cast(
            WhatsAppConversationRoute,
            frappe.get_doc(
                "WhatsApp Conversation Route",
                f"15551234567-{account.name}",
            ),
        )
        self.assertEqual(route.last_source_app, app.name)
        self.assertFalse(route.last_outgoing_message)
        self.assertFalse(route.last_outgoing_at)

    @patch("frappe_whatsapp.utils.client_delivery.queue_client_event")
    def test_forward_incoming_to_app_uses_account_default_app_when_unrouted(
        self,
        mock_queue_client_event,
    ):
        app = self._create_client_app()
        account = self._create_account(whatsapp_client_app=app.name)
        incoming_message_doc = frappe._dict(
            {
                "name": "MSG-0002",
                "doctype": "WhatsApp Message",
                "routed_app": None,
                "from": "+15551234567",
                "to": "15557654321",
                "profile_name": "Jane Sender",
                "whatsapp_account": account.name,
                "content_type": "text",
                "message": "Hello there",
                "message_id": "wamid.456",
                "creation": "2026-03-17 10:00:00",
                "attach": None,
            }
        )

        forward_incoming_to_app(incoming_message_doc=incoming_message_doc)

        mock_queue_client_event.assert_called_once()
        route = cast(
            WhatsAppConversationRoute,
            frappe.get_doc(
                "WhatsApp Conversation Route",
                f"15551234567-{account.name}",
            ),
        )
        self.assertEqual(route.last_source_app, app.name)

    @patch("frappe_whatsapp.utils.webhook._handle_consent_keywords")
    @patch("frappe_whatsapp.utils.webhook.forward_incoming_to_app_async")
    def test_process_incoming_message_sets_routed_app_from_account_default(
        self,
        mock_forward_async,
        _mock_handle_consent_keywords,
    ):
        app = self._create_client_app()
        account = self._create_account(whatsapp_client_app=app.name)
        message_id = f"wamid.{frappe.generate_hash(length=8)}"

        _process_incoming_message(
            message={
                "id": message_id,
                "from": "+15551234567",
                "type": "text",
                "text": {"body": "Hello there"},
                "referral": {
                    "source_type": "ad",
                    "source_id": "ad-123",
                    "ctwa_clid": "click-123",
                },
            },
            whatsapp_account=account,
            sender_profile_name="Jane Sender",
        )

        doc_name = frappe.db.get_value(
            "WhatsApp Message",
            {"message_id": message_id},
            "name",
        )
        self.assertTrue(doc_name)

        message_doc = _get_message_doc(doc_name)
        self.assertEqual(message_doc.routed_app, app.name)
        self.assertEqual(message_doc.profile_name, "Jane Sender")
        self.assertEqual(message_doc.referral_source_id, "ad-123")
        self.assertEqual(message_doc.referral_ctwa_clid, "click-123")
        self.assertEqual(message_doc.attribution_status, "Pending")
        mock_forward_async.assert_called_once_with(
            incoming_message_name=str(message_doc.name)
        )

    def test_process_incoming_unsupported_message_is_logged_and_skipped(self):
        message = {
            "from": "19293461149",
            "from_user_id": "US.1033196179544075",
            "id": "wamid.unsupported-production-shape",
            "type": "unsupported",
            "unsupported": {
                "type": "unknown",
                "raw_type": "unknown",
            },
            "errors": [{
                "code": 131051,
                "title": "Message type unknown",
                "error_data": {
                    "details": "Message type is currently not supported.",
                },
            }],
        }
        account = frappe._dict({"name": "Test WhatsApp Account"})

        with (
            patch(
                "frappe_whatsapp.utils.webhook.is_contact_blocked",
                return_value=False,
            ),
            patch(
                "frappe_whatsapp.utils.webhook.resolve_incoming_routed_app"
            ) as mock_resolve_route,
            patch(
                "frappe_whatsapp.utils.webhook.frappe.get_doc"
            ) as mock_get_doc,
            patch(
                "frappe_whatsapp.utils.webhook.frappe.enqueue"
            ) as mock_enqueue,
            patch(
                "frappe_whatsapp.utils.webhook._handle_consent_keywords"
            ) as mock_handle_consent,
            patch(
                "frappe_whatsapp.utils.webhook._enqueue_language_detection"
            ) as mock_enqueue_language,
            patch(
                "frappe_whatsapp.utils.webhook.forward_incoming_to_app_async"
            ) as mock_forward_async,
            patch(
                "frappe_whatsapp.utils.webhook.frappe.logger"
            ) as mock_logger,
        ):
            _process_incoming_message(
                message=message,
                whatsapp_account=account,
                sender_profile_name="Sensitive Sender Name",
            )

        mock_resolve_route.assert_not_called()
        mock_get_doc.assert_not_called()
        mock_enqueue.assert_not_called()
        mock_handle_consent.assert_not_called()
        mock_enqueue_language.assert_not_called()
        mock_forward_async.assert_not_called()
        mock_logger.assert_called_once_with("frappe_whatsapp")

        warning = mock_logger.return_value.warning
        warning.assert_called_once()
        warning_message = warning.call_args.args[0]
        self.assertIn("wamid.unsupported-production-shape", warning_message)
        self.assertIn("Test WhatsApp Account", warning_message)
        self.assertIn('"message_type": "unsupported"', warning_message)
        self.assertIn('"unsupported_type": "unknown"', warning_message)
        self.assertIn('"raw_type": "unknown"', warning_message)
        self.assertIn('"code": "131051"', warning_message)
        self.assertIn("Message type unknown", warning_message)
        self.assertIn(
            "Message type is currently not supported.", warning_message
        )
        self.assertNotIn("19293461149", warning_message)
        self.assertNotIn("US.1033196179544075", warning_message)
        self.assertNotIn("Sensitive Sender Name", warning_message)

    def test_process_incoming_legacy_unknown_with_malformed_metadata_skips(self):
        message = {
            "from": "15551234567",
            "id": "wamid.legacy-unknown",
            "type": "unknown",
            "unsupported": "malformed",
            "errors": {
                "code": 131051,
                "title": "Unsupported message type",
                "error_data": "malformed",
                "details": "Message type is not currently supported",
            },
        }
        account = frappe._dict({"name": "Test WhatsApp Account"})

        with (
            patch(
                "frappe_whatsapp.utils.webhook.is_contact_blocked",
                return_value=False,
            ),
            patch(
                "frappe_whatsapp.utils.webhook.resolve_incoming_routed_app"
            ) as mock_resolve_route,
            patch(
                "frappe_whatsapp.utils.webhook.frappe.get_doc"
            ) as mock_get_doc,
            patch(
                "frappe_whatsapp.utils.webhook.frappe.logger"
            ) as mock_logger,
        ):
            _process_incoming_message(
                message=message,
                whatsapp_account=account,
                sender_profile_name="Sensitive Sender Name",
            )

        mock_resolve_route.assert_not_called()
        mock_get_doc.assert_not_called()
        mock_logger.assert_called_once_with("frappe_whatsapp")
        warning_message = mock_logger.return_value.warning.call_args.args[0]
        self.assertIn('"message_type": "unknown"', warning_message)
        self.assertIn('"unsupported_type": null', warning_message)
        self.assertIn('"raw_type": null', warning_message)
        self.assertIn('"code": "131051"', warning_message)
        self.assertIn(
            "Message type is not currently supported", warning_message
        )
        self.assertNotIn("15551234567", warning_message)
        self.assertNotIn("Sensitive Sender Name", warning_message)

    @patch("frappe_whatsapp.utils.webhook._enqueue_language_detection")
    @patch("frappe_whatsapp.utils.webhook._handle_consent_keywords")
    @patch("frappe_whatsapp.utils.webhook.forward_incoming_to_app_async")
    def test_process_incoming_contacts_normalizes_inserts_and_forwards_once(
        self,
        mock_forward_async,
        mock_handle_consent,
        mock_enqueue_language,
    ):
        frappe.reload_doc("frappe_whatsapp", "doctype", "whatsapp_message")
        app = self._create_client_app()
        account = self._create_account(whatsapp_client_app=app.name)
        message_id = f"wamid.{frappe.generate_hash(length=8)}"
        message = {
            "id": message_id,
            "from": "+15551234567",
            "type": "contacts",
            "contacts": [
                {
                    "name": {"formatted_name": "Ana Vera Mamá ❤️"},
                    "phones": [
                        {
                            "phone": "+1 (201) 970-9401",
                            "wa_id": "12019709401",
                        }
                    ],
                },
                {
                    "name": {"first_name": "José", "last_name": "Pérez"},
                    "emails": [{"email": "jose@example.com"}],
                },
            ],
        }

        _process_incoming_message(
            message=message,
            whatsapp_account=account,
            sender_profile_name="Jane Sender",
        )
        _process_incoming_message(
            message=message,
            whatsapp_account=account,
            sender_profile_name="Jane Sender",
        )

        message_names = frappe.get_all(
            "WhatsApp Message",
            filters={"message_id": message_id},
            pluck="name",
        )
        self.assertEqual(len(message_names), 1)

        message_doc = _get_message_doc(message_names[0])
        self.assertEqual(message_doc.content_type, "contact")
        self.assertEqual(message_doc.profile_name, "Jane Sender")
        self.assertEqual(message_doc.routed_app, app.name)
        self.assertEqual(
            message_doc.message,
            (
                "Shared contacts (2)\n\n"
                "Contact 1\n"
                "Name: Ana Vera Mamá ❤️\n"
                "Phone: +1 (201) 970-9401\n\n"
                "Contact 2\n"
                "Name: José Pérez\n"
                "Email: jose@example.com"
            ),
        )
        mock_forward_async.assert_called_once_with(
            incoming_message_name=str(message_doc.name)
        )
        mock_handle_consent.assert_not_called()
        mock_enqueue_language.assert_not_called()

    @patch("frappe_whatsapp.utils.webhook._handle_consent_keywords")
    @patch("frappe_whatsapp.utils.webhook.forward_incoming_to_app_async")
    @patch("frappe_whatsapp.utils.webhook.frappe.enqueue")
    def test_process_incoming_sticker_message_enqueues_media_download(
        self,
        mock_enqueue,
        mock_forward_async,
        _mock_handle_consent_keywords,
    ):
        frappe.reload_doc("frappe_whatsapp", "doctype", "whatsapp_message")
        app = self._create_client_app()
        account = self._create_account(whatsapp_client_app=app.name)
        message_id = f"wamid.{frappe.generate_hash(length=8)}"
        media_id = frappe.generate_hash(length=12)

        _process_incoming_message(
            message={
                "id": message_id,
                "from": "+15551234567",
                "type": "sticker",
                "sticker": {
                    "id": media_id,
                    "mime_type": "image/webp",
                    "animated": False,
                },
            },
            whatsapp_account=account,
            sender_profile_name="Jane Sender",
        )

        doc_name = frappe.db.get_value(
            "WhatsApp Message",
            {"message_id": message_id},
            "name",
        )
        self.assertTrue(doc_name)

        message_doc = _get_message_doc(doc_name)
        self.assertEqual(message_doc.content_type, "sticker")
        self.assertEqual(message_doc.message, "")
        self.assertEqual(message_doc.routed_app, app.name)
        self.assertEqual(message_doc.profile_name, "Jane Sender")

        mock_enqueue.assert_called_once_with(
            "frappe_whatsapp.utils.webhook.download_and_attach_media",
            queue="long",
            whatsapp_account_name=account.name,
            message_docname=message_doc.name,
            media_id=media_id,
            message_type="sticker",
            enqueue_after_commit=True,
        )
        mock_forward_async.assert_not_called()

    @patch("frappe_whatsapp.utils.webhook._handle_consent_keywords")
    @patch("frappe_whatsapp.utils.webhook.forward_incoming_to_app_async")
    @patch("frappe_whatsapp.utils.webhook.frappe.enqueue")
    def test_process_incoming_audio_voice_note_enqueues_media_download(
        self,
        mock_enqueue,
        mock_forward_async,
        _mock_handle_consent_keywords,
    ):
        frappe.reload_doc("frappe_whatsapp", "doctype", "whatsapp_message")
        app = self._create_client_app()
        account = self._create_account(whatsapp_client_app=app.name)
        message_id = f"wamid.{frappe.generate_hash(length=8)}"
        media_id = frappe.generate_hash(length=12)

        _process_incoming_message(
            message={
                "id": message_id,
                "from": "+15551234567",
                "type": "audio",
                "audio": {
                    "id": media_id,
                    "mime_type": "audio/ogg; codecs=opus",
                    "voice": True,
                },
            },
            whatsapp_account=account,
            sender_profile_name="Jane Sender",
        )

        doc_name = frappe.db.get_value(
            "WhatsApp Message",
            {"message_id": message_id},
            "name",
        )
        self.assertTrue(doc_name)

        message_doc = _get_message_doc(doc_name)
        self.assertEqual(message_doc.content_type, "audio")
        self.assertEqual(message_doc.message, "")
        if message_doc.meta.has_field("is_voice_note"):
            self.assertEqual(message_doc.get("is_voice_note"), 1)

        mock_enqueue.assert_called_once_with(
            "frappe_whatsapp.utils.webhook.download_and_attach_media",
            queue="long",
            whatsapp_account_name=account.name,
            message_docname=message_doc.name,
            media_id=media_id,
            message_type="audio",
            enqueue_after_commit=True,
        )
        mock_forward_async.assert_not_called()

    @patch("frappe_whatsapp.utils.webhook.frappe.enqueue")
    def test_duplicate_media_message_id_prevents_duplicate_insert(
        self,
        mock_enqueue,
    ):
        frappe.reload_doc("frappe_whatsapp", "doctype", "whatsapp_message")
        account = self._create_account()
        message_id = f"wamid.{frappe.generate_hash(length=8)}"

        frappe.get_doc({
            "doctype": "WhatsApp Message",
            "type": "Incoming",
            "from": "+15551234567",
            "message_id": message_id,
            "message": "",
            "content_type": "audio",
            "whatsapp_account": account.name,
        }).insert(ignore_permissions=True)

        _process_incoming_message(
            message={
                "id": message_id,
                "from": "+15551234567",
                "type": "audio",
                "audio": {
                    "id": frappe.generate_hash(length=12),
                    "mime_type": "audio/ogg; codecs=opus",
                    "voice": True,
                },
            },
            whatsapp_account=account,
            sender_profile_name="Jane Sender",
        )

        self.assertEqual(
            frappe.db.count(
                "WhatsApp Message",
                filters={"message_id": message_id},
            ),
            1,
        )
        mock_enqueue.assert_not_called()

    def test_media_file_extension_normalizes_mime_parameters(self):
        self.assertEqual(
            normalize_media_mime_type("audio/ogg; codecs=opus"),
            "audio/ogg",
        )
        self.assertEqual(
            get_media_file_extension(
                "audio/ogg; codecs=opus",
                message_type="audio",
            ),
            "ogg",
        )
        self.assertEqual(
            get_media_file_extension("audio/mp4", message_type="audio"),
            "m4a",
        )
