import json
from unittest.mock import patch

import frappe
from frappe.tests.utils import FrappeTestCase

from frappe_whatsapp.utils.routing import (
    forward_incoming_to_app,
    resolve_incoming_routed_app,
    serialize_incoming_message_for_forwarding,
)
from frappe_whatsapp.utils.webhook import (
    _process_incoming_message,
    get_media_file_extension,
    normalize_media_mime_type,
)


class TestRouting(FrappeTestCase):
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
        incoming_message_doc = frappe._dict(
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
        )

        payload = serialize_incoming_message_for_forwarding(
            incoming_message_doc=incoming_message_doc
        )

        self.assertEqual(payload["profile_name"], "Jane Sender")

    @patch("frappe_whatsapp.utils.routing._mark_incoming_message_forwarded")
    @patch("frappe_whatsapp.utils.routing.make_post_request")
    @patch(
        "frappe_whatsapp.utils.routing._incoming_message_already_forwarded",
        return_value=False,
    )
    @patch("frappe_whatsapp.utils.routing.frappe.get_doc")
    def test_forward_incoming_to_app_posts_profile_name_in_payload(
        self,
        mock_get_doc,
        _mock_already_forwarded,
        mock_make_post_request,
        _mock_mark_forwarded,
    ):
        mock_get_doc.return_value = frappe._dict(
            {
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

        self.assertTrue(mock_make_post_request.called)
        payload = json.loads(mock_make_post_request.call_args.kwargs["data"])
        self.assertEqual(payload["event"], "whatsapp.incoming")
        self.assertEqual(payload["message"]["profile_name"], "Jane Sender")

    def test_resolve_incoming_routed_app_seeds_default_account_route(self):
        app = self._create_client_app()
        account = self._create_account(whatsapp_client_app=app.name)

        routed_app = resolve_incoming_routed_app(
            whatsapp_account=str(account.name),
            contact_number="+15551234567",
        )

        self.assertEqual(routed_app, app.name)
        route = frappe.get_doc(
            "WhatsApp Conversation Route",
            f"15551234567-{account.name}",
        )
        self.assertEqual(route.last_source_app, app.name)
        self.assertFalse(route.last_outgoing_message)
        self.assertFalse(route.last_outgoing_at)

    @patch("frappe_whatsapp.utils.routing._mark_incoming_message_forwarded")
    @patch("frappe_whatsapp.utils.routing.make_post_request")
    @patch(
        "frappe_whatsapp.utils.routing._incoming_message_already_forwarded",
        return_value=False,
    )
    def test_forward_incoming_to_app_uses_account_default_app_when_unrouted(
        self,
        _mock_already_forwarded,
        mock_make_post_request,
        _mock_mark_forwarded,
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

        self.assertTrue(mock_make_post_request.called)
        route = frappe.get_doc(
            "WhatsApp Conversation Route",
            f"15551234567-{account.name}",
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

        message_doc = frappe.get_doc("WhatsApp Message", doc_name)
        self.assertEqual(message_doc.routed_app, app.name)
        self.assertEqual(message_doc.profile_name, "Jane Sender")
        mock_forward_async.assert_called_once_with(
            incoming_message_name=str(message_doc.name)
        )

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

        message_doc = frappe.get_doc("WhatsApp Message", doc_name)
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

        message_doc = frappe.get_doc("WhatsApp Message", doc_name)
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
