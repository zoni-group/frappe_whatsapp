import json
from typing import cast
from unittest.mock import Mock, patch

import frappe
from frappe.core.doctype.file.file import File
from frappe.tests.utils import FrappeTestCase
from frappe.utils import get_url
from ..doctype.whatsapp_account.whatsapp_account import WhatsAppAccount
from ..doctype.whatsapp_client_app.whatsapp_client_app import (
    WhatsAppClientApp,
)
from ..doctype.whatsapp_message.whatsapp_message import WhatsAppMessage

from frappe_whatsapp.utils.routing import (
    FORWARDED_INCOMING_CACHE_PREFIX,
    forward_incoming_to_app_by_name,
    set_last_sender_app,
)
from frappe_whatsapp.utils.webhook import (
    _process_incoming_message,
    download_and_attach_media,
)


class TestInboundRouting(FrappeTestCase):
    def setUp(self):
        self.created_file_names: list[str] = []
        self.created_message_names: list[str] = []

        self.test_suffix = frappe.generate_hash(length=8)
        self.contact_number = f"1555{frappe.generate_hash(length=7)}"

        self.account = cast(
            WhatsAppAccount,
            frappe.get_doc({
                "doctype": "WhatsApp Account",
                "account_name": f"Routing Test Account {self.test_suffix}",
                "url": "https://graph.facebook.com",
                "version": "v18.0",
                "phone_id": f"phone-{self.test_suffix}",
                "business_id": f"biz-{self.test_suffix}",
                "token": "test_token",
                "webhook_verify_token": f"verify-{self.test_suffix}",
            }).insert(ignore_permissions=True),
        )
        self.account_name = str(self.account.name)

        self.client_app = cast(
            WhatsAppClientApp,
            frappe.get_doc({
                "doctype": "WhatsApp Client App",
                "app_id": f"route-app-{self.test_suffix}",
                "enabled": 1,
                "inbound_webhook_url": "https://example.com/inbound",
            }).insert(ignore_permissions=True),
        )
        self.client_app_name = str(self.client_app.name)

        set_last_sender_app(
            whatsapp_account=self.account_name,
            to_number=self.contact_number,
            source_app=self.client_app_name,
        )
        self.route_name = f"{self.contact_number}-{self.account_name}"

    def tearDown(self):
        for message_name in self.created_message_names:
            frappe.cache().delete_value(
                f"{FORWARDED_INCOMING_CACHE_PREFIX}{message_name}"
            )

        for file_name in reversed(self.created_file_names):
            if frappe.db.exists("File", file_name):
                frappe.delete_doc("File", file_name, force=True)

        if self.created_message_names:
            frappe.db.delete(
                "Document Share Key",
                {
                    "reference_doctype": "WhatsApp Message",
                    "reference_docname": ["in", self.created_message_names],
                },
            )

        for message_name in reversed(self.created_message_names):
            if frappe.db.exists("WhatsApp Message", message_name):
                frappe.delete_doc(
                    "WhatsApp Message",
                    message_name,
                    force=True,
                )

        if frappe.db.exists("WhatsApp Conversation Route", self.route_name):
            frappe.delete_doc(
                "WhatsApp Conversation Route",
                self.route_name,
                force=True,
            )

        if frappe.db.exists("WhatsApp Client App", self.client_app_name):
            frappe.delete_doc(
                "WhatsApp Client App",
                self.client_app_name,
                force=True,
            )

        if frappe.db.exists("WhatsApp Account", self.account_name):
            frappe.delete_doc(
                "WhatsApp Account",
                self.account_name,
                force=True,
            )

    def _create_incoming_message(
        self,
        *,
        content_type: str,
        message: str = "",
        message_id: str | None = None,
    ) -> WhatsAppMessage:
        doc = cast(
            WhatsAppMessage,
            frappe.get_doc({
                "doctype": "WhatsApp Message",
                "type": "Incoming",
                "from": self.contact_number,
                "message": message,
                "message_id": (
                    message_id or f"wamid-{frappe.generate_hash(length=10)}"
                ),
                "content_type": content_type,
                "whatsapp_account": self.account_name,
                "routed_app": self.client_app_name,
            }).insert(ignore_permissions=True),
        )
        self.created_message_names.append(str(doc.name))
        return doc

    def _attach_file(
        self,
        *,
        message_doc: WhatsAppMessage,
        file_name: str,
        content: bytes,
    ) -> tuple[str, str]:
        file_doc = cast(
            File,
            frappe.get_doc({
                "doctype": "File",
                "file_name": file_name,
                "attached_to_doctype": "WhatsApp Message",
                "attached_to_name": str(message_doc.name),
                "attached_to_field": "attach",
                "content": content,
            }),
        )
        file_doc.save(ignore_permissions=True)
        self.created_file_names.append(str(file_doc.name))

        file_url = file_doc.file_url
        assert file_url

        frappe.db.set_value(
            "WhatsApp Message",
            str(message_doc.name),
            "attach",
            file_url,
        )
        message_doc.reload()

        return str(file_doc.name), str(file_url)

    def test_text_message_forwarding_still_works(self):
        with patch(
            "frappe_whatsapp.utils.webhook._handle_consent_keywords"
        ), patch(
            "frappe_whatsapp.utils.webhook.forward_incoming_to_app_async"
        ) as forward_mock:
            _process_incoming_message(
                message={
                    "from": self.contact_number,
                    "type": "text",
                    "id": f"wamid-{frappe.generate_hash(length=10)}",
                    "text": {"body": "hello from whatsapp"},
                },
                whatsapp_account=self.account,
                sender_profile_name="Test Sender",
            )

        self.assertEqual(forward_mock.call_count, 1)
        message_name = str(
            forward_mock.call_args.kwargs["incoming_message_name"]
        )
        self.created_message_names.append(message_name)

        message_doc = cast(
            WhatsAppMessage,
            frappe.get_doc("WhatsApp Message", message_name),
        )
        self.assertEqual(message_doc.content_type, "text")
        self.assertEqual(message_doc.message, "hello from whatsapp")

    def test_media_message_forwarding_includes_attachment_url(self):
        message_doc = self._create_incoming_message(
            content_type="video",
            message="clip caption",
        )
        file_name, file_url = self._attach_file(
            message_doc=message_doc,
            file_name="clip.mp4",
            content=b"video-bytes",
        )
        self.assertTrue(frappe.db.exists("File", file_name))

        with patch(
            "frappe_whatsapp.utils.routing.make_post_request"
        ) as post_request_mock:
            forward_incoming_to_app_by_name(
                incoming_message_name=str(message_doc.name)
            )

        payload = json.loads(post_request_mock.call_args.kwargs["data"])
        self.assertEqual(payload["event"], "whatsapp.incoming")
        self.assertEqual(payload["message"]["attach"], file_url)
        self.assertEqual(
            payload["message"]["attachment_url"],
            get_url(file_url),
        )
        self.assertTrue(payload["message"]["has_attachment"])
        self.assertEqual(payload["message"]["attachment_name"], "clip.mp4")
        self.assertEqual(
            payload["message"]["attachment_mime_type"],
            "video/mp4",
        )

    def test_media_messages_forward_only_after_attachment_is_stored(self):
        message_id = f"wamid-{frappe.generate_hash(length=10)}"

        with patch(
            "frappe_whatsapp.utils.webhook._handle_consent_keywords"
        ), patch(
            "frappe_whatsapp.utils.webhook.forward_incoming_to_app_async"
        ) as forward_mock, patch(
            "frappe_whatsapp.utils.webhook.frappe.enqueue"
        ) as enqueue_mock:
            _process_incoming_message(
                message={
                    "from": self.contact_number,
                    "type": "video",
                    "id": message_id,
                    "video": {
                        "id": "media-123",
                        "caption": "watch this",
                    },
                },
                whatsapp_account=self.account,
                sender_profile_name="Test Sender",
            )

        forward_mock.assert_not_called()
        enqueue_mock.assert_called_once()

        message_name = frappe.db.get_value(
            "WhatsApp Message",
            {"message_id": message_id},
            "name",
        )
        assert message_name
        message_name = str(message_name)
        self.created_message_names.append(message_name)
        message_doc = cast(
            WhatsAppMessage,
            frappe.get_doc("WhatsApp Message", message_name),
        )
        self.assertFalse(bool(message_doc.attach))

        metadata_response = Mock()
        metadata_response.raise_for_status.return_value = None
        metadata_response.json.return_value = {
            "url": "https://graph.facebook.com/media-download",
            "mime_type": "video/mp4",
        }

        content_response = Mock()
        content_response.raise_for_status.return_value = None
        content_response.content = b"video-bytes"

        forwarded_attach_values: list[str | None] = []

        def capture_forward(*, incoming_message_name: str):
            forwarded_attach_values.append(
                cast(
                    str | None,
                    frappe.db.get_value(
                        "WhatsApp Message",
                        incoming_message_name,
                        "attach",
                    ),
                )
            )

        with patch(
            "frappe_whatsapp.utils.webhook.requests.get",
            side_effect=[metadata_response, content_response],
        ), patch(
            "frappe_whatsapp.utils.webhook.forward_incoming_to_app_async",
            side_effect=capture_forward,
        ) as forward_after_attach_mock:
            download_and_attach_media(
                whatsapp_account_name=self.account_name,
                message_docname=str(message_doc.name),
                media_id="media-123",
                message_type="video",
            )

        message_doc.reload()
        self.assertTrue(bool(message_doc.attach))
        self.assertEqual(forward_after_attach_mock.call_count, 1)
        self.assertEqual(forwarded_attach_values, [message_doc.attach])

        file_name = frappe.db.get_value(
            "File",
            {
                "attached_to_doctype": "WhatsApp Message",
                "attached_to_name": message_doc.name,
                "attached_to_field": "attach",
                "file_url": message_doc.attach,
            },
            "name",
        )
        assert file_name
        self.created_file_names.append(str(file_name))
