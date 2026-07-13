"""test Whatsapp messages."""
# Copyright (c) 2022, Shridhar Patil and Contributors
# See license.txt

import os
import tempfile
from unittest.mock import patch

import frappe
from frappe.tests.utils import FrappeTestCase
from requests import Response

from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_message.whatsapp_message import (  # noqa: E501
    WhatsAppMessage,
    _get_integration_request_json,
)


class TestWhatsAppMessage(FrappeTestCase):
    """Test whatsapp messages."""

    def _template(self, **overrides):
        values = {
            "actual_name": "test_template",
            "template_name": "test_template",
            "language_code": "en_US",
            "sample_values": "",
            "field_names": "",
            "header_type": "",
            "sample": "",
            "buttons": [],
            "is_call_permission_request": 0,
        }
        values.update(overrides)
        return frappe._dict(values)

    def _template_message(self, **overrides):
        values = {
            "doctype": "WhatsApp Message",
            "type": "Outgoing",
            "to": "15551234567",
            "message_type": "Template",
            "template": "test_template-en_US",
            "whatsapp_account": "Test Account",
        }
        values.update(overrides)
        return WhatsAppMessage(values)

    @patch(
        "frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_message."
        "whatsapp_message.enforce_template_send_rules"
    )
    @patch(
        "frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_message."
        "whatsapp_message.enforce_marketing_template_compliance"
    )
    @patch(
        "frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_message."
        "whatsapp_message.frappe.get_doc"
    )
    def test_parameterless_template_omits_components(
        self, mock_get_doc, _mock_compliance, _mock_rules
    ):
        mock_get_doc.return_value = self._template()
        message = self._template_message()

        with patch.object(message, "notify") as mock_notify:
            message.send_template()

        payload = mock_notify.call_args.args[0]
        self.assertNotIn("components", payload["template"])

    @patch(
        "frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_message."
        "whatsapp_message.enforce_template_send_rules"
    )
    @patch(
        "frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_message."
        "whatsapp_message.enforce_marketing_template_compliance"
    )
    @patch(
        "frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_message."
        "whatsapp_message.frappe.get_doc"
    )
    def test_template_runtime_parameters_keep_components(
        self, mock_get_doc, _mock_compliance, _mock_rules
    ):
        mock_get_doc.return_value = self._template(
            sample_values="Customer",
            header_type="IMAGE",
            buttons=[frappe._dict({
                "button_type": "Quick Reply",
                "button_label": "Confirm",
            })],
        )
        message = self._template_message(
            body_param='{"customer": "Oscar"}',
            attach="https://example.com/header.png",
        )

        with patch.object(message, "notify") as mock_notify:
            message.send_template()

        components = mock_notify.call_args.args[0]["template"]["components"]
        self.assertEqual(
            [component["type"] for component in components],
            ["body", "header", "button"],
        )
        self.assertEqual(
            components[0]["parameters"],
            [{"type": "text", "text": "Oscar"}],
        )

    @patch(
        "frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_message."
        "whatsapp_message.request_meta_json"
    )
    @patch(
        "frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_message."
        "whatsapp_message.frappe.get_doc"
    )
    def test_successful_send_populates_message_id(
        self, mock_get_doc, mock_request
    ):
        account = frappe._dict({
            "name": "Test Account",
            "url": "https://graph.facebook.com",
            "version": "v24.0",
            "phone_id": "phone-123",
        })
        account.get_password = lambda _fieldname: "token-123"
        mock_get_doc.return_value = account
        mock_request.return_value = {"messages": [{"id": "wamid.123"}]}
        message = self._template_message()
        payload = {"messaging_product": "whatsapp", "to": "15551234567"}

        message.notify(payload)

        self.assertEqual(message.message_id, "wamid.123")
        self.assertEqual(mock_request.call_args.kwargs["json_body"], payload)

    def test_400_response_json_is_not_discarded_as_falsy(self):
        response = Response()
        response.status_code = 400
        response._content = b'{"error": {"code": 100}}'
        previous = getattr(frappe.flags, "integration_request", None)
        frappe.flags.integration_request = response
        try:
            self.assertEqual(
                _get_integration_request_json(),
                {"error": {"code": 100}},
            )
        finally:
            frappe.flags.integration_request = previous

    @patch(
        "frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_message."
        "whatsapp_message.enforce_template_send_rules"
    )
    @patch(
        "frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_message."
        "whatsapp_message.enforce_marketing_template_compliance"
    )
    @patch(
        "frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_message."
        "whatsapp_message.frappe.get_doc"
    )
    def test_active_call_permission_blocks_post(
        self, mock_get_doc, _mock_compliance, _mock_rules
    ):
        mock_get_doc.return_value = self._template(
            is_call_permission_request=1)
        message = self._template_message()

        with patch(
            "frappe_whatsapp.utils.calling.refresh_permission_state",
            return_value={"permission_status": "Permanent"},
        ), patch.object(message, "notify") as mock_notify:
            with self.assertRaises(frappe.ValidationError) as raised:
                message.send_template()

        self.assertIn("already has permanent", str(raised.exception))
        self.assertIn("outbound-call workflow", str(raised.exception))
        mock_notify.assert_not_called()

    @patch(
        "frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_message."
        "whatsapp_message.enforce_template_send_rules"
    )
    @patch(
        "frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_message."
        "whatsapp_message.enforce_marketing_template_compliance"
    )
    @patch(
        "frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_message."
        "whatsapp_message.frappe.get_doc"
    )
    def test_inactive_call_permission_allows_one_post(
        self, mock_get_doc, _mock_compliance, _mock_rules
    ):
        mock_get_doc.return_value = self._template(
            is_call_permission_request=1)
        message = self._template_message()

        with patch(
            "frappe_whatsapp.utils.calling.refresh_permission_state",
            return_value={"permission_status": "No Permission"},
        ), patch.object(message, "notify") as mock_notify:
            message.send_template()

        mock_notify.assert_called_once()
        self.assertNotIn(
            "components", mock_notify.call_args.args[0]["template"])

    def test_before_insert_does_not_double_wrap_validation_error(self):
        message = self._template_message(use_template=1)

        with patch.object(message, "set_whatsapp_account"), patch(
            "frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_message."
            "whatsapp_message.get_service_window_status",
            return_value=(False, "closed"),
        ), patch.object(message, "_check_consent"), patch.object(
            message,
            "send_template",
            side_effect=frappe.ValidationError("specific Meta failure"),
        ):
            with self.assertRaises(frappe.ValidationError) as raised:
                message.before_insert()

        self.assertEqual(str(raised.exception), "specific Meta failure")
        self.assertNotIn("Failed to send template message", str(raised.exception))

    def test_upload_local_audio_to_whatsapp_uses_media_endpoint(self):
        with tempfile.NamedTemporaryFile(suffix=".ogg", delete=False) as f:
            f.write(b"OggS fake test audio")
            file_path = f.name

        file_doc = frappe._dict({"file_name": "voice-note.ogg"})
        file_doc.get_full_path = lambda: file_path

        account_doc = frappe._dict({
            "url": "https://graph.facebook.com",
            "version": "v19.0",
            "phone_id": "phone-123",
        })
        account_doc.get_password = lambda fieldname: "token-123"

        response = frappe._dict({"content": b'{"id":"media-123"}'})
        response.raise_for_status = lambda: None
        response.json = lambda: {"id": "media-123"}

        message_doc = WhatsAppMessage({
            "doctype": "WhatsApp Message",
            "attach": "/files/voice-note.ogg",
            "whatsapp_account": "Test Account",
        })

        try:
            with patch.object(
                message_doc,
                "_get_local_attachment_file",
                return_value=file_doc,
            ), patch(
                "frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_message."
                "whatsapp_message.frappe.get_doc",
                return_value=account_doc,
            ), patch(
                "frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_message."
                "whatsapp_message.requests.post",
                return_value=response,
            ) as mock_post:
                media_id = message_doc._upload_local_audio_to_whatsapp()
        finally:
            os.unlink(file_path)

        self.assertEqual(media_id, "media-123")
        call_kwargs = mock_post.call_args.kwargs
        # WhatsApp recipient clients require "codecs=opus" to recognize the
        # file as a playable voice note. Without it, the bubble appears but
        # tapping it shows "This audio is no longer available".
        self.assertEqual(
            call_kwargs["data"]["type"], "audio/ogg; codecs=opus")
        self.assertEqual(
            call_kwargs["files"]["file"][0],
            "voice-note.ogg",
        )
        self.assertEqual(
            call_kwargs["files"]["file"][2],
            "audio/ogg; codecs=opus",
        )

    def test_voice_note_upload_rejects_non_ogg_opus_attachment(self):
        with tempfile.NamedTemporaryFile(suffix=".m4a", delete=False) as f:
            f.write(b"fake m4a bytes")
            file_path = f.name

        file_doc = frappe._dict({"file_name": "voice-note.m4a"})
        file_doc.get_full_path = lambda: file_path

        message_doc = WhatsAppMessage({
            "doctype": "WhatsApp Message",
            "attach": "/files/voice-note.m4a",
            "whatsapp_account": "Test Account",
            "is_voice_note": 1,
        })

        try:
            with patch.object(
                message_doc,
                "_get_local_attachment_file",
                return_value=file_doc,
            ), patch(
                "frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_message."
                "whatsapp_message.requests.post",
            ) as mock_post:
                with self.assertRaises(frappe.ValidationError):
                    message_doc._upload_local_audio_to_whatsapp()
        finally:
            os.unlink(file_path)

        mock_post.assert_not_called()

    def test_voice_note_send_uses_media_id_and_voice_flag(self):
        message_doc = WhatsAppMessage({
            "doctype": "WhatsApp Message",
            "type": "Outgoing",
            "to": "15551234567",
            "content_type": "audio",
            "attach": "/files/voice-note.ogg",
            "message_type": "Manual",
            "whatsapp_account": "Test Account",
            "is_voice_note": 1,
        })

        with patch.object(
            message_doc,
            "set_whatsapp_account",
        ), patch.object(
            message_doc,
            "_check_consent",
        ), patch(
            "frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_message."
            "whatsapp_message.get_service_window_status",
            return_value=(True, ""),
        ), patch.object(
            message_doc,
            "_upload_local_audio_to_whatsapp",
            return_value="media-123",
        ), patch.object(
            message_doc,
            "notify",
        ) as mock_notify, patch.object(
            message_doc,
            "create_whatsapp_profile",
        ):
            message_doc.before_insert()

        payload = mock_notify.call_args.args[0]
        self.assertEqual(payload["type"], "audio")
        self.assertEqual(payload["audio"], {
            "id": "media-123",
            "voice": True,
        })

    def test_voice_note_requires_local_file_for_media_id_send(self):
        message_doc = WhatsAppMessage({
            "doctype": "WhatsApp Message",
            "type": "Outgoing",
            "to": "15551234567",
            "content_type": "audio",
            "attach": "https://example.com/voice-note.ogg",
            "message_type": "Manual",
            "whatsapp_account": "Test Account",
            "is_voice_note": 1,
        })

        with patch.object(
            message_doc,
            "set_whatsapp_account",
        ), patch.object(
            message_doc,
            "_check_consent",
        ), patch(
            "frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_message."
            "whatsapp_message.get_service_window_status",
            return_value=(True, ""),
        ), patch.object(
            message_doc,
            "_upload_local_audio_to_whatsapp",
            return_value=None,
        ), patch.object(
            message_doc,
            "notify",
        ) as mock_notify:
            with self.assertRaises(frappe.ValidationError):
                message_doc.before_insert()

        mock_notify.assert_not_called()

    def test_generic_remote_audio_still_sends_by_link(self):
        message_doc = WhatsAppMessage({
            "doctype": "WhatsApp Message",
            "type": "Outgoing",
            "to": "15551234567",
            "content_type": "audio",
            "attach": "https://example.com/audio.mp3",
            "message_type": "Manual",
            "whatsapp_account": "Test Account",
            "is_voice_note": 0,
        })

        with patch.object(
            message_doc,
            "set_whatsapp_account",
        ), patch.object(
            message_doc,
            "_check_consent",
        ), patch(
            "frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_message."
            "whatsapp_message.get_service_window_status",
            return_value=(True, ""),
        ), patch.object(
            message_doc,
            "_upload_local_audio_to_whatsapp",
            return_value=None,
        ), patch.object(
            message_doc,
            "notify",
        ) as mock_notify, patch.object(
            message_doc,
            "create_whatsapp_profile",
        ):
            message_doc.before_insert()

        payload = mock_notify.call_args.args[0]
        self.assertEqual(payload["type"], "audio")
        self.assertEqual(payload["audio"], {
            "link": "https://example.com/audio.mp3",
        })
