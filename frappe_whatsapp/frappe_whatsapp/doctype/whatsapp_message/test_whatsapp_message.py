"""test Whatsapp messages."""
# Copyright (c) 2022, Shridhar Patil and Contributors
# See license.txt

import os
import tempfile
from unittest.mock import patch

import frappe
from frappe.tests.utils import FrappeTestCase

from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_message.whatsapp_message import (  # noqa: E501
    WhatsAppMessage,
)


class TestWhatsAppMessage(FrappeTestCase):
    """Test whatsapp messages."""

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
