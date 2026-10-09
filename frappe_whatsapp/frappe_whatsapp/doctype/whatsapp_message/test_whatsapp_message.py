"""test Whatsapp messages."""
# Copyright (c) 2022, Shridhar Patil and Contributors
# See license.txt

import os
import tempfile
from typing import Protocol, cast
from unittest.mock import patch

import frappe
from frappe.tests.utils import FrappeTestCase
from requests import Response

from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_message.whatsapp_message import (  # noqa: E501
    WhatsAppMessage,
    _get_integration_request_json,
)
from frappe_whatsapp.utils.identity import resolve_identity


class _AccountFixture(Protocol):
    name: str


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

    def _account(self) -> _AccountFixture:
        suffix = frappe.generate_hash(length=8)
        return cast(
            _AccountFixture,
            frappe.get_doc({
                "doctype": "WhatsApp Account",
                "account_name": f"Message Window Test {suffix}",
                "status": "Active",
                "token": "test-token",
                "url": "https://graph.facebook.com",
                "version": "v24.0",
                "phone_id": f"message-window-{suffix}",
            }).insert(ignore_permissions=True),
        )

    def _recent_incoming(
        self, *, account, profile, user_id: str, phone: str | None = None
    ):
        return frappe.get_doc({
            "doctype": "WhatsApp Message",
            "type": "Incoming",
            "from": phone,
            "from_user_id": user_id,
            "contact_profile": profile.name,
            "message": "Recent inbound message",
            "message_id": f"wamid.{frappe.generate_hash(length=16)}",
            "content_type": "text",
            "whatsapp_account": account.name,
        }).insert(ignore_permissions=True)

    def _manual_bsuid_message(self, *, account, recipient: str):
        return WhatsAppMessage({
            "doctype": "WhatsApp Message",
            "type": "Outgoing",
            "recipient": recipient,
            "message": "Reply over BSUID",
            "message_type": "Manual",
            "content_type": "text",
            "whatsapp_account": account.name,
        })

    def _create_phone_message(
        self, account, *, response=None, **overrides
    ) -> WhatsAppMessage:
        from frappe.api.v1 import create_doc

        values = {
            "to": "15550101999",
            "message": "Phone reply with known identity",
            "content_type": "text",
            "use_template": 0,
            "whatsapp_account": account.name,
        }
        values.update(overrides)
        module = (
            "frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_message."
            "whatsapp_message."
        )
        with patch(
            "frappe.api.v1.get_request_form_data", return_value=values
        ), patch(
            module + "get_service_window_status", return_value=(True, "")
        ), patch(
            module + "WhatsAppMessage._check_consent"
        ), patch(
            module + "request_meta_json",
            return_value=response or {"messages": [{"id": "wamid.identity-test"}]},
        ) as mock_meta:
            message = cast(WhatsAppMessage, create_doc("WhatsApp Message"))
        payload = mock_meta.call_args.kwargs["json_body"]
        self.assertEqual(payload["to"], values["to"])
        self.assertNotIn("recipient", payload)
        return message

    def test_direct_create_returns_and_persists_known_bsuids(self):
        account = self._account()
        user_id = f"US.{frappe.generate_hash(length=20)}"
        parent_id = f"US.ENT.{frappe.generate_hash(length=20)}"
        profile = resolve_identity(
            whatsapp_account=account.name,
            identity={
                "phone": "15550101999", "user_id": user_id,
                "parent_user_id": parent_id,
            },
        )
        message = self._create_phone_message(account)
        data = message.as_dict()
        self.assertEqual(data["recipient"], user_id)
        self.assertEqual(data["recipient_user_id"], user_id)
        self.assertEqual(data["recipient_parent_user_id"], parent_id)
        self.assertEqual(data["contact_profile"], profile.name)
        message.reload()
        self.assertEqual(message.recipient, user_id)
        self.assertEqual(message.recipient_user_id, user_id)
        self.assertEqual(message.recipient_parent_user_id, parent_id)

        resolve_identity(
            whatsapp_account=account.name,
            identity={
                "phone": "15550101999",
                "user_id": f"US.{frappe.generate_hash(length=20)}",
                "parent_user_id": f"US.ENT.{frappe.generate_hash(length=20)}",
            },
        )
        message.status = "delivered"
        message.save(ignore_permissions=True)
        message.reload()
        self.assertEqual(message.recipient, user_id)
        self.assertEqual(message.recipient_user_id, user_id)
        self.assertEqual(message.recipient_parent_user_id, parent_id)

    def test_direct_create_uses_parent_bsuid_when_regular_unknown(self):
        account = self._account()
        parent_id = f"US.ENT.{frappe.generate_hash(length=20)}"
        resolve_identity(
            whatsapp_account=account.name,
            identity={"phone": "15550101999", "parent_user_id": parent_id},
        )
        message = self._create_phone_message(account)
        self.assertEqual(message.recipient, parent_id)
        self.assertFalse(message.recipient_user_id)
        self.assertEqual(message.recipient_parent_user_id, parent_id)

    def test_direct_create_does_not_infer_bsuid_from_phone(self):
        message = self._create_phone_message(self._account())
        self.assertEqual(message.status, "Success")
        self.assertFalse(message.recipient)
        self.assertFalse(message.recipient_user_id)
        self.assertFalse(message.recipient_parent_user_id)

    def test_direct_create_uses_bsuid_learned_from_meta(self):
        account = self._account()
        old_id = f"US.{frappe.generate_hash(length=20)}"
        new_id = f"US.{frappe.generate_hash(length=20)}"
        resolve_identity(
            whatsapp_account=account.name,
            identity={"phone": "15550101999", "user_id": old_id},
        )
        message = self._create_phone_message(account, response={
            "messages": [{"id": "wamid.new-identity"}],
            "contacts": [{"wa_id": "15550101999", "user_id": new_id}],
        })
        self.assertEqual(message.as_dict()["recipient"], new_id)
        self.assertEqual(message.recipient_user_id, new_id)
        message.reload()
        self.assertEqual(message.recipient, new_id)

    def test_direct_create_preserves_explicit_recipient_with_phone(self):
        account = self._account()
        known_id = f"US.{frappe.generate_hash(length=20)}"
        explicit_id = f"US.{frappe.generate_hash(length=20)}"
        resolve_identity(
            whatsapp_account=account.name,
            identity={"phone": "15550101999", "user_id": known_id},
        )
        message = self._create_phone_message(account, recipient=explicit_id)
        self.assertEqual(message.recipient, explicit_id)
        self.assertEqual(message.recipient_user_id, known_id)

    def test_direct_create_respects_identity_scope(self):
        account = self._account()
        other_account = self._account()
        user_id = f"US.{frappe.generate_hash(length=20)}"
        resolve_identity(
            whatsapp_account=other_account.name,
            identity={"phone": "15550101999", "user_id": user_id},
        )
        isolated = self._create_phone_message(account)
        self.assertFalse(isolated.recipient_user_id)
        self.assertFalse(isolated.recipient)

        portfolio_id = frappe.generate_hash(length=20)
        frappe.db.set_value(
            "WhatsApp Account", account.name, "business_portfolio_id", portfolio_id
        )
        frappe.db.set_value(
            "WhatsApp Account", other_account.name, "business_portfolio_id", portfolio_id
        )
        resolve_identity(
            whatsapp_account=other_account.name,
            identity={"phone": "15550101998", "user_id": user_id},
        )
        shared = self._create_phone_message(account, to="15550101998")
        self.assertEqual(shared.recipient_user_id, user_id)
        self.assertEqual(shared.recipient, user_id)

    def test_bsuid_uses_linked_profile_window_before_validate(self):
        account = self._account()
        user_id = f"US.{frappe.generate_hash(length=20)}"
        phone = "15550101001"
        profile = resolve_identity(
            whatsapp_account=account.name,
            identity={"phone": phone, "user_id": user_id},
        )
        self._recent_incoming(
            account=account,
            profile=profile,
            user_id=user_id,
            phone=phone,
        )
        message = self._manual_bsuid_message(
            account=account, recipient=user_id
        )

        consent_result = frappe._dict(
            allowed=True, status="Bypassed", reason="Service window bypass"
        )
        with patch(
            "frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_message."
            "whatsapp_message.verify_consent_for_send",
            return_value=consent_result,
        ) as mock_consent, patch.object(
            message, "notify"
        ) as mock_notify:
            message.insert(ignore_permissions=True)

        self.assertEqual(message.contact_profile, profile.name)
        self.assertEqual(message.recipient_user_id, user_id)
        self.assertEqual(message.within_conversation_window, 1)
        payload = mock_notify.call_args.args[0]
        self.assertEqual(payload["recipient"], user_id)
        self.assertNotIn("to", payload)
        self.assertEqual(
            mock_consent.call_args.kwargs["contact_profile"], profile.name
        )
        self.assertTrue(
            mock_consent.call_args.kwargs["service_window_active"]
        )

    def test_phone_less_bsuid_profile_uses_profile_window(self):
        account = self._account()
        user_id = f"US.{frappe.generate_hash(length=20)}"
        profile = resolve_identity(
            whatsapp_account=account.name,
            identity={"user_id": user_id},
        )
        self._recent_incoming(
            account=account, profile=profile, user_id=user_id
        )
        message = self._manual_bsuid_message(
            account=account, recipient=user_id
        )

        with patch.object(
            message, "_check_consent"
        ), patch.object(message, "notify") as mock_notify:
            message.insert(ignore_permissions=True)

        self.assertEqual(message.contact_profile, profile.name)
        self.assertEqual(message.within_conversation_window, 1)
        self.assertEqual(
            mock_notify.call_args.args[0]["recipient"], user_id
        )

    def test_bsuid_without_recent_incoming_remains_outside_window(self):
        account = self._account()
        user_id = f"US.{frappe.generate_hash(length=20)}"
        message = self._manual_bsuid_message(
            account=account, recipient=user_id
        )

        settings = frappe._dict(
            window_hours=24,
            enforce_24_hour_window=1,
        )
        with patch.object(
            message, "_check_consent"
        ), patch.object(message, "notify") as mock_notify, patch(
            "frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_message."
            "whatsapp_message.get_compliance_settings",
            return_value=settings,
        ), patch(
            "frappe_whatsapp.utils.consent.get_compliance_settings",
            return_value=settings,
        ):
            with self.assertRaises(frappe.ValidationError) as raised:
                message.before_insert()

        self.assertTrue(message.contact_profile)
        self.assertIn(
            "No incoming message found from this contact",
            str(raised.exception),
        )
        mock_notify.assert_not_called()

    def test_resolved_identity_replaces_mismatched_contact_profile(self):
        account = self._account()
        selected_user_id = f"US.{frappe.generate_hash(length=20)}"
        other_user_id = f"US.{frappe.generate_hash(length=20)}"
        selected_profile = resolve_identity(
            whatsapp_account=account.name,
            identity={"user_id": selected_user_id},
        )
        other_profile = resolve_identity(
            whatsapp_account=account.name,
            identity={"user_id": other_user_id},
        )
        message = self._manual_bsuid_message(
            account=account, recipient=selected_user_id
        )
        message.contact_profile = other_profile.name

        message._resolve_outgoing_contact_profile()

        self.assertEqual(message.contact_profile, selected_profile.name)

    def test_template_send_sees_profile_resolved_before_insert(self):
        account = self._account()
        user_id = f"US.{frappe.generate_hash(length=20)}"
        profile = resolve_identity(
            whatsapp_account=account.name,
            identity={"user_id": user_id},
        )
        message = self._template_message(
            to=None,
            recipient=user_id,
            whatsapp_account=account.name,
            use_template=1,
        )
        profile_seen_by_template = []

        with patch(
            "frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_message."
            "whatsapp_message.get_service_window_status",
            return_value=(False, "closed"),
        ), patch.object(message, "_check_consent"), patch.object(
            message,
            "send_template",
            side_effect=lambda: profile_seen_by_template.append(
                message.contact_profile
            ),
        ):
            message.before_insert()

        self.assertEqual(profile_seen_by_template, [profile.name])

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
        mock_get_doc.return_value = self._template(language_code="pt_BR")
        message = self._template_message()

        with patch.object(message, "notify") as mock_notify:
            message.send_template()

        payload = mock_notify.call_args.args[0]
        self.assertNotIn("components", payload["template"])
        self.assertEqual(payload["template"]["language"]["code"], "pt_BR")

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

        with patch.object(message, "set_whatsapp_account"), patch.object(
            message, "_resolve_outgoing_contact_profile"
        ), patch(
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

    def test_contact_request_uses_meta_request_contact_info_shape(self):
        message = WhatsAppMessage({
            "doctype": "WhatsApp Message",
            "type": "Outgoing",
            "recipient": "US.13491208655302741918",
            "content_type": "contact_request",
            "message": "Please share your number.",
            "message_type": "Manual",
            "whatsapp_account": "Test Account",
        })

        with patch.object(message, "set_whatsapp_account"), patch.object(
            message, "_resolve_outgoing_contact_profile"
        ), patch.object(
            message, "_check_consent"
        ), patch(
            "frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_message."
            "whatsapp_message.get_service_window_status",
            return_value=(True, ""),
        ), patch.object(message, "notify") as mock_notify, patch.object(
            message, "create_whatsapp_profile"
        ):
            message.before_insert()

        payload = mock_notify.call_args.args[0]
        self.assertNotIn("to", payload)
        self.assertEqual(payload["recipient"], "US.13491208655302741918")
        self.assertEqual(payload["type"], "interactive")
        self.assertEqual(
            payload["interactive"],
            {
                "type": "request_contact_info",
                "body": {"text": "Please share your number."},
                "action": {"name": "request_contact_info"},
            },
        )

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
    def test_authentication_template_rejects_bsuid_only_recipient(
        self, mock_get_doc, _mock_compliance, _mock_rules
    ):
        mock_get_doc.return_value = self._template(category="AUTHENTICATION")
        message = self._template_message(
            to=None,
            recipient="US.13491208655302741918",
        )

        with patch.object(message, "notify") as mock_notify:
            with self.assertRaises(frappe.ValidationError):
                message.send_template()

        mock_notify.assert_not_called()

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
            "_resolve_outgoing_contact_profile",
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
            "_resolve_outgoing_contact_profile",
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
            "_resolve_outgoing_contact_profile",
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
