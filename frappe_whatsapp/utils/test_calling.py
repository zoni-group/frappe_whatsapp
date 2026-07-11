from __future__ import annotations

import time
from types import SimpleNamespace
from unittest.mock import patch

import frappe
from frappe.tests.utils import FrappeTestCase

from frappe_whatsapp.utils.calling import (
    _build_originate_payload,
    _get_account,
    handle_call_permission_reply,
    parse_permission_state,
)


class TestWhatsAppCallingPermissionState(FrappeTestCase):
    def test_parse_permission_state_variants(self):
        future = int(time.time()) + 3600
        past = int(time.time()) - 3600

        self.assertEqual(
            parse_permission_state({"data": []})["permission_status"],
            "No Permission",
        )
        self.assertEqual(
            parse_permission_state({
                "call_permission": {
                    "status": "temporary",
                    "expiration_timestamp": future,
                }
            })["permission_status"],
            "Temporary",
        )
        self.assertEqual(
            parse_permission_state({
                "call_permission": {
                    "status": "temporary",
                    "expiration_timestamp": past,
                }
            })["permission_status"],
            "Expired",
        )
        self.assertEqual(
            parse_permission_state({
                "call_permission_reply": {
                    "response": "accept",
                    "is_permanent": True,
                }
            })["permission_status"],
            "Permanent",
        )
        self.assertEqual(
            parse_permission_state({
                "call_permission_reply": {"response": "reject"}
            })["permission_status"],
            "Rejected",
        )


class TestWhatsAppCallingAMI(FrappeTestCase):
    def setUp(self):
        frappe.set_user("Administrator")
        for doctype in [
            "whatsapp_account",
            "whatsapp_templates",
            "whatsapp_calling_settings",
        ]:
            frappe.reload_doc("frappe_whatsapp", "doctype", doctype)

    def test_build_originate_payload_uses_configured_templates(self):
        settings = SimpleNamespace(
            agent_channel_template="Local/{extension}@from-internal",
            destination_number_template="WA{number}",
            destination_context="from-internal",
            originate_timeout=45,
        )
        call_doc = SimpleNamespace(
            name="CALL-1",
            phone_number="+15551234567",
            agent_extension="1001",
        )

        payload = _build_originate_payload(settings, call_doc, "action-1")

        self.assertEqual(payload["Action"], "Originate")
        self.assertEqual(payload["ActionID"], "action-1")
        self.assertEqual(payload["Channel"], "Local/1001@from-internal")
        self.assertEqual(payload["Context"], "from-internal")
        self.assertEqual(payload["Exten"], "WA15551234567")
        self.assertEqual(payload["Timeout"], "45000")
        self.assertEqual(payload["Variable"], "WHATSAPP_CALL_ID=CALL-1")

    def test_default_calling_account_comes_from_permission_template(self):
        suffix = frappe.generate_hash(length=8)
        default_outgoing = frappe.get_doc({
            "doctype": "WhatsApp Account",
            "account_name": f"Default Outgoing {suffix}",
            "status": "Active",
            "is_default_outgoing": 1,
            "phone_id": f"default-phone-{suffix}",
            "webhook_verify_token": f"default-verify-{suffix}",
        }).insert(ignore_permissions=True)
        calling_account = frappe.get_doc({
            "doctype": "WhatsApp Account",
            "account_name": f"Calling Account {suffix}",
            "status": "Active",
            "phone_id": f"calling-phone-{suffix}",
            "webhook_verify_token": f"calling-verify-{suffix}",
        }).insert(ignore_permissions=True)
        with patch(
            "frappe_whatsapp.frappe_whatsapp.doctype."
            "whatsapp_templates.whatsapp_templates.WhatsAppTemplates.after_insert"
        ):
            template = frappe.get_doc({
                "doctype": "WhatsApp Templates",
                "template_name": f"call_permission_{suffix}",
                "actual_name": f"call_permission_{suffix}",
                "template": "Can we call you?",
                "language": "en",
                "category": "UTILITY",
                "status": "APPROVED",
                "whatsapp_account": calling_account.name,
                "is_call_permission_request": 1,
            }).insert(ignore_permissions=True)

        frappe.db.set_single_value(
            "WhatsApp Calling Settings",
            "call_permission_template",
            template.name,
        )

        self.assertEqual(default_outgoing.is_default_outgoing, 1)
        self.assertEqual(_get_account().name, calling_account.name)


class TestWhatsAppCallingWebhook(FrappeTestCase):
    def setUp(self):
        frappe.set_user("Administrator")
        for doctype in [
            "whatsapp_account",
            "whatsapp_message",
            "whatsapp_call",
            "whatsapp_call_permission",
        ]:
            frappe.reload_doc("frappe_whatsapp", "doctype", doctype)

    def _create_account(self):
        suffix = frappe.generate_hash(length=8)
        return frappe.get_doc({
            "doctype": "WhatsApp Account",
            "account_name": f"Calling Test Account {suffix}",
            "status": "Active",
            "is_default_outgoing": 1,
            "url": "https://graph.facebook.com",
            "version": "v24.0",
            "phone_id": f"phone-{suffix}",
            "webhook_verify_token": f"verify-{suffix}",
        }).insert(ignore_permissions=True)

    def _create_permission_request_message(self, account, phone):
        return frappe.get_doc({
            "doctype": "WhatsApp Message",
            "type": "Outgoing",
            "to": phone,
            "content_type": "text",
            "message_type": "Template",
            "message": "",
            "message_id": f"wamid.{frappe.generate_hash(length=8)}",
            "whatsapp_account": account.name,
        }).insert(ignore_permissions=True)

    def _create_pending_call(self, account, phone, request_message):
        return frappe.get_doc({
            "doctype": "WhatsApp Call",
            "phone_number": phone,
            "whatsapp_account": account.name,
            "contact": "test-room",
            "agent_user": "Administrator",
            "agent_extension": "1001",
            "status": "Permission Requested",
            "permission_request_message": request_message.name,
        }).insert(ignore_permissions=True)

    @patch("frappe_whatsapp.utils.calling.publish_call_update")
    @patch("frappe_whatsapp.utils.calling.frappe.enqueue")
    def test_accept_reply_updates_permission_and_enqueues_originate(
        self, mock_enqueue, _mock_publish
    ):
        account = self._create_account()
        phone = "15551234567"
        request_message = self._create_permission_request_message(
            account, phone)
        call_doc = self._create_pending_call(account, phone, request_message)

        handle_call_permission_reply(
            contact_number=phone,
            whatsapp_account_name=account.name,
            response="accept",
            expiration_timestamp=int(time.time()) + 3600,
            response_source="user_action",
            context_message_id=request_message.message_id,
            message_doc_name="incoming-reply",
        )

        call_doc.reload()
        self.assertEqual(call_doc.status, "Permission Accepted")
        permission = frappe.get_doc(
            "WhatsApp Call Permission",
            f"{phone}-{account.name}",
        )
        self.assertEqual(permission.permission_status, "Temporary")
        mock_enqueue.assert_called_once()
        self.assertEqual(mock_enqueue.call_args.kwargs["call_name"], call_doc.name)

    @patch("frappe_whatsapp.utils.calling.publish_call_update")
    @patch("frappe_whatsapp.utils.calling.frappe.enqueue")
    def test_reject_reply_marks_call_rejected(
        self, mock_enqueue, _mock_publish
    ):
        account = self._create_account()
        phone = "15557654321"
        request_message = self._create_permission_request_message(
            account, phone)
        call_doc = self._create_pending_call(account, phone, request_message)

        handle_call_permission_reply(
            contact_number=phone,
            whatsapp_account_name=account.name,
            response="reject",
            response_source="user_action",
            context_message_id=request_message.message_id,
            message_doc_name="incoming-reply",
        )

        call_doc.reload()
        self.assertEqual(call_doc.status, "Permission Rejected")
        mock_enqueue.assert_not_called()
