# Copyright (c) 2022, Shridhar Patil and Contributors
# See license.txt

from unittest.mock import MagicMock, patch

import frappe
from frappe.tests.utils import FrappeTestCase

from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_notification.whatsapp_notification import (  # noqa: E501
    WhatsAppNotification,
)


class TestWhatsAppNotification(FrappeTestCase):
    @patch(
        "frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_notification."
        "whatsapp_notification.frappe.msgprint"
    )
    @patch(
        "frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_notification."
        "whatsapp_notification.request_meta_json"
    )
    @patch(
        "frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_notification."
        "whatsapp_notification.frappe.get_doc"
    )
    def test_scheduled_failure_uses_detailed_meta_error(
        self, mock_get_doc, mock_request, mock_msgprint
    ):
        account = frappe._dict({
            "name": "Test Account",
            "url": "https://graph.facebook.com",
            "version": "v24.0",
            "phone_id": "phone-123",
        })
        account.get_password = lambda _fieldname: "token-123"
        notification_log = MagicMock()

        def get_doc(doctype, *args):
            if doctype == "WhatsApp Account":
                return account
            if isinstance(doctype, dict):
                return notification_log
            raise AssertionError(f"Unexpected get_doc call: {doctype}, {args}")

        mock_get_doc.side_effect = get_doc
        detail = (
            "WhatsApp Account Test Account: scheduled notification send "
            "failed. Invalid payload Details: components must not be empty "
            "(code 100, subcode 2494073)"
        )
        mock_request.side_effect = frappe.ValidationError(detail)
        notification = WhatsAppNotification({
            "doctype": "WhatsApp Notification",
            "template": "test-template",
        })
        payload = {
            "type": "template",
            "template": {
                "name": "test-template",
                "language": {"code": "en_US"},
                "components": [],
            },
        }

        notification.notify(payload, template_account="Test Account")

        self.assertNotIn("components", payload["template"])
        self.assertEqual(mock_request.call_args.kwargs["json_body"], payload)
        self.assertIn(detail, mock_msgprint.call_args.args[0])
        notification_log.insert.assert_called_once_with(ignore_permissions=True)
