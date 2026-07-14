from unittest.mock import patch

import frappe
from frappe.tests.utils import FrappeTestCase

from frappe_whatsapp.frappe_whatsapp.api.calling import (
    get_call_state,
    request_call_permission,
    start_outbound_call,
)


class TestCRMCallingAPI(FrappeTestCase):
    def setUp(self):
        frappe.set_user("Administrator")
        self.role_patch = patch(
            "frappe_whatsapp.frappe_whatsapp.api.calling." "_require_calling_api_role"
        )
        self.role_patch.start()
        self.addCleanup(self.role_patch.stop)

    @staticmethod
    def _db_value(doctype, _filters, fieldname=None, *args, **kwargs):
        if doctype == "WhatsApp Account" and fieldname == "status":
            return "Active"
        if doctype == "WhatsApp Client App" and fieldname == "enabled":
            return 1
        if doctype == "WhatsApp Call Permission":
            return None
        return None

    @patch("frappe_whatsapp.frappe_whatsapp.api.calling.get_service_call_state")
    @patch("frappe_whatsapp.frappe_whatsapp.api.calling.frappe.db.get_value")
    def test_state_uses_unmapped_direct_extension(self, mock_get_value, mock_state):
        mock_get_value.side_effect = self._db_value
        mock_state.return_value = {
            "status": "No Permission",
            "message": "Permission required",
            "can_call": False,
            "can_request_permission": True,
            "pending_call": None,
        }

        result = get_call_state(
            phone_number="+1 (555) 123-4567",
            whatsapp_account="account-a",
            agent_extension="9876",
            source_app="crm-app",
            external_reference="lead-123",
        )

        self.assertEqual(result["status"], "permission_required")
        self.assertTrue(result["can_request_permission"])
        self.assertFalse(result["can_start_call"])
        self.assertEqual(result["agent_extension"], "9876")
        mock_state.assert_called_once_with(
            phone_number="15551234567",
            agent_extension="9876",
            whatsapp_account="account-a",
        )

    @patch("frappe_whatsapp.frappe_whatsapp.api.calling.get_service_call_state")
    @patch("frappe_whatsapp.frappe_whatsapp.api.calling.frappe.db.get_value")
    def test_invalid_extension_is_rejected_before_database_or_service(
        self, mock_get_value, mock_state
    ):
        with self.assertRaises(frappe.ValidationError):
            get_call_state(
                phone_number="15551234567",
                whatsapp_account="account-a",
                agent_extension="847\r\nVariable: EVIL=1",
                source_app="crm-app",
            )

        queried_doctypes = [call.args[0] for call in mock_get_value.call_args_list]
        self.assertNotIn("WhatsApp Account", queried_doctypes)
        self.assertNotIn("WhatsApp Client App", queried_doctypes)
        self.assertNotIn("WhatsApp Call Permission", queried_doctypes)
        mock_state.assert_not_called()

    @patch(
        "frappe_whatsapp.frappe_whatsapp.api.calling." "request_service_call_permission"
    )
    @patch("frappe_whatsapp.frappe_whatsapp.api.calling.frappe.db.get_value")
    def test_permission_request_returns_canonical_pending_response(
        self, mock_get_value, mock_request
    ):
        mock_get_value.side_effect = self._db_value
        mock_request.return_value = {
            "ok": True,
            "status": "Permission Requested",
            "message": "Call permission request sent.",
            "call": "CALL-1",
            "retryable": False,
        }

        result = request_call_permission(
            phone_number="15551234567",
            whatsapp_account="account-a",
            agent_extension="9876",
            source_app="crm-app",
            external_reference="lead-123",
            idempotency_key="permission-request-1234",
        )

        self.assertEqual(result["status"], "permission_pending")
        self.assertEqual(result["pending_call_id"], "CALL-1")
        self.assertEqual(result["idempotency_key"], "permission-request-1234")
        self.assertIsNone(result["call_id"])
        self.assertNotIn("agent_email", result)
        self.assertEqual(mock_request.call_args.kwargs["agent_extension"], "9876")
        self.assertNotIn("agent_user", mock_request.call_args.kwargs)

    @patch("frappe_whatsapp.frappe_whatsapp.api.calling." "start_service_outbound_call")
    @patch("frappe_whatsapp.frappe_whatsapp.api.calling.frappe.db.get_value")
    def test_start_call_distinguishes_pbx_queued_from_connected(
        self, mock_get_value, mock_start
    ):
        mock_get_value.side_effect = self._db_value
        mock_start.return_value = {
            "ok": True,
            "status": "PBX Queued",
            "message": "Calling your PBX extension now.",
            "call": "CALL-2",
            "retryable": False,
        }

        result = start_outbound_call(
            phone_number="15551234567",
            whatsapp_account="account-a",
            agent_extension="9876",
            source_app="crm-app",
            idempotency_key="outbound-request-1234",
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], "pbx_queued")
        self.assertEqual(result["call_id"], "CALL-2")
        self.assertFalse(result["can_start_call"])

    @patch(
        "frappe_whatsapp.frappe_whatsapp.api.calling.frappe.get_roles",
        return_value=[],
    )
    def test_role_is_required(self, _mock_roles):
        self.role_patch.stop()
        frappe.set_user("Guest")
        with self.assertRaises(frappe.PermissionError):
            get_call_state(
                phone_number="15551234567",
                whatsapp_account="account-a",
                agent_extension="9876",
                source_app="crm-app",
            )
