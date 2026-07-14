from __future__ import annotations

import socket
import time
from contextlib import nullcontext
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import MagicMock, call, patch

import frappe
from frappe.tests.utils import FrappeTestCase
from frappe.utils import now_datetime

from frappe_whatsapp.utils.calling import (
    _build_originate_payload,
    _get_account,
    _permission_action_allowed,
    _read_ami_banner,
    _send_ami_originate,
    _send_permission_template,
    get_call_state,
    handle_call_permission_reply,
    originate_pending_call,
    parse_permission_state,
    request_call_permission,
    start_outbound_call,
)

if TYPE_CHECKING:
    from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_account.whatsapp_account import WhatsAppAccount
    from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_call.whatsapp_call import WhatsAppCall
    from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_call_permission.whatsapp_call_permission import WhatsAppCallPermission
    from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_message.whatsapp_message import WhatsAppMessage


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

    def test_parse_live_meta_permission_envelope(self):
        payload = {
            "messaging_product": "whatsapp",
            "permission": {"status": "permanent"},
            "actions": [{
                "action_name": "start_call",
                "can_perform_action": True,
            }],
        }

        parsed = parse_permission_state(payload)

        self.assertEqual(parsed["permission_status"], "Permanent")
        self.assertEqual(parsed["is_permanent"], 1)


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

        payload = _build_originate_payload(
            cast(Any, settings), cast(Any, call_doc), "action-1")

        self.assertEqual(payload["Action"], "Originate")
        self.assertEqual(payload["ActionID"], "action-1")
        self.assertEqual(payload["Channel"], "Local/1001@from-internal")
        self.assertEqual(payload["Context"], "from-internal")
        self.assertEqual(payload["Exten"], "WA15551234567")
        self.assertEqual(payload["Timeout"], "45000")
        self.assertEqual(payload["Variable"], "WHATSAPP_CALL_ID=CALL-1")

    def _ami_settings(self) -> Any:
        settings = SimpleNamespace(
            ami_host="172.31.1.252",
            ami_port=5038,
            ami_username="frappe_whatsapp",
            ami_use_tls=0,
            originate_timeout=45,
            agent_channel_template="Local/{extension}@from-internal",
            destination_number_template="829944{number}",
            destination_context="from-internal",
        )
        settings.get_password = lambda _fieldname: "test-ami-secret"
        return settings

    def _ami_call(self) -> Any:
        return SimpleNamespace(
            name="CALL-1",
            phone_number="+12015550123",
            agent_extension="847",
        )

    def test_read_ami_banner_accepts_production_single_line_frame(self):
        sock = MagicMock()
        sock.recv.return_value = b"Asterisk Call Manager/11.0.0\r\n"

        banner = _read_ami_banner(sock)

        self.assertEqual(banner, "Asterisk Call Manager/11.0.0")
        sock.recv.assert_called_once_with(4096)

    def test_read_ami_banner_rejects_unexpected_data(self):
        sock = MagicMock()
        sock.recv.return_value = b"unexpected server\r\n"

        with self.assertRaises(frappe.ValidationError) as raised:
            _read_ami_banner(sock)

        self.assertIn("unexpected server banner", str(raised.exception))

    @patch("frappe_whatsapp.utils.calling._read_ami_banner")
    @patch("frappe_whatsapp.utils.calling._send_ami_action")
    @patch("frappe_whatsapp.utils.calling.socket.create_connection")
    def test_ami_originate_disables_events_and_logs_off(
        self, mock_connect, mock_action, mock_banner
    ):
        sock = MagicMock()
        mock_connect.return_value = sock
        mock_banner.return_value = "Asterisk Call Manager/11.0.0"
        mock_action.side_effect = [
            {"Response": "Success", "Message": "Authentication accepted"},
            {
                "Response": "Success",
                "Message": "Originate successfully queued",
                "ActionID": "action-1",
            },
            {"Response": "Goodbye"},
        ]

        response = _send_ami_originate(
            self._ami_settings(), self._ami_call(), "action-1")

        self.assertEqual(response["Response"], "Success")
        self.assertEqual(
            mock_action.call_args_list,
            [
                call(sock, {
                    "Action": "Login",
                    "Username": "frappe_whatsapp",
                    "Secret": "test-ami-secret",
                    "Events": "off",
                }),
                call(sock, {
                    "Action": "Originate",
                    "ActionID": "action-1",
                    "Channel": "Local/847@from-internal",
                    "Context": "from-internal",
                    "Exten": "82994412015550123",
                    "Priority": "1",
                    "Timeout": "45000",
                    "CallerID": "WhatsApp <847>",
                    "Async": "true",
                    "Variable": "WHATSAPP_CALL_ID=CALL-1",
                }),
                call(sock, {"Action": "Logoff"}),
            ],
        )
        mock_banner.assert_called_once_with(sock)
        sock.close.assert_called_once()

    @patch("frappe_whatsapp.utils.calling._read_ami_banner")
    @patch("frappe_whatsapp.utils.calling._send_ami_action")
    @patch("frappe_whatsapp.utils.calling.socket.create_connection")
    def test_ami_login_rejection_is_preserved_without_secret(
        self, mock_connect, mock_action, mock_banner
    ):
        sock = MagicMock()
        mock_connect.return_value = sock
        mock_banner.return_value = "Asterisk Call Manager/11.0.0"
        mock_action.side_effect = [
            {"Response": "Error", "Message": "Authentication failed"},
            {"Response": "Goodbye"},
        ]

        with self.assertRaises(frappe.ValidationError) as raised:
            _send_ami_originate(
                self._ami_settings(), self._ami_call(), "action-1")

        self.assertEqual(str(raised.exception), "Authentication failed")
        self.assertNotIn("test-ami-secret", str(raised.exception))
        self.assertEqual(
            mock_action.call_args_list[-1],
            call(sock, {"Action": "Logoff"}),
        )
        sock.close.assert_called_once()

    @patch("frappe_whatsapp.utils.calling._read_ami_banner")
    @patch("frappe_whatsapp.utils.calling._send_ami_action")
    @patch("frappe_whatsapp.utils.calling.socket.create_connection")
    def test_ami_login_requires_explicit_success(
        self, mock_connect, mock_action, mock_banner
    ):
        sock = MagicMock()
        mock_connect.return_value = sock
        mock_banner.return_value = "Asterisk Call Manager/11.0.0"
        mock_action.side_effect = [{"Event": "FullyBooted"}, {}]

        with self.assertRaises(frappe.ValidationError) as raised:
            _send_ami_originate(
                self._ami_settings(), self._ami_call(), "action-1")

        self.assertIn("unexpected response", str(raised.exception))
        sock.close.assert_called_once()

    @patch("frappe_whatsapp.utils.calling._read_ami_banner")
    @patch("frappe_whatsapp.utils.calling._send_ami_action")
    @patch("frappe_whatsapp.utils.calling.socket.create_connection")
    def test_ami_originate_rejects_mismatched_action_id(
        self, mock_connect, mock_action, mock_banner
    ):
        sock = MagicMock()
        mock_connect.return_value = sock
        mock_banner.return_value = "Asterisk Call Manager/11.0.0"
        mock_action.side_effect = [
            {"Response": "Success"},
            {"Response": "Success", "ActionID": "another-action"},
            {"Response": "Goodbye"},
        ]

        with self.assertRaises(frappe.ValidationError) as raised:
            _send_ami_originate(
                self._ami_settings(), self._ami_call(), "action-1")

        self.assertIn("unexpected action", str(raised.exception))
        sock.close.assert_called_once()

    @patch("frappe_whatsapp.utils.calling.socket.create_connection")
    def test_ami_connection_timeout_does_not_expose_secret(self, mock_connect):
        mock_connect.side_effect = socket.timeout("timed out")

        with self.assertRaises(socket.timeout) as raised:
            _send_ami_originate(
                self._ami_settings(), self._ami_call(), "action-1")

        self.assertNotIn("test-ami-secret", str(raised.exception))

    def test_default_calling_account_comes_from_permission_template(self):
        suffix = frappe.generate_hash(length=8)
        default_outgoing = cast("WhatsAppAccount", frappe.get_doc({
            "doctype": "WhatsApp Account",
            "account_name": f"Default Outgoing {suffix}",
            "status": "Active",
            "is_default_outgoing": 1,
            "phone_id": f"default-phone-{suffix}",
            "webhook_verify_token": f"default-verify-{suffix}",
        }).insert(ignore_permissions=True))
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


class TestWhatsAppCallingActions(FrappeTestCase):
    def setUp(self):
        frappe.set_user("Administrator")
        for doctype in [
            "whatsapp_account",
            "whatsapp_call",
            "whatsapp_call_permission",
        ]:
            frappe.reload_doc("frappe_whatsapp", "doctype", doctype)

    def _create_account(self) -> WhatsAppAccount:
        suffix = frappe.generate_hash(length=8)
        return cast("WhatsAppAccount", frappe.get_doc({
            "doctype": "WhatsApp Account",
            "account_name": f"Call Action Account {suffix}",
            "status": "Active",
            "url": "https://graph.facebook.com",
            "version": "v24.0",
            "phone_id": f"phone-{suffix}",
            "webhook_verify_token": f"verify-{suffix}",
        }).insert(ignore_permissions=True))

    def _permission(
        self,
        status="No Permission",
        *,
        can_request=True,
        can_start=None,
    ):
        if can_start is None:
            can_start = status in {"Temporary", "Permanent"}
        return SimpleNamespace(
            permission_status=status,
            expires_at=None,
            raw_meta_state={
                "actions": [{
                    "action_name": "send_call_permission_request",
                    "can_perform_action": can_request,
                }, {
                    "action_name": "start_call",
                    "can_perform_action": can_start,
                }],
            },
            last_checked_at=now_datetime(),
        )

    def _service_patches(self, account, permission):
        settings = SimpleNamespace(
            enabled=1,
            call_permission_template="call_permission-en_US",
        )
        return (
            patch("frappe_whatsapp.utils.calling._ensure_enabled", return_value=settings),
            patch("frappe_whatsapp.utils.calling._get_account", return_value=account),
            patch(
                "frappe_whatsapp.utils.calling._get_agent",
                return_value=SimpleNamespace(extension="847"),
            ),
            patch(
                "frappe_whatsapp.utils.calling.refresh_permission_state",
                return_value=permission,
            ),
        )

    def test_permission_action_parses_stored_json(self):
        permission = SimpleNamespace(raw_meta_state='{"actions": [{'
            '"action_name": "send_call_permission_request", '
            '"can_perform_action": true}]}')

        self.assertIs(
            _permission_action_allowed(
                permission, "send_call_permission_request"),
            True,
        )

    def test_request_is_idempotent_across_duplicate_contact_rooms(self):
        account = self._create_account()
        permission = self._permission()
        patches = self._service_patches(account, permission)

        with (
            patches[0], patches[1], patches[2], patches[3],
            patch(
                "frappe_whatsapp.utils.calling._validate_call_permission_template"
            ),
            patch(
                "frappe_whatsapp.utils.calling.filelock",
                side_effect=lambda *_args, **_kwargs: nullcontext(),
            ),
            patch(
                "frappe_whatsapp.utils.calling._send_permission_template",
                side_effect=lambda *, call_doc, settings: {
                    "status": call_doc.status,
                    "call": call_doc.name,
                    "message": "sent",
                    "waiting_for_permission": True,
                },
            ) as mock_send,
        ):
            first = request_call_permission(
                phone_number="+1 (555) 123-4567",
                contact="room-a",
                agent_user="Administrator",
            )
            second = request_call_permission(
                phone_number="15551234567",
                contact="room-b",
                agent_user="Administrator",
            )

        self.assertEqual(first["message"], "sent")
        self.assertEqual(second["call"], first["call"])
        self.assertIn("already waiting", second["message"])
        mock_send.assert_called_once()
        self.assertEqual(
            frappe.db.count(
                "WhatsApp Call",
                {
                    "phone_number": "15551234567",
                    "whatsapp_account": account.name,
                    "status": "Permission Requested",
                },
            ),
            1,
        )

    def test_active_permission_does_not_send_request(self):
        account = self._create_account()
        permission = self._permission("Permanent", can_request=False)
        patches = self._service_patches(account, permission)

        with (
            patches[0], patches[1], patches[2], patches[3],
            patch(
                "frappe_whatsapp.utils.calling._validate_call_permission_template"
            ),
            patch(
                "frappe_whatsapp.utils.calling.filelock",
                side_effect=lambda *_args, **_kwargs: nullcontext(),
            ),
            patch(
                "frappe_whatsapp.utils.calling._send_permission_template"
            ) as mock_send,
        ):
            result = request_call_permission(
                phone_number="15551234568",
                agent_user="Administrator",
            )

        self.assertEqual(result["status"], "Ready")
        mock_send.assert_not_called()

    def test_direct_extension_requests_permission_without_agent_mapping(self):
        account = self._create_account()
        permission = self._permission()

        with (
            patch(
                "frappe_whatsapp.utils.calling._ensure_enabled",
                return_value=SimpleNamespace(
                    enabled=1,
                    call_permission_template="call_permission-en_US",
                ),
            ),
            patch(
                "frappe_whatsapp.utils.calling._get_account",
                return_value=account,
            ),
            patch(
                "frappe_whatsapp.utils.calling.refresh_permission_state",
                return_value=permission,
            ),
            patch(
                "frappe_whatsapp.utils.calling._validate_call_permission_template"
            ),
            patch(
                "frappe_whatsapp.utils.calling.filelock",
                side_effect=lambda *_args, **_kwargs: nullcontext(),
            ),
            patch(
                "frappe_whatsapp.utils.calling._send_permission_template",
                side_effect=lambda *, call_doc, settings: {
                    "ok": True,
                    "status": call_doc.status,
                    "call": call_doc.name,
                    "message": "sent",
                    "waiting_for_permission": True,
                },
            ),
            patch("frappe_whatsapp.utils.calling._get_agent") as mock_agent,
        ):
            result = request_call_permission(
                phone_number="15551234580",
                agent_extension="9555",
                source_app=None,
                external_reference="lead-direct",
                idempotency_key="permission-direct-1234",
            )

        mock_agent.assert_not_called()
        call_doc = cast(
            "WhatsAppCall",
            frappe.get_doc("WhatsApp Call", result["call"]),
        )
        self.assertIsNone(call_doc.agent_user)
        self.assertEqual(call_doc.agent_extension, "9555")
        self.assertEqual(call_doc.action_type, "Permission Request")
        self.assertEqual(call_doc.external_reference, "lead-direct")

    def test_meta_disallowed_permission_request_is_blocked(self):
        account = self._create_account()
        permission = self._permission(can_request=False)
        patches = self._service_patches(account, permission)

        with (
            patches[0], patches[1], patches[2], patches[3],
            patch(
                "frappe_whatsapp.utils.calling._validate_call_permission_template"
            ),
            patch(
                "frappe_whatsapp.utils.calling.filelock",
                side_effect=lambda *_args, **_kwargs: nullcontext(),
            ),
            patch(
                "frappe_whatsapp.utils.calling._send_permission_template"
            ) as mock_send,
        ):
            result = request_call_permission(
                phone_number="15551234569",
                agent_user="Administrator",
            )

        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "Unavailable")
        self.assertIn("does not currently allow", result["message"])
        mock_send.assert_not_called()

    @patch("frappe_whatsapp.utils.calling.originate_call")
    @patch("frappe_whatsapp.utils.calling._create_call")
    @patch("frappe_whatsapp.utils.calling.refresh_permission_state")
    @patch("frappe_whatsapp.utils.calling._get_agent")
    @patch("frappe_whatsapp.utils.calling._get_account")
    @patch("frappe_whatsapp.utils.calling._ensure_enabled")
    def test_call_without_permission_never_sends_template_or_originates(
        self,
        mock_enabled,
        mock_account,
        mock_agent,
        mock_refresh,
        mock_create,
        mock_originate,
    ):
        mock_enabled.return_value = SimpleNamespace(enabled=1)
        mock_account.return_value = SimpleNamespace(name="calling-account")
        mock_agent.return_value = SimpleNamespace(extension="847")
        mock_refresh.return_value = self._permission()

        result = start_outbound_call(
            phone_number="15551234570",
            agent_user="Administrator",
        )

        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "No Permission")
        self.assertIn("Active WhatsApp call permission", result["message"])
        mock_create.assert_not_called()
        mock_originate.assert_not_called()

    @patch("frappe_whatsapp.utils.calling.originate_call")
    @patch("frappe_whatsapp.utils.calling._create_call")
    @patch("frappe_whatsapp.utils.calling.refresh_permission_state")
    @patch("frappe_whatsapp.utils.calling._get_agent")
    @patch("frappe_whatsapp.utils.calling._get_account")
    @patch("frappe_whatsapp.utils.calling._ensure_enabled")
    def test_call_with_permission_originates_once(
        self,
        mock_enabled,
        mock_account,
        mock_agent,
        mock_refresh,
        mock_create,
        mock_originate,
    ):
        call_doc = SimpleNamespace(
            name="CALL-1",
            status="PBX Queued",
            idempotency_key=None,
            failure_reason=None,
        )
        mock_enabled.return_value = SimpleNamespace(enabled=1)
        mock_account.return_value = SimpleNamespace(name="calling-account")
        mock_agent.return_value = SimpleNamespace(extension="847")
        mock_refresh.return_value = self._permission(
            "Permanent", can_request=False)
        mock_create.return_value = call_doc

        result = start_outbound_call(
            phone_number="15551234571",
            contact="room-a",
            agent_user="Administrator",
        )

        self.assertEqual(result["call"], "CALL-1")
        self.assertTrue(result["ok"])
        mock_originate.assert_called_once_with(
            call_doc, raise_on_failure=False)

    @patch("frappe_whatsapp.utils.calling.originate_call")
    @patch("frappe_whatsapp.utils.calling._create_call")
    @patch("frappe_whatsapp.utils.calling.refresh_permission_state")
    @patch("frappe_whatsapp.utils.calling._get_agent")
    @patch("frappe_whatsapp.utils.calling._get_account")
    @patch("frappe_whatsapp.utils.calling._ensure_enabled")
    def test_direct_extension_originates_without_agent_mapping(
        self,
        mock_enabled,
        mock_account,
        mock_agent,
        mock_refresh,
        mock_create,
        mock_originate,
    ):
        call_doc = SimpleNamespace(
            name="CALL-DIRECT",
            status="PBX Queued",
            idempotency_key="call-direct-1234",
            failure_reason=None,
        )
        mock_enabled.return_value = SimpleNamespace(enabled=1)
        mock_account.return_value = SimpleNamespace(name="calling-account")
        mock_refresh.return_value = self._permission(
            "Permanent", can_request=False)
        mock_create.return_value = call_doc

        result = start_outbound_call(
            phone_number="15551234573",
            agent_extension="9999",
            idempotency_key="call-direct-1234",
        )

        self.assertTrue(result["ok"])
        mock_agent.assert_not_called()
        self.assertIsNone(mock_create.call_args.kwargs["agent_user"])
        self.assertEqual(mock_create.call_args.kwargs["agent_extension"], "9999")
        mock_originate.assert_called_once_with(
            call_doc, raise_on_failure=False)

    @patch("frappe_whatsapp.utils.calling._get_account")
    @patch("frappe_whatsapp.utils.calling._ensure_enabled")
    def test_invalid_direct_extension_is_rejected_before_meta_database_or_ami(
        self, mock_enabled, mock_account
    ):
        mock_enabled.return_value = SimpleNamespace(enabled=1)
        mock_account.return_value = SimpleNamespace(name="calling-account")

        for extension in (
            " 847",
            "847 ",
            "847@from-internal",
            "847\r\nVariable: EVIL=1",
            "PJSIP/847",
            "１２３",
            "12345678901",
        ):
            with self.subTest(extension=extension):
                with (
                    patch("frappe_whatsapp.utils.calling._get_agent") as mock_agent,
                    patch(
                        "frappe_whatsapp.utils.calling.refresh_permission_state"
                    ) as mock_refresh,
                    patch("frappe_whatsapp.utils.calling._create_call") as mock_create,
                    patch("frappe_whatsapp.utils.calling.originate_call") as mock_originate,
                    self.assertRaises(frappe.ValidationError),
                ):
                    start_outbound_call(
                        phone_number="15551234574",
                        agent_extension=extension,
                        idempotency_key="invalid-extension-1",
                    )
                mock_agent.assert_not_called()
                mock_refresh.assert_not_called()
                mock_create.assert_not_called()
                mock_originate.assert_not_called()
        mock_enabled.assert_not_called()
        mock_account.assert_not_called()

    def test_idempotent_direct_call_retry_originates_only_once(self):
        account = self._create_account()
        permission = self._permission("Permanent", can_request=False)
        settings = SimpleNamespace(enabled=1)

        def queue_call(call_doc, **_kwargs):
            call_doc.status = "PBX Queued"
            call_doc.save(ignore_permissions=True)

        with (
            patch(
                "frappe_whatsapp.utils.calling._ensure_enabled",
                return_value=settings,
            ),
            patch(
                "frappe_whatsapp.utils.calling._get_account",
                return_value=account,
            ),
            patch(
                "frappe_whatsapp.utils.calling.refresh_permission_state",
                return_value=permission,
            ),
            patch(
                "frappe_whatsapp.utils.calling.filelock",
                side_effect=lambda *_args, **_kwargs: nullcontext(),
            ),
            patch(
                "frappe_whatsapp.utils.calling.originate_call",
                side_effect=queue_call,
            ) as mock_originate,
            patch("frappe_whatsapp.utils.calling._get_agent") as mock_agent,
        ):
            first = start_outbound_call(
                phone_number="15551234575",
                agent_extension="9123",
                idempotency_key="outbound-call-retry-1234",
            )
            second = start_outbound_call(
                phone_number="15551234575",
                agent_extension="9123",
                idempotency_key="outbound-call-retry-1234",
            )

        self.assertTrue(first["ok"])
        self.assertEqual(second["call"], first["call"])
        self.assertTrue(second["idempotent_replay"])
        mock_originate.assert_called_once()
        mock_agent.assert_not_called()

    def test_idempotency_key_cannot_be_reused_for_another_call(self):
        account = self._create_account()
        existing = cast("WhatsAppCall", frappe.get_doc({
            "doctype": "WhatsApp Call",
            "phone_number": "15551234576",
            "whatsapp_account": account.name,
            "agent_extension": "9001",
            "status": "PBX Queued",
            "action_type": "Outbound Call",
            "idempotency_key": "conflicting-call-key-1234",
        }).insert(ignore_permissions=True))

        with (
            patch(
                "frappe_whatsapp.utils.calling._ensure_enabled",
                return_value=SimpleNamespace(enabled=1),
            ),
            patch(
                "frappe_whatsapp.utils.calling._get_account",
                return_value=account,
            ),
            patch(
                "frappe_whatsapp.utils.calling.filelock",
                side_effect=lambda *_args, **_kwargs: nullcontext(),
            ),
            patch(
                "frappe_whatsapp.utils.calling.refresh_permission_state"
            ) as mock_refresh,
            self.assertRaises(frappe.ValidationError),
        ):
            start_outbound_call(
                phone_number="15551234577",
                agent_extension="9001",
                idempotency_key=existing.idempotency_key,
            )

        mock_refresh.assert_not_called()
        self.assertEqual(frappe.local.response.get("http_status_code"), 409)
        frappe.local.response.pop("http_status_code", None)

    def test_ami_failure_is_returned_and_persisted(self):
        account = self._create_account()
        permission = self._permission("Permanent", can_request=False)

        with (
            patch(
                "frappe_whatsapp.utils.calling._ensure_enabled",
                return_value=SimpleNamespace(enabled=1),
            ),
            patch(
                "frappe_whatsapp.utils.calling._get_account",
                return_value=account,
            ),
            patch(
                "frappe_whatsapp.utils.calling.refresh_permission_state",
                return_value=permission,
            ),
            patch(
                "frappe_whatsapp.utils.calling.filelock",
                side_effect=lambda *_args, **_kwargs: nullcontext(),
            ),
            patch(
                "frappe_whatsapp.utils.calling._send_ami_originate",
                side_effect=RuntimeError("AMI unavailable"),
            ),
            patch("frappe_whatsapp.utils.calling.publish_call_update"),
            patch("frappe_whatsapp.utils.calling.frappe.log_error"),
        ):
            result = start_outbound_call(
                phone_number="15551234578",
                agent_extension="9002",
                idempotency_key="failed-call-audit-1234",
            )

        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "Failed")
        self.assertTrue(result["retryable"])
        call_doc = cast(
            "WhatsAppCall",
            frappe.get_doc("WhatsApp Call", result["call"]),
        )
        self.assertEqual(call_doc.status, "Failed")
        self.assertEqual(call_doc.agent_user, None)
        self.assertEqual(call_doc.agent_extension, "9002")
        self.assertIn("AMI unavailable", str(call_doc.failure_reason))

    @patch("frappe_whatsapp.utils.calling.publish_call_update")
    @patch("frappe_whatsapp.utils.calling._upsert_permission")
    @patch("frappe_whatsapp.utils.calling._validate_call_permission_template")
    @patch("frappe_whatsapp.utils.calling.frappe.get_doc")
    def test_permission_message_keeps_crm_routing_fields(
        self, mock_get_doc, mock_validate_template, _mock_upsert, _mock_publish
    ):
        mock_validate_template.return_value = SimpleNamespace(name="call-template")
        message_doc = SimpleNamespace(
            name="MESSAGE-1",
            message_id="wamid.test",
            insert=MagicMock(),
        )
        mock_get_doc.return_value = message_doc
        call_doc = SimpleNamespace(
            name="CALL-ROUTING",
            phone_number="15551234579",
            whatsapp_account="account-a",
            source_app="crm-app",
            external_reference="lead-123",
            idempotency_key="permission-routing-1234",
            status="Permission Requested",
            requested_at=None,
            permission_request_message=None,
            save=MagicMock(),
        )

        _send_permission_template(
            call_doc=cast(Any, call_doc),
            settings=cast(
                Any, SimpleNamespace(call_permission_template="call-template")
            ),
        )

        message_values = mock_get_doc.call_args.args[0]
        self.assertEqual(message_values["source_app"], "crm-app")
        self.assertEqual(message_values["external_reference"], "lead-123")
        message_doc.insert.assert_called_once_with(ignore_permissions=True)

    @patch("frappe_whatsapp.utils.calling.refresh_permission_state")
    @patch("frappe_whatsapp.utils.calling._find_pending_call")
    @patch("frappe_whatsapp.utils.calling.get_local_permission")
    @patch("frappe_whatsapp.utils.calling._get_agent")
    @patch("frappe_whatsapp.utils.calling._get_account")
    @patch("frappe_whatsapp.utils.calling._get_settings")
    def test_call_state_exposes_separate_request_and_call_actions(
        self,
        mock_settings,
        mock_account,
        mock_agent,
        mock_local,
        mock_pending,
        mock_refresh,
    ):
        mock_settings.return_value = SimpleNamespace(
            enabled=1,
            call_permission_template="call_permission-en_US",
        )
        mock_account.return_value = SimpleNamespace(name="calling-account")
        mock_agent.return_value = SimpleNamespace(extension="847")
        mock_pending.return_value = None
        mock_local.return_value = self._permission()

        no_permission = get_call_state(
            phone_number="15551234572",
            contact="room-a",
            agent_user="Administrator",
        )
        self.assertFalse(no_permission["can_call"])
        self.assertTrue(no_permission["can_request_permission"])

        mock_local.return_value = self._permission(
            "Permanent", can_request=False)
        ready = get_call_state(
            phone_number="15551234572",
            contact="room-b",
            agent_user="Administrator",
        )
        self.assertTrue(ready["can_call"])
        self.assertFalse(ready["can_request_permission"])

        mock_local.return_value = self._permission(
            "Permanent", can_request=False, can_start=False)
        unavailable = get_call_state(
            phone_number="15551234572",
            contact="room-b",
            agent_user="Administrator",
        )
        self.assertEqual(unavailable["status"], "Unavailable")
        self.assertFalse(unavailable["can_call"])

        pending = SimpleNamespace(
            name="CALL-PENDING",
            status="Permission Requested",
            permission_responded_at=None,
            save=MagicMock(),
        )
        mock_pending.return_value = pending
        mock_local.return_value = self._permission(
            "Permanent", can_request=False)
        reconciled = get_call_state(
            phone_number="15551234572",
            contact="room-b",
            agent_user="Administrator",
        )
        self.assertEqual(reconciled["status"], "Ready")
        self.assertIsNone(reconciled["pending_call"])
        self.assertEqual(pending.status, "Permission Accepted")
        pending.save.assert_called_once_with(ignore_permissions=True)
        mock_refresh.assert_not_called()

        mock_agent.reset_mock()
        mock_pending.return_value = None
        mock_local.return_value = self._permission()
        direct = get_call_state(
            phone_number="15551234572",
            agent_extension="9555",
            whatsapp_account="calling-account",
        )
        self.assertEqual(direct["agent_extension"], "9555")
        self.assertIsNone(direct["agent_user"])
        mock_agent.assert_not_called()

    @patch("frappe_whatsapp.utils.calling.originate_call")
    @patch("frappe_whatsapp.utils.calling.frappe.get_doc")
    def test_legacy_accepted_job_cannot_originate(
        self, mock_get_doc, mock_originate
    ):
        call_doc = SimpleNamespace(status="Permission Accepted")
        mock_get_doc.return_value = call_doc

        result = originate_pending_call("CALL-1")

        self.assertIs(result, call_doc)
        mock_originate.assert_not_called()


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

    def _create_account(self) -> WhatsAppAccount:
        suffix = frappe.generate_hash(length=8)
        return cast("WhatsAppAccount", frappe.get_doc({
            "doctype": "WhatsApp Account",
            "account_name": f"Calling Test Account {suffix}",
            "status": "Active",
            "is_default_outgoing": 1,
            "url": "https://graph.facebook.com",
            "version": "v24.0",
            "phone_id": f"phone-{suffix}",
            "webhook_verify_token": f"verify-{suffix}",
        }).insert(ignore_permissions=True))

    def _create_permission_request_message(
        self, account: WhatsAppAccount, phone: str
    ) -> WhatsAppMessage:
        return cast("WhatsAppMessage", frappe.get_doc({
            "doctype": "WhatsApp Message",
            "type": "Outgoing",
            "to": phone,
            "content_type": "text",
            "message_type": "Template",
            "message": "",
            "message_id": f"wamid.{frappe.generate_hash(length=8)}",
            "whatsapp_account": account.name,
        }).insert(ignore_permissions=True))

    def _create_pending_call(
        self,
        account: WhatsAppAccount,
        phone: str,
        request_message: WhatsAppMessage,
    ) -> WhatsAppCall:
        return cast("WhatsAppCall", frappe.get_doc({
            "doctype": "WhatsApp Call",
            "phone_number": phone,
            "whatsapp_account": account.name,
            "contact": "test-room",
            "agent_user": "Administrator",
            "agent_extension": "1001",
            "status": "Permission Requested",
            "permission_request_message": request_message.name,
        }).insert(ignore_permissions=True))

    @patch("frappe_whatsapp.utils.calling.publish_call_update")
    @patch("frappe_whatsapp.utils.calling.originate_call")
    @patch("frappe_whatsapp.utils.calling.frappe.enqueue")
    def test_accept_reply_updates_permission_without_enqueuing_originate(
        self, mock_enqueue, mock_originate, mock_publish
    ):
        account = self._create_account()
        phone = "15551234567"
        request_message = self._create_permission_request_message(
            account, phone)
        call_doc = self._create_pending_call(account, phone, request_message)

        handle_call_permission_reply(
            contact_number=phone,
            whatsapp_account_name=str(account.name),
            response="accept",
            expiration_timestamp=int(time.time()) + 3600,
            response_source="user_action",
            context_message_id=request_message.message_id,
            message_doc_name="incoming-reply",
        )

        call_doc.reload()
        self.assertEqual(call_doc.status, "Permission Accepted")
        permission = cast(
            "WhatsAppCallPermission",
            frappe.get_doc(
                "WhatsApp Call Permission",
                f"{phone}-{account.name}",
            ),
        )
        self.assertEqual(permission.permission_status, "Temporary")
        mock_enqueue.assert_not_called()
        mock_originate.assert_not_called()
        self.assertIn(
            "Click Call",
            mock_publish.call_args.args[1],
        )

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
            whatsapp_account_name=str(account.name),
            response="reject",
            response_source="user_action",
            context_message_id=request_message.message_id,
            message_doc_name="incoming-reply",
        )

        call_doc.reload()
        self.assertEqual(call_doc.status, "Permission Rejected")
        mock_enqueue.assert_not_called()
