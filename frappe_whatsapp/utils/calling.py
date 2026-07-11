from __future__ import annotations

import json
import socket
import ssl
from datetime import datetime
from typing import Any

import frappe
import requests
from frappe import _
from frappe.utils import cint, now_datetime

from frappe_whatsapp.utils import format_number, get_whatsapp_account


CALL_UPDATE_EVENT = "whatsapp_call_update"
ACTIVE_PERMISSION_STATUSES = {"Temporary", "Permanent"}
TERMINAL_CALL_STATUSES = {"Permission Rejected", "PBX Queued", "Failed", "Cancelled"}


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _json(value: Any) -> str:
    return json.dumps(value or {}, default=str)


def _get_settings():
    return frappe.get_cached_doc("WhatsApp Calling Settings")


def _get_configured_calling_account(settings=None) -> str | None:
    settings = settings or _get_settings()
    template_name = getattr(settings, "call_permission_template", None)
    if not template_name:
        return None

    account_name = frappe.db.get_value(
        "WhatsApp Templates",
        template_name,
        "whatsapp_account",
    )
    return str(account_name) if account_name else None


def _get_account(whatsapp_account: str | None = None):
    if whatsapp_account:
        return frappe.get_doc("WhatsApp Account", whatsapp_account)

    configured_calling_account = _get_configured_calling_account()
    if configured_calling_account:
        return frappe.get_doc("WhatsApp Account", configured_calling_account)

    account = get_whatsapp_account(account_type="outgoing")
    if not account:
        frappe.throw(
            _("Please set a default outgoing WhatsApp Account."),
            title=_("WhatsApp Account Required"),
        )
    return account


def _ensure_enabled(settings=None):
    settings = settings or _get_settings()
    if not cint(settings.enabled):
        frappe.throw(
            _("WhatsApp calling is not enabled."),
            title=_("WhatsApp Calling Disabled"),
        )
    return settings


def _get_agent(user: str | None = None, *, throw: bool = True):
    user = user or frappe.session.user
    agent = frappe.db.get_all(
        "WhatsApp Call Agent",
        filters={"user": user, "enabled": 1},
        fields=["name", "user", "extension"],
        limit=1,
    )
    if agent:
        return agent[0]

    if throw:
        frappe.throw(
            _("No enabled PBX extension is mapped for user {0}.").format(user),
            title=_("PBX Extension Required"),
        )
    return None


def _timestamp_to_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        timestamp = int(value)
    except Exception:
        return None
    if timestamp > 100000000000:
        timestamp = int(timestamp / 1000)
    try:
        return datetime.fromtimestamp(timestamp)
    except Exception:
        return None


def _normalize_permission_status(
    raw_status: Any,
    *,
    is_permanent: bool = False,
    expires_at: datetime | None = None,
) -> str:
    raw = str(raw_status or "").strip().lower()
    if raw in {"permanent", "permanent_permission"} or is_permanent:
        return "Permanent"
    if raw in {"temporary", "temporary_permission", "accepted", "accept", "granted", "allow", "allowed"}:
        if expires_at and expires_at <= now_datetime():
            return "Expired"
        return "Temporary"
    if raw in {"rejected", "reject", "declined", "denied"}:
        return "Rejected"
    if raw in {"expired"}:
        return "Expired"
    if raw in {"no_permission", "not_granted", "none", "missing"}:
        return "No Permission"
    return "Unknown"


def parse_permission_state(payload: dict[str, Any] | None) -> dict[str, Any]:
    """Normalize Meta's call-permission response into local fields.

    Meta has shipped this API with slightly different response envelopes
    across beta/provider examples, so this parser accepts the direct object,
    a first item in ``data``, or nested ``call_permission`` /
    ``call_permission_reply`` objects.
    """
    payload = payload or {}
    state = payload

    data = _as_list(payload.get("data"))
    if data:
        state = _as_dict(data[0])
    elif "call_permission" in payload:
        state = _as_dict(payload.get("call_permission"))
    elif "call_permission_reply" in payload:
        state = _as_dict(payload.get("call_permission_reply"))

    raw_expires = (
        state.get("expiration_timestamp")
        or state.get("expires_at")
        or state.get("expiration_time")
    )
    expires_at = _timestamp_to_datetime(raw_expires)
    is_permanent = bool(
        cint(state.get("is_permanent"))
        or str(state.get("permission_type") or "").lower() == "permanent"
        or str(state.get("status") or "").lower() == "permanent"
    )
    raw_status = (
        state.get("permission_status")
        or state.get("status")
        or state.get("response")
        or state.get("permission")
    )
    status = _normalize_permission_status(
        raw_status,
        is_permanent=is_permanent,
        expires_at=expires_at,
    )
    if ("data" in payload and not data) or (not data and not state):
        status = "No Permission"

    return {
        "permission_status": status,
        "is_permanent": 1 if status == "Permanent" or is_permanent else 0,
        "expires_at": expires_at,
        "response_source": state.get("response_source"),
        "raw_meta_state": payload,
    }


def permission_is_active(permission: Any) -> bool:
    status = getattr(permission, "permission_status", None)
    if isinstance(permission, dict):
        status = permission.get("permission_status")
        expires_at = permission.get("expires_at")
    else:
        expires_at = getattr(permission, "expires_at", None)

    if status == "Permanent":
        return True
    if status != "Temporary":
        return False
    if expires_at and expires_at <= now_datetime():
        return False
    return True


def _upsert_permission(
    *,
    whatsapp_account: str,
    phone_number: str,
    state: dict[str, Any],
    last_requested_at=None,
    last_request_message: str | None = None,
):
    number = format_number(phone_number)
    existing = frappe.db.get_value(
        "WhatsApp Call Permission",
        {"whatsapp_account": whatsapp_account, "phone_number": number},
        "name",
    )
    if existing:
        doc = frappe.get_doc("WhatsApp Call Permission", existing)
    else:
        doc = frappe.new_doc("WhatsApp Call Permission")
        doc.whatsapp_account = whatsapp_account
        doc.phone_number = number

    doc.permission_status = state.get("permission_status") or "Unknown"
    doc.is_permanent = cint(state.get("is_permanent"))
    doc.expires_at = state.get("expires_at")
    doc.response_source = state.get("response_source")
    doc.last_checked_at = now_datetime()
    if last_requested_at:
        doc.last_requested_at = last_requested_at
    if last_request_message:
        doc.last_request_message = last_request_message
    doc.raw_meta_state = _json(state.get("raw_meta_state") or {})

    if existing:
        doc.save(ignore_permissions=True)
    else:
        doc.insert(ignore_permissions=True)
    return doc


def get_local_permission(phone_number: str, whatsapp_account: str):
    number = format_number(phone_number)
    existing = frappe.db.get_value(
        "WhatsApp Call Permission",
        {"whatsapp_account": whatsapp_account, "phone_number": number},
        "name",
    )
    if not existing:
        return None

    doc = frappe.get_doc("WhatsApp Call Permission", existing)
    if doc.permission_status == "Temporary" and doc.expires_at:
        if doc.expires_at <= now_datetime():
            doc.permission_status = "Expired"
            doc.save(ignore_permissions=True)
    return doc


def refresh_permission_state(phone_number: str, whatsapp_account: str | None = None):
    account = _get_account(whatsapp_account)
    number = format_number(phone_number)
    token = account.get_password("token")
    url = f"{account.url}/{account.version}/{account.phone_id}/call_permissions"

    try:
        response = requests.get(
            url,
            headers={"authorization": f"Bearer {token}"},
            params={"user_wa_id": number},
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json() if response.content else {}
    except Exception as exc:
        response_obj = getattr(exc, "response", None)
        response_text = getattr(response_obj, "text", None)
        frappe.throw(
            _("Could not check WhatsApp call permission. {0}").format(
                response_text or str(exc)
            ),
            title=_("WhatsApp Calling Unavailable"),
        )

    state = parse_permission_state(payload if isinstance(payload, dict) else {})
    return _upsert_permission(
        whatsapp_account=str(account.name),
        phone_number=number,
        state=state,
    )


def _find_pending_call(*, phone_number: str, whatsapp_account: str, contact: str | None = None):
    filters: dict[str, Any] = {
        "phone_number": format_number(phone_number),
        "whatsapp_account": whatsapp_account,
        "status": "Permission Requested",
    }
    if contact:
        filters["contact"] = contact

    rows = frappe.get_all(
        "WhatsApp Call",
        filters=filters,
        fields=["name"],
        order_by="creation desc",
        limit=1,
    )
    return frappe.get_doc("WhatsApp Call", rows[0].name) if rows else None


def get_call_state(
    *,
    phone_number: str,
    contact: str | None = None,
    agent_user: str | None = None,
    whatsapp_account: str | None = None,
) -> dict[str, Any]:
    settings = _get_settings()
    if not cint(settings.enabled):
        return {
            "enabled": 0,
            "status": "Disabled",
            "message": _("WhatsApp calling is not enabled."),
            "can_call": False,
            "permission_status": "Unknown",
            "pending_call": None,
            "agent_extension": None,
        }

    account = _get_account(whatsapp_account)
    agent = _get_agent(agent_user, throw=False)
    number = format_number(phone_number)
    permission = get_local_permission(number, str(account.name))
    pending = _find_pending_call(
        phone_number=number,
        whatsapp_account=str(account.name),
        contact=contact,
    )

    if not agent:
        status = "Missing Agent Extension"
        message = _("No enabled PBX extension is mapped for your user.")
    elif pending:
        status = "Permission Requested"
        message = _("Waiting for the contact to accept the call request.")
    elif permission and permission_is_active(permission):
        status = "Ready"
        message = _("Ready to call.")
    else:
        status = "No Permission"
        message = _("Call permission is required before dialing.")

    return {
        "enabled": cint(settings.enabled),
        "status": status,
        "message": message,
        "can_call": status in {"Ready", "No Permission"},
        "permission_status": (
            permission.permission_status if permission else "No Permission"
        ),
        "pending_call": pending.name if pending else None,
        "agent_extension": agent.extension if agent else None,
    }


def _create_call(
    *,
    phone_number: str,
    whatsapp_account: str,
    contact: str | None,
    agent_user: str,
    agent_extension: str,
    status: str,
):
    doc = frappe.get_doc({
        "doctype": "WhatsApp Call",
        "phone_number": format_number(phone_number),
        "whatsapp_account": whatsapp_account,
        "contact": contact,
        "agent_user": agent_user,
        "agent_extension": agent_extension,
        "status": status,
        "requested_at": now_datetime(),
    })
    doc.insert(ignore_permissions=True)
    return doc


def _validate_call_permission_template(settings, account_name: str):
    template_name = settings.call_permission_template
    if not template_name:
        frappe.throw(
            _("Configure a Call Permission Template in WhatsApp Calling Settings."),
            title=_("Call Permission Template Required"),
        )

    template = frappe.get_doc("WhatsApp Templates", template_name)
    if not cint(template.get("is_call_permission_request")):
        frappe.throw(
            _("The configured template is not marked as a call-permission request."),
            title=_("Invalid Call Permission Template"),
        )

    template_account = str(template.get("whatsapp_account") or "")
    if template_account and template_account != account_name:
        frappe.throw(
            _("The call-permission template belongs to WhatsApp Account {0}.").format(
                template_account
            ),
            title=_("Invalid Call Permission Template"),
        )

    return template


def _send_permission_template(*, call_doc, settings):
    template = _validate_call_permission_template(settings, call_doc.whatsapp_account)
    message_doc = frappe.get_doc({
        "doctype": "WhatsApp Message",
        "to": call_doc.phone_number,
        "type": "Outgoing",
        "message_type": "Template",
        "use_template": 1,
        "content_type": "text",
        "template": template.name,
        "whatsapp_account": call_doc.whatsapp_account,
    })
    message_doc.insert(ignore_permissions=False)

    call_doc.permission_request_message = message_doc.name
    call_doc.status = "Permission Requested"
    call_doc.requested_at = now_datetime()
    call_doc.save(ignore_permissions=True)

    _upsert_permission(
        whatsapp_account=call_doc.whatsapp_account,
        phone_number=call_doc.phone_number,
        state={
            "permission_status": "No Permission",
            "is_permanent": 0,
            "raw_meta_state": {"request_message": message_doc.message_id},
        },
        last_requested_at=call_doc.requested_at,
        last_request_message=message_doc.name,
    )
    publish_call_update(call_doc, _("Call permission requested."))
    return {
        "status": call_doc.status,
        "call": call_doc.name,
        "message": _("Call permission request sent."),
        "waiting_for_permission": True,
    }


def start_outbound_call(
    *,
    phone_number: str,
    contact: str | None = None,
    agent_user: str | None = None,
    whatsapp_account: str | None = None,
) -> dict[str, Any]:
    settings = _ensure_enabled()
    account = _get_account(whatsapp_account)
    agent_user = agent_user or frappe.session.user
    agent = _get_agent(agent_user)
    number = format_number(phone_number)

    pending = _find_pending_call(
        phone_number=number,
        whatsapp_account=str(account.name),
        contact=contact,
    )
    permission = refresh_permission_state(number, str(account.name))
    if permission_is_active(permission):
        call_doc = pending
        if call_doc:
            call_doc.status = "Permission Accepted"
            call_doc.permission_responded_at = now_datetime()
            call_doc.save(ignore_permissions=True)
        else:
            call_doc = _create_call(
                phone_number=number,
                whatsapp_account=str(account.name),
                contact=contact,
                agent_user=agent_user,
                agent_extension=str(agent.extension),
                status="Permission Accepted",
            )
        originate_call(call_doc, raise_on_failure=True)
        return {
            "status": call_doc.status,
            "call": call_doc.name,
            "message": _("Calling your PBX extension now."),
            "waiting_for_permission": False,
        }

    if pending:
        return {
            "status": pending.status,
            "call": pending.name,
            "message": _("Waiting for the contact to accept the call request."),
            "waiting_for_permission": True,
        }

    call_doc = _create_call(
        phone_number=number,
        whatsapp_account=str(account.name),
        contact=contact,
        agent_user=agent_user,
        agent_extension=str(agent.extension),
        status="Permission Requested",
    )
    return _send_permission_template(call_doc=call_doc, settings=settings)


def _safe_format(template: str, values: dict[str, str], *, label: str) -> str:
    try:
        return template.format(**values)
    except Exception as exc:
        frappe.throw(
            _("{0} has an invalid placeholder: {1}").format(label, exc),
            title=_("Invalid Calling Settings"),
        )


def _build_originate_payload(settings, call_doc, action_id: str) -> dict[str, str]:
    number = format_number(call_doc.phone_number)
    values = {
        "number": number,
        "e164": f"+{number}",
        "extension": str(call_doc.agent_extension or ""),
    }
    channel = _safe_format(
        settings.agent_channel_template or "Local/{extension}@from-internal",
        values,
        label=_("Agent Channel Template"),
    )
    exten = _safe_format(
        settings.destination_number_template or "{number}",
        values,
        label=_("Destination Number Template"),
    )
    timeout_ms = max(cint(settings.originate_timeout) or 30, 1) * 1000

    return {
        "Action": "Originate",
        "ActionID": action_id,
        "Channel": channel,
        "Context": settings.destination_context or "from-internal",
        "Exten": exten,
        "Priority": "1",
        "Timeout": str(timeout_ms),
        "CallerID": f"WhatsApp <{call_doc.agent_extension}>",
        "Async": "true",
        "Variable": f"WHATSAPP_CALL_ID={call_doc.name}",
    }


def _read_ami_response(sock) -> dict[str, str]:
    data = b""
    while b"\r\n\r\n" not in data:
        chunk = sock.recv(4096)
        if not chunk:
            break
        data += chunk
    text = data.decode("utf-8", errors="replace")
    parsed: dict[str, str] = {}
    for line in text.splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        parsed[key.strip()] = value.strip()
    return parsed


def _send_ami_action(sock, fields: dict[str, str]) -> dict[str, str]:
    payload = "".join(f"{key}: {value}\r\n" for key, value in fields.items())
    sock.sendall(f"{payload}\r\n".encode("utf-8"))
    return _read_ami_response(sock)


def _send_ami_originate(settings, call_doc, action_id: str) -> dict[str, str]:
    if not settings.ami_host:
        frappe.throw(_("AMI Host is required."), title=_("Invalid Calling Settings"))
    if not settings.ami_username:
        frappe.throw(_("AMI Username is required."), title=_("Invalid Calling Settings"))

    password = settings.get_password("ami_password")
    if not password:
        frappe.throw(_("AMI Password is required."), title=_("Invalid Calling Settings"))

    timeout = max(cint(settings.originate_timeout) or 30, 1)
    raw_sock = socket.create_connection(
        (settings.ami_host, cint(settings.ami_port) or 5038),
        timeout=timeout,
    )
    sock = (
        ssl.create_default_context().wrap_socket(raw_sock, server_hostname=settings.ami_host)
        if cint(settings.ami_use_tls)
        else raw_sock
    )
    sock.settimeout(timeout)

    try:
        try:
            _read_ami_response(sock)
        except socket.timeout:
            pass

        login_response = _send_ami_action(sock, {
            "Action": "Login",
            "Username": settings.ami_username,
            "Secret": password,
        })
        if login_response.get("Response") == "Error":
            frappe.throw(
                login_response.get("Message") or _("AMI login failed."),
                title=_("AMI Login Failed"),
            )

        response = _send_ami_action(
            sock,
            _build_originate_payload(settings, call_doc, action_id),
        )
        if response.get("Response") == "Error":
            frappe.throw(
                response.get("Message") or _("AMI originate failed."),
                title=_("AMI Originate Failed"),
            )
        return response
    finally:
        try:
            _send_ami_action(sock, {"Action": "Logoff"})
        except Exception:
            pass
        try:
            sock.close()
        except Exception:
            pass


def originate_call(call_doc, *, raise_on_failure: bool = False):
    settings = _ensure_enabled()
    action_id = f"whatsapp-call-{frappe.generate_hash(length=10)}"
    try:
        response = _send_ami_originate(settings, call_doc, action_id)
        call_doc.status = "PBX Queued"
        call_doc.ami_action_id = action_id
        call_doc.pbx_queued_at = now_datetime()
        call_doc.failure_reason = ""
        call_doc.last_error_payload = _json(response)
        call_doc.save(ignore_permissions=True)
        publish_call_update(call_doc, _("PBX call queued."))
        return call_doc
    except Exception as exc:
        call_doc.status = "Failed"
        call_doc.failure_reason = str(exc)
        call_doc.last_error_payload = _json({"error": str(exc)})
        call_doc.save(ignore_permissions=True)
        publish_call_update(call_doc, _("Could not queue the PBX call."))
        if raise_on_failure:
            raise
        frappe.log_error(frappe.get_traceback(), "WhatsApp AMI originate failed")
        return call_doc


def originate_pending_call(call_name: str):
    call_doc = frappe.get_doc("WhatsApp Call", call_name)
    if call_doc.status in TERMINAL_CALL_STATUSES:
        return call_doc
    return originate_call(call_doc, raise_on_failure=False)


def _find_call_from_permission_reply(
    *,
    phone_number: str,
    whatsapp_account: str,
    context_message_id: str | None = None,
):
    request_docname = None
    if context_message_id:
        request_docname = frappe.db.get_value(
            "WhatsApp Message",
            {"message_id": context_message_id},
            "name",
        )

    filters: dict[str, Any] = {
        "phone_number": format_number(phone_number),
        "whatsapp_account": whatsapp_account,
        "status": "Permission Requested",
    }
    if request_docname:
        filters["permission_request_message"] = request_docname

    rows = frappe.get_all(
        "WhatsApp Call",
        filters=filters,
        fields=["name"],
        order_by="creation desc",
        limit=1,
    )
    if rows:
        return frappe.get_doc("WhatsApp Call", rows[0].name)

    if request_docname:
        rows = frappe.get_all(
            "WhatsApp Call",
            filters={
                "phone_number": format_number(phone_number),
                "whatsapp_account": whatsapp_account,
                "status": "Permission Requested",
            },
            fields=["name"],
            order_by="creation desc",
            limit=1,
        )
        if rows:
            return frappe.get_doc("WhatsApp Call", rows[0].name)

    return None


def handle_call_permission_reply(
    *,
    contact_number: str,
    whatsapp_account_name: str,
    response: str,
    is_permanent: bool = False,
    expiration_timestamp: Any = None,
    response_source: str | None = None,
    context_message_id: str | None = None,
    message_doc_name: str | None = None,
) -> None:
    expires_at = _timestamp_to_datetime(expiration_timestamp)
    status = _normalize_permission_status(
        response,
        is_permanent=is_permanent,
        expires_at=expires_at,
    )
    state = {
        "permission_status": status,
        "is_permanent": 1 if is_permanent or status == "Permanent" else 0,
        "expires_at": expires_at,
        "response_source": response_source,
        "raw_meta_state": {
            "response": response,
            "is_permanent": is_permanent,
            "expiration_timestamp": expiration_timestamp,
            "response_source": response_source,
            "message_doc_name": message_doc_name,
        },
    }
    _upsert_permission(
        whatsapp_account=whatsapp_account_name,
        phone_number=contact_number,
        state=state,
    )

    call_doc = _find_call_from_permission_reply(
        phone_number=contact_number,
        whatsapp_account=whatsapp_account_name,
        context_message_id=context_message_id,
    )
    if not call_doc:
        return

    call_doc.permission_responded_at = now_datetime()
    if status in ACTIVE_PERMISSION_STATUSES:
        call_doc.status = "Permission Accepted"
        call_doc.save(ignore_permissions=True)
        publish_call_update(call_doc, _("Call permission accepted."))
        frappe.enqueue(
            "frappe_whatsapp.utils.calling.originate_pending_call",
            queue="short",
            enqueue_after_commit=True,
            call_name=call_doc.name,
        )
    elif status == "Rejected":
        call_doc.status = "Permission Rejected"
        call_doc.save(ignore_permissions=True)
        publish_call_update(call_doc, _("Call permission rejected."))
    else:
        call_doc.status = "Failed"
        call_doc.failure_reason = _("Call permission response was not accepted.")
        call_doc.save(ignore_permissions=True)
        publish_call_update(call_doc, _("Call permission was not accepted."))


def publish_call_update(call_doc, message: str):
    payload = {
        "event_type": "whatsapp_call_update",
        "room": call_doc.contact,
        "call": call_doc.name,
        "status": call_doc.status,
        "content": message,
        "creation": now_datetime(),
        "user": "System",
    }
    if call_doc.agent_user:
        frappe.publish_realtime(
            CALL_UPDATE_EVENT,
            payload,
            user=call_doc.agent_user,
        )
