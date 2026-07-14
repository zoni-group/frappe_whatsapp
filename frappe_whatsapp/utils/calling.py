from __future__ import annotations

import hashlib
import json
import re
import socket
import ssl
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Literal, Protocol, cast

import frappe
import requests
from frappe import _
from frappe.utils import cint, get_datetime, now_datetime
from frappe.utils.file_lock import LockTimeoutError
from frappe.utils.synchronization import filelock

from frappe_whatsapp.utils import get_whatsapp_account

if TYPE_CHECKING:
    from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_account.whatsapp_account import WhatsAppAccount
    from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_call.whatsapp_call import WhatsAppCall
    from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_call_permission.whatsapp_call_permission import WhatsAppCallPermission
    from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_calling_settings.whatsapp_calling_settings import WhatsAppCallingSettings
    from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_message.whatsapp_message import WhatsAppMessage
    from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_templates.whatsapp_templates import WhatsAppTemplates


CALL_UPDATE_EVENT = "whatsapp_call_update"
ACTIVE_PERMISSION_STATUSES = {"Temporary", "Permanent"}
TERMINAL_CALL_STATUSES = {
    "Permission Accepted",
    "Permission Rejected",
    "PBX Queued",
    "Failed",
    "Cancelled",
}
CALL_PERMISSION_STATE_TTL_SECONDS = 60
CALL_ACTION_PERMISSION_REQUEST = "Permission Request"
CALL_ACTION_OUTBOUND = "Outbound Call"
_AGENT_EXTENSION_PATTERN = re.compile(r"^[0-9]{1,10}$", re.ASCII)
_IDEMPOTENCY_KEY_PATTERN = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._:-]{7,139}$",
    re.ASCII,
)

PermissionStatus = Literal[
    "No Permission", "Temporary", "Permanent", "Rejected", "Expired", "Unknown"
]


class CallAgentRow(Protocol):
    extension: str


def _as_int(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, (int, float, str)):
        return cint(value)
    return 0


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _json(value: Any) -> str:
    return json.dumps(value or {}, default=str)


def _normalize_phone_number(phone_number: Any) -> str:
    return "".join(
        character
        for character in str(phone_number or "")
        if character in "0123456789"
    )


def validate_call_phone_number(phone_number: Any) -> str:
    number = _normalize_phone_number(phone_number)
    if not 8 <= len(number) <= 15:
        frappe.throw(
            _("Phone Number must contain between 8 and 15 digits."),
            title=_("Invalid Phone Number"),
        )
    return number


def validate_agent_extension(agent_extension: Any) -> str:
    raw_extension = str(agent_extension or "")
    extension = raw_extension.strip()
    if (
        extension != raw_extension
        or not _AGENT_EXTENSION_PATTERN.fullmatch(extension)
    ):
        frappe.throw(
            _("PBX extension must contain only 1 to 10 ASCII digits."),
            title=_("Invalid PBX Extension"),
        )
    return extension


def validate_idempotency_key(idempotency_key: Any) -> str:
    raw_key = str(idempotency_key or "")
    key = raw_key.strip()
    if key != raw_key or not _IDEMPOTENCY_KEY_PATTERN.fullmatch(key):
        frappe.throw(
            _(
                "Idempotency Key must be 8 to 140 characters and contain "
                "only letters, digits, periods, underscores, colons, or hyphens."
            ),
            title=_("Invalid Idempotency Key"),
        )
    return key


def _get_settings() -> WhatsAppCallingSettings:
    return cast(
        "WhatsAppCallingSettings",
        frappe.get_cached_doc("WhatsApp Calling Settings"),
    )


def _get_configured_calling_account(
    settings: WhatsAppCallingSettings | None = None,
) -> str | None:
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


def _get_account(whatsapp_account: str | None = None) -> WhatsAppAccount:
    if whatsapp_account:
        return cast(
            "WhatsAppAccount",
            frappe.get_doc("WhatsApp Account", whatsapp_account),
        )

    configured_calling_account = _get_configured_calling_account()
    if configured_calling_account:
        return cast(
            "WhatsAppAccount",
            frappe.get_doc("WhatsApp Account", configured_calling_account),
        )

    account = get_whatsapp_account(account_type="outgoing")
    if not account:
        frappe.throw(
            _("Please set a default outgoing WhatsApp Account."),
            title=_("WhatsApp Account Required"),
        )
        raise RuntimeError("WhatsApp Account is required")
    return cast("WhatsAppAccount", account)


def _ensure_enabled(
    settings: WhatsAppCallingSettings | None = None,
) -> WhatsAppCallingSettings:
    settings = settings or _get_settings()
    if not settings.enabled:
        frappe.throw(
            _("WhatsApp calling is not enabled."),
            title=_("WhatsApp Calling Disabled"),
        )
    return settings


def _get_agent(
    user: str | None = None, *, throw: bool = True
) -> CallAgentRow | None:
    user = user or frappe.session.user
    agent = frappe.db.get_all(
        "WhatsApp Call Agent",
        filters={"user": user, "enabled": 1},
        fields=["name", "user", "extension"],
        limit=1,
    )
    if agent:
        return cast(CallAgentRow, agent[0])

    if throw:
        frappe.throw(
            _("No enabled PBX extension is mapped for user {0}.").format(user),
            title=_("PBX Extension Required"),
        )
    return None


def _resolve_agent_identity(
    *,
    agent_user: str | None = None,
    agent_extension: str | None = None,
    throw: bool = True,
) -> tuple[str | None, str | None]:
    """Resolve exactly one agent mode: mapped Desk user or direct extension."""
    if agent_extension not in (None, ""):
        if agent_user:
            frappe.throw(
                _("Provide either Agent User or Agent Extension, not both."),
                title=_("Invalid Calling Agent"),
            )
        return None, validate_agent_extension(agent_extension)

    resolved_user = agent_user or frappe.session.user
    agent = _get_agent(resolved_user, throw=throw)
    if not agent:
        return resolved_user, None
    return resolved_user, validate_agent_extension(agent.extension)


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
) -> PermissionStatus:
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

    data_value = payload.get("data")
    data = _as_list(data_value)
    if data:
        state = _as_dict(data[0])
    elif isinstance(data_value, dict):
        state = data_value

    for nested_key in (
        "call_permission",
        "call_permission_reply",
        "permission",
    ):
        nested_state = _as_dict(state.get(nested_key))
        if nested_state:
            state = nested_state
            break

    raw_expires = (
        state.get("expiration_timestamp")
        or state.get("expires_at")
        or state.get("expiration_time")
    )
    expires_at = _timestamp_to_datetime(raw_expires)
    is_permanent = bool(
        _as_int(state.get("is_permanent"))
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
    if (
        isinstance(data_value, (list, dict))
        and not data_value
    ) or not state:
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


def _permission_action_allowed(
    permission: Any,
    action_name: str,
) -> bool | None:
    """Return Meta's action decision, or None when it is not in the payload."""
    if isinstance(permission, dict):
        raw_state = permission.get("raw_meta_state")
    else:
        raw_state = getattr(permission, "raw_meta_state", None)

    if isinstance(raw_state, str):
        try:
            raw_state = json.loads(raw_state)
        except (TypeError, ValueError):
            raw_state = {}

    for action in _as_list(_as_dict(raw_state).get("actions")):
        action = _as_dict(action)
        if action.get("action_name") == action_name:
            return bool(action.get("can_perform_action"))
    return None


def _permission_state_is_fresh(permission: Any) -> bool:
    last_checked_at = getattr(permission, "last_checked_at", None)
    if not last_checked_at:
        return False
    checked_at = get_datetime(last_checked_at)
    if checked_at is None:
        return False
    return checked_at >= (
        now_datetime() - timedelta(seconds=CALL_PERMISSION_STATE_TTL_SECONDS)
    )


def _permission_request_lock_name(
    whatsapp_account: str,
    phone_number: str,
) -> str:
    key = f"{whatsapp_account}:{_normalize_phone_number(phone_number)}"
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()[:24]
    return f"whatsapp-call-permission-{digest}"


def _call_start_lock_name(
    whatsapp_account: str,
    phone_number: str,
) -> str:
    key = f"{whatsapp_account}:{_normalize_phone_number(phone_number)}"
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()[:24]
    return f"whatsapp-call-start-{digest}"


def _get_idempotent_call(
    *,
    idempotency_key: str | None,
    action_type: str,
    phone_number: str,
    whatsapp_account: str,
    agent_extension: str,
) -> WhatsAppCall | None:
    if not idempotency_key:
        return None

    existing = frappe.db.get_value(
        "WhatsApp Call",
        {"idempotency_key": idempotency_key},
        "name",
    )
    if not existing:
        return None

    call_doc = cast(
        "WhatsAppCall",
        frappe.get_doc("WhatsApp Call", str(existing)),
    )
    expected = {
        "action_type": action_type,
        "phone_number": _normalize_phone_number(phone_number),
        "whatsapp_account": whatsapp_account,
        "agent_extension": agent_extension,
    }
    actual = {
        "action_type": str(call_doc.action_type or ""),
        "phone_number": _normalize_phone_number(call_doc.phone_number),
        "whatsapp_account": str(call_doc.whatsapp_account or ""),
        "agent_extension": str(call_doc.agent_extension or ""),
    }
    if actual != expected:
        frappe.local.response["http_status_code"] = 409
        frappe.throw(
            _("Idempotency Key has already been used for another call action."),
            title=_("Idempotency Conflict"),
        )
    return call_doc


def _upsert_permission(
    *,
    whatsapp_account: str,
    phone_number: str,
    state: dict[str, Any],
    last_requested_at: datetime | None = None,
    last_request_message: str | None = None,
) -> WhatsAppCallPermission:
    number = _normalize_phone_number(phone_number)
    existing = frappe.db.get_value(
        "WhatsApp Call Permission",
        {"whatsapp_account": whatsapp_account, "phone_number": number},
        "name",
    )
    if existing:
        doc = cast(
            "WhatsAppCallPermission",
            frappe.get_doc("WhatsApp Call Permission", str(existing)),
        )
    else:
        doc = cast(
            "WhatsAppCallPermission",
            frappe.new_doc("WhatsApp Call Permission"),
        )
        doc.whatsapp_account = whatsapp_account
        doc.phone_number = number

    raw_status = str(state.get("permission_status") or "Unknown")
    doc.permission_status = cast(
        PermissionStatus,
        raw_status if raw_status in ACTIVE_PERMISSION_STATUSES | {
            "No Permission", "Rejected", "Expired", "Unknown"
        } else "Unknown",
    )
    doc.is_permanent = _as_int(state.get("is_permanent"))
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


def get_local_permission(
    phone_number: str, whatsapp_account: str
) -> WhatsAppCallPermission | None:
    number = _normalize_phone_number(phone_number)
    existing = frappe.db.get_value(
        "WhatsApp Call Permission",
        {"whatsapp_account": whatsapp_account, "phone_number": number},
        "name",
    )
    if not existing:
        return None

    doc = cast(
        "WhatsAppCallPermission",
        frappe.get_doc("WhatsApp Call Permission", str(existing)),
    )
    if doc.permission_status == "Temporary" and doc.expires_at:
        expires_at = get_datetime(doc.expires_at)
        if expires_at and expires_at <= now_datetime():
            doc.permission_status = "Expired"
            doc.save(ignore_permissions=True)
    return doc


def refresh_permission_state(
    phone_number: str, whatsapp_account: str | None = None
) -> WhatsAppCallPermission:
    account = _get_account(whatsapp_account)
    number = _normalize_phone_number(phone_number)
    token = account.get_password("token")
    url = f"{account.url}/{account.version}/{account.phone_id}/call_permissions"

    payload: dict[str, Any] = {}
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
        raise RuntimeError("Could not check WhatsApp call permission")

    state = parse_permission_state(payload if isinstance(payload, dict) else {})
    return _upsert_permission(
        whatsapp_account=str(account.name),
        phone_number=number,
        state=state,
    )


def _find_pending_call(
    *, phone_number: str, whatsapp_account: str, contact: str | None = None
) -> WhatsAppCall | None:
    filters: dict[str, Any] = {
        "phone_number": _normalize_phone_number(phone_number),
        "whatsapp_account": whatsapp_account,
        "status": "Permission Requested",
    }
    rows = frappe.get_all(
        "WhatsApp Call",
        filters=filters,
        fields=["name"],
        order_by="creation desc",
        limit=1,
    )
    return (
        cast(
            "WhatsAppCall",
            frappe.get_doc("WhatsApp Call", str(rows[0].name)),
        )
        if rows else None
    )


def get_call_state(
    *,
    phone_number: str,
    contact: str | None = None,
    agent_user: str | None = None,
    agent_extension: str | None = None,
    whatsapp_account: str | None = None,
) -> dict[str, Any]:
    settings = _get_settings()
    if not settings.enabled:
        return {
            "enabled": 0,
            "status": "Disabled",
            "message": _("WhatsApp calling is not enabled."),
            "can_call": False,
            "can_request_permission": False,
            "permission_status": "Unknown",
            "pending_call": None,
            "agent_extension": None,
            "call_permission_template": None,
            "whatsapp_account": None,
        }

    account = _get_account(whatsapp_account)
    resolved_agent_user, resolved_extension = _resolve_agent_identity(
        agent_user=agent_user,
        agent_extension=agent_extension,
        throw=False,
    )
    number = _normalize_phone_number(phone_number)
    account_name = str(account.name)
    permission = get_local_permission(number, account_name)
    pending = _find_pending_call(
        phone_number=number,
        whatsapp_account=account_name,
        contact=contact,
    )

    if not resolved_extension:
        status = "Missing Agent Extension"
        message = _("No enabled PBX extension is mapped for your user.")
    else:
        action_allowed = _permission_action_allowed(
            permission, "send_call_permission_request")
        start_allowed = _permission_action_allowed(permission, "start_call")
        required_action_known = (
            start_allowed
            if permission and permission_is_active(permission)
            else action_allowed
        )
        if (
            not permission
            or not _permission_state_is_fresh(permission)
            or (not pending and required_action_known is None)
        ):
            try:
                permission = refresh_permission_state(number, account_name)
            except frappe.ValidationError:
                return {
                    "enabled": int(bool(settings.enabled)),
                    "status": "Unavailable",
                    "message": _(
                        "Could not verify call permission with Meta. Try again shortly."
                    ),
                    "can_call": False,
                    "can_request_permission": False,
                    "permission_status": (
                        permission.permission_status
                        if permission else "Unknown"
                    ),
                    "pending_call": pending.name if pending else None,
                    "agent_extension": resolved_extension,
                    "call_permission_template": settings.call_permission_template,
                    "whatsapp_account": account_name,
                }

        action_allowed = _permission_action_allowed(
            permission, "send_call_permission_request")
        start_allowed = _permission_action_allowed(permission, "start_call")
        if pending and permission and permission_is_active(permission):
            pending.status = "Permission Accepted"
            pending.permission_responded_at = now_datetime()
            pending.save(ignore_permissions=True)
            pending = None

        if (
            permission
            and permission_is_active(permission)
            and start_allowed is True
        ):
            status = "Ready"
            message = _("Permission is active. Click Call when you are ready.")
        elif permission and permission_is_active(permission):
            status = "Unavailable"
            message = _("Meta does not currently allow starting this call.")
        elif pending:
            status = "Permission Requested"
            message = _("Waiting for the contact to accept the call request.")
        else:
            status = "No Permission"
            message = (
                _("Call permission is required before dialing.")
                if action_allowed
                else _("Meta does not currently allow a call-permission request.")
            )

    can_request_permission = bool(
        resolved_extension
        and status == "No Permission"
        and _permission_action_allowed(
            permission, "send_call_permission_request") is True
    )

    return {
        "enabled": int(bool(settings.enabled)),
        "status": status,
        "message": message,
        "can_call": status == "Ready",
        "can_request_permission": can_request_permission,
        "permission_status": (
            permission.permission_status if permission else "No Permission"
        ),
        "pending_call": pending.name if pending else None,
        "agent_extension": resolved_extension,
        "agent_user": resolved_agent_user,
        "call_permission_template": settings.call_permission_template,
        "whatsapp_account": account_name,
        "permission_expires_at": (
            permission.expires_at if permission else None
        ),
        "permission_last_checked_at": (
            permission.last_checked_at if permission else None
        ),
    }


def _create_call(
    *,
    phone_number: str,
    whatsapp_account: str,
    contact: str | None,
    agent_user: str | None,
    agent_extension: str,
    status: str,
    action_type: str | None = None,
    source_app: str | None = None,
    external_reference: str | None = None,
    idempotency_key: str | None = None,
) -> WhatsAppCall:
    doc = cast("WhatsAppCall", frappe.get_doc({
        "doctype": "WhatsApp Call",
        "phone_number": _normalize_phone_number(phone_number),
        "whatsapp_account": whatsapp_account,
        "contact": contact,
        "agent_user": agent_user,
        "agent_extension": agent_extension,
        "status": status,
        "action_type": action_type,
        "source_app": source_app,
        "external_reference": external_reference,
        "idempotency_key": idempotency_key,
        "requested_at": now_datetime(),
    }))
    doc.insert(ignore_permissions=True)
    return doc


def _validate_call_permission_template(
    settings: WhatsAppCallingSettings, account_name: str
) -> WhatsAppTemplates:
    template_name = settings.call_permission_template
    if not template_name:
        frappe.throw(
            _("Configure a Call Permission Template in WhatsApp Calling Settings."),
            title=_("Call Permission Template Required"),
        )

    template = cast(
        "WhatsAppTemplates",
        frappe.get_doc("WhatsApp Templates", str(template_name)),
    )
    if not template.is_call_permission_request:
        frappe.throw(
            _("The configured template is not marked as a call-permission request."),
            title=_("Invalid Call Permission Template"),
        )

    template_account = str(template.whatsapp_account or "")
    if template_account and template_account != account_name:
        frappe.throw(
            _("The call-permission template belongs to WhatsApp Account {0}.").format(
                template_account
            ),
            title=_("Invalid Call Permission Template"),
        )

    return template


def _send_permission_template(
    *, call_doc: WhatsAppCall, settings: WhatsAppCallingSettings
) -> dict[str, Any]:
    template = _validate_call_permission_template(settings, call_doc.whatsapp_account)
    message_doc = cast("WhatsAppMessage", frappe.get_doc({
        "doctype": "WhatsApp Message",
        "to": call_doc.phone_number,
        "type": "Outgoing",
        "message_type": "Template",
        "use_template": 1,
        "content_type": "text",
        "template": template.name,
        "whatsapp_account": call_doc.whatsapp_account,
        "source_app": call_doc.source_app,
        "external_reference": call_doc.external_reference,
    }))
    message_doc.insert(ignore_permissions=True)

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
        "ok": True,
        "status": call_doc.status,
        "call": call_doc.name,
        "message": _("Call permission request sent."),
        "waiting_for_permission": True,
        "retryable": False,
        "idempotency_key": call_doc.idempotency_key,
    }


def _permission_request_replay_result(
    call_doc: WhatsAppCall,
) -> dict[str, Any]:
    waiting = call_doc.status == "Permission Requested"
    return {
        "ok": call_doc.status not in {"Failed", "Permission Rejected"},
        "status": call_doc.status,
        "call": call_doc.name,
        "message": _("Returning the existing call-permission request."),
        "waiting_for_permission": waiting,
        "retryable": False,
        "idempotency_key": call_doc.idempotency_key,
        "idempotent_replay": True,
    }


def request_call_permission(
    *,
    phone_number: str,
    contact: str | None = None,
    agent_user: str | None = None,
    agent_extension: str | None = None,
    whatsapp_account: str | None = None,
    source_app: str | None = None,
    external_reference: str | None = None,
    idempotency_key: str | None = None,
) -> dict[str, Any]:
    """Send one explicit call-permission request without starting a call."""
    resolved_agent_user, resolved_extension = _resolve_agent_identity(
        agent_user=agent_user,
        agent_extension=agent_extension,
    )
    assert resolved_extension is not None
    number = validate_call_phone_number(phone_number)
    if idempotency_key:
        idempotency_key = validate_idempotency_key(idempotency_key)
    settings = _ensure_enabled()
    account = _get_account(whatsapp_account)
    account_name = str(account.name)
    _validate_call_permission_template(settings, account_name)

    lock_context = filelock(
        _permission_request_lock_name(account_name, number),
        timeout=5,
    )
    release_after_transaction = False
    try:
        lock_context.__enter__()
        try:
            replay = _get_idempotent_call(
                idempotency_key=idempotency_key,
                action_type=CALL_ACTION_PERMISSION_REQUEST,
                phone_number=number,
                whatsapp_account=account_name,
                agent_extension=resolved_extension,
            )
            if replay:
                return _permission_request_replay_result(replay)

            try:
                permission = refresh_permission_state(number, account_name)
            except frappe.ValidationError:
                return {
                    "ok": False,
                    "status": "Unavailable",
                    "call": None,
                    "message": _(
                        "Could not verify call permission with Meta. "
                        "Try again shortly."
                    ),
                    "waiting_for_permission": False,
                    "retryable": True,
                    "can_request_permission": False,
                    "can_call": False,
                    "idempotency_key": idempotency_key,
                }
            if permission_is_active(permission):
                can_start = _permission_action_allowed(permission, "start_call")
                message = (
                    _(
                        "Call permission is already active. "
                        "Click Call when you are ready."
                    )
                    if can_start is True
                    else _(
                        "Call permission is active, but Meta does not currently "
                        "allow starting this call."
                    )
                )
                return {
                    "ok": can_start is True,
                    "status": "Ready" if can_start is True else "Unavailable",
                    "call": None,
                    "message": message,
                    "waiting_for_permission": False,
                    "retryable": False,
                    "idempotency_key": idempotency_key,
                }

            pending = _find_pending_call(
                phone_number=number,
                whatsapp_account=account_name,
            )
            if pending:
                return {
                    "ok": True,
                    "status": pending.status,
                    "call": pending.name,
                    "message": _(
                        "A call-permission request is already waiting for this contact."
                    ),
                    "waiting_for_permission": True,
                    "retryable": False,
                    "idempotency_key": pending.idempotency_key,
                }

            if (
                _permission_action_allowed(
                    permission, "send_call_permission_request"
                )
                is not True
            ):
                return {
                    "ok": False,
                    "status": "Unavailable",
                    "call": None,
                    "message": _(
                        "Meta does not currently allow a call-permission request "
                        "for this contact."
                    ),
                    "waiting_for_permission": False,
                    "retryable": False,
                    "idempotency_key": idempotency_key,
                }

            call_doc = _create_call(
                phone_number=number,
                whatsapp_account=account_name,
                contact=contact,
                agent_user=resolved_agent_user,
                agent_extension=resolved_extension,
                status="Permission Requested",
                action_type=CALL_ACTION_PERMISSION_REQUEST,
                source_app=source_app,
                external_reference=external_reference,
                idempotency_key=idempotency_key,
            )
            result = _send_permission_template(
                call_doc=call_doc,
                settings=settings,
            )

            def release_lock():
                lock_context.__exit__(None, None, None)

            frappe.db.after_commit.add(release_lock)
            frappe.db.after_rollback.add(release_lock)
            release_after_transaction = True
            return result
        finally:
            if not release_after_transaction:
                lock_context.__exit__(None, None, None)
    except LockTimeoutError:
        return {
            "ok": False,
            "status": "Unavailable",
            "call": None,
            "message": _(
                "Another call-permission request is already being processed."
            ),
            "waiting_for_permission": False,
            "retryable": True,
            "idempotency_key": idempotency_key,
        }


def _outbound_call_result(
    call_doc: WhatsAppCall, *, replay: bool = False
) -> dict[str, Any]:
    queued = call_doc.status == "PBX Queued"
    result = {
        "ok": queued,
        "status": call_doc.status,
        "call": call_doc.name,
        "message": (
            _("Calling your PBX extension now.")
            if queued
            else _("Could not queue the PBX call.")
        ),
        "waiting_for_permission": False,
        "retryable": call_doc.status == "Failed",
        "can_request_permission": False,
        "can_call": call_doc.status == "Failed",
        "idempotency_key": getattr(call_doc, "idempotency_key", None),
        "failure_reason": getattr(call_doc, "failure_reason", None) or None,
    }
    if replay:
        result["idempotent_replay"] = True
    return result


def start_outbound_call(
    *,
    phone_number: str,
    contact: str | None = None,
    agent_user: str | None = None,
    agent_extension: str | None = None,
    whatsapp_account: str | None = None,
    source_app: str | None = None,
    external_reference: str | None = None,
    idempotency_key: str | None = None,
) -> dict[str, Any]:
    resolved_agent_user, resolved_extension = _resolve_agent_identity(
        agent_user=agent_user,
        agent_extension=agent_extension,
    )
    assert resolved_extension is not None
    number = validate_call_phone_number(phone_number)
    if idempotency_key:
        idempotency_key = validate_idempotency_key(idempotency_key)
    _ensure_enabled()
    account = _get_account(whatsapp_account)

    account_name = str(account.name)
    lock_context = filelock(
        _call_start_lock_name(account_name, number),
        timeout=5,
    )
    release_after_transaction = False
    try:
        lock_context.__enter__()
        try:
            replay = _get_idempotent_call(
                idempotency_key=idempotency_key,
                action_type=CALL_ACTION_OUTBOUND,
                phone_number=number,
                whatsapp_account=account_name,
                agent_extension=resolved_extension,
            )
            if replay:
                return _outbound_call_result(replay, replay=True)

            try:
                permission = refresh_permission_state(number, account_name)
            except frappe.ValidationError:
                return {
                    "ok": False,
                    "status": "Unavailable",
                    "call": None,
                    "message": _(
                        "Could not verify call permission with Meta. "
                        "Try again shortly."
                    ),
                    "waiting_for_permission": False,
                    "retryable": True,
                    "can_request_permission": False,
                    "can_call": False,
                    "idempotency_key": idempotency_key,
                }
            if not permission_is_active(permission):
                return {
                    "ok": False,
                    "status": "No Permission",
                    "call": None,
                    "message": _(
                        "Active WhatsApp call permission is required. Send a "
                        "call-permission request and wait for the contact to accept it."
                    ),
                    "waiting_for_permission": False,
                    "retryable": False,
                    "can_request_permission": _permission_action_allowed(
                        permission, "send_call_permission_request"
                    ) is True,
                    "can_call": False,
                    "idempotency_key": idempotency_key,
                }
            if _permission_action_allowed(permission, "start_call") is not True:
                return {
                    "ok": False,
                    "status": "Unavailable",
                    "call": None,
                    "message": _(
                        "Meta does not currently allow starting this WhatsApp call."
                    ),
                    "waiting_for_permission": False,
                    "retryable": False,
                    "can_request_permission": False,
                    "can_call": False,
                    "idempotency_key": idempotency_key,
                }

            call_doc = _create_call(
                phone_number=number,
                whatsapp_account=account_name,
                contact=contact,
                agent_user=resolved_agent_user,
                agent_extension=resolved_extension,
                status="Permission Accepted",
                action_type=CALL_ACTION_OUTBOUND,
                source_app=source_app,
                external_reference=external_reference,
                idempotency_key=idempotency_key,
            )
            originate_call(call_doc, raise_on_failure=False)

            def release_lock():
                lock_context.__exit__(None, None, None)

            frappe.db.after_commit.add(release_lock)
            frappe.db.after_rollback.add(release_lock)
            release_after_transaction = True
            return _outbound_call_result(call_doc)
        finally:
            if not release_after_transaction:
                lock_context.__exit__(None, None, None)
    except LockTimeoutError:
        return {
            "ok": False,
            "status": "Unavailable",
            "call": None,
            "message": _("Another outbound call is already being processed."),
            "waiting_for_permission": False,
            "retryable": True,
            "idempotency_key": idempotency_key,
        }


def _safe_format(template: str, values: dict[str, str], *, label: str) -> str:
    try:
        return template.format(**values)
    except Exception as exc:
        frappe.throw(
            _("{0} has an invalid placeholder: {1}").format(label, exc),
            title=_("Invalid Calling Settings"),
        )
        raise RuntimeError(f"Invalid {label}")


def _build_originate_payload(
    settings: WhatsAppCallingSettings,
    call_doc: WhatsAppCall,
    action_id: str,
) -> dict[str, str]:
    number = _normalize_phone_number(call_doc.phone_number)
    extension = validate_agent_extension(call_doc.agent_extension)
    values = {
        "number": number,
        "e164": f"+{number}",
        "extension": extension,
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
    timeout_ms = max(int(settings.originate_timeout or 30), 1) * 1000

    return {
        "Action": "Originate",
        "ActionID": action_id,
        "Channel": channel,
        "Context": settings.destination_context or "from-internal",
        "Exten": exten,
        "Priority": "1",
        "Timeout": str(timeout_ms),
        "CallerID": f"WhatsApp <{extension}>",
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


def _read_ami_banner(sock) -> str:
    data = b""
    while b"\n" not in data:
        chunk = sock.recv(4096)
        if not chunk:
            break
        data += chunk
        if len(data) > 4096:
            frappe.throw(
                _("AMI returned an invalid server banner."),
                title=_("AMI Connection Failed"),
            )

    banner = data.decode("utf-8", errors="replace").strip()
    if not banner.startswith("Asterisk Call Manager/"):
        frappe.throw(
            _("AMI returned an unexpected server banner."),
            title=_("AMI Connection Failed"),
        )
    return banner


def _send_ami_action(sock, fields: dict[str, str]) -> dict[str, str]:
    payload = "".join(f"{key}: {value}\r\n" for key, value in fields.items())
    sock.sendall(f"{payload}\r\n".encode("utf-8"))
    return _read_ami_response(sock)


def _require_ami_success(
    response: dict[str, str],
    *,
    title: str,
    fallback_message: str,
    expected_action_id: str | None = None,
) -> None:
    if response.get("Response") != "Success":
        frappe.throw(
            response.get("Message") or fallback_message,
            title=title,
        )

    response_action_id = response.get("ActionID")
    if (
        expected_action_id
        and response_action_id
        and response_action_id != expected_action_id
    ):
        frappe.throw(
            _("AMI returned a response for an unexpected action."),
            title=_("AMI Originate Failed"),
        )


def _send_ami_originate(
    settings: WhatsAppCallingSettings,
    call_doc: WhatsAppCall,
    action_id: str,
) -> dict[str, str]:
    if not settings.ami_host:
        frappe.throw(_("AMI Host is required."), title=_("Invalid Calling Settings"))
    if not settings.ami_username:
        frappe.throw(_("AMI Username is required."), title=_("Invalid Calling Settings"))

    raw_password = settings.get_password("ami_password")
    if not raw_password:
        frappe.throw(_("AMI Password is required."), title=_("Invalid Calling Settings"))
        raise RuntimeError("AMI Password is required")
    password = str(raw_password)

    timeout = max(int(settings.originate_timeout or 30), 1)
    raw_sock = socket.create_connection(
        (str(settings.ami_host), int(settings.ami_port or 5038)),
        timeout=timeout,
    )
    sock = (
        ssl.create_default_context().wrap_socket(
            raw_sock, server_hostname=str(settings.ami_host)
        )
        if settings.ami_use_tls
        else raw_sock
    )
    sock.settimeout(timeout)

    try:
        _read_ami_banner(sock)

        login_response = _send_ami_action(sock, {
            "Action": "Login",
            "Username": str(settings.ami_username),
            "Secret": password,
            "Events": "off",
        })
        _require_ami_success(
            login_response,
            title=_("AMI Login Failed"),
            fallback_message=_("AMI login returned an unexpected response."),
        )

        response = _send_ami_action(
            sock,
            _build_originate_payload(settings, call_doc, action_id),
        )
        _require_ami_success(
            response,
            title=_("AMI Originate Failed"),
            fallback_message=_("AMI originate returned an unexpected response."),
            expected_action_id=action_id,
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


def originate_call(
    call_doc: WhatsAppCall, *, raise_on_failure: bool = False
) -> WhatsAppCall:
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


def originate_pending_call(call_name: str) -> WhatsAppCall:
    call_doc = cast(
        "WhatsAppCall", frappe.get_doc("WhatsApp Call", call_name)
    )
    if call_doc.status in TERMINAL_CALL_STATUSES:
        return call_doc
    return originate_call(call_doc, raise_on_failure=False)


def _find_call_from_permission_reply(
    *,
    phone_number: str,
    whatsapp_account: str,
    context_message_id: str | None = None,
) -> WhatsAppCall | None:
    request_docname = None
    if context_message_id:
        request_docname = frappe.db.get_value(
            "WhatsApp Message",
            {"message_id": context_message_id},
            "name",
        )

    filters: dict[str, Any] = {
        "phone_number": _normalize_phone_number(phone_number),
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
        return cast(
            "WhatsAppCall",
            frappe.get_doc("WhatsApp Call", str(rows[0].name)),
        )

    if request_docname:
        rows = frappe.get_all(
            "WhatsApp Call",
            filters={
                "phone_number": _normalize_phone_number(phone_number),
                "whatsapp_account": whatsapp_account,
                "status": "Permission Requested",
            },
            fields=["name"],
            order_by="creation desc",
            limit=1,
        )
        if rows:
            return cast(
                "WhatsAppCall",
                frappe.get_doc("WhatsApp Call", str(rows[0].name)),
            )

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
        publish_call_update(
            call_doc,
            _("Call permission accepted. Click Call when you are ready."),
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


def publish_call_update(call_doc: WhatsAppCall, message: str) -> None:
    payload = {
        "event_type": "whatsapp_call_update",
        "room": call_doc.contact,
        "phone_number": _normalize_phone_number(call_doc.phone_number),
        "whatsapp_account": call_doc.whatsapp_account,
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
