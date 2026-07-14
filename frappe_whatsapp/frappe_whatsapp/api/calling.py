from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

import frappe
from frappe import _

from frappe_whatsapp.utils.calling import (
    get_call_state as get_service_call_state,
)
from frappe_whatsapp.utils.calling import (
    request_call_permission as request_service_call_permission,
)
from frappe_whatsapp.utils.calling import (
    start_outbound_call as start_service_outbound_call,
)
from frappe_whatsapp.utils.calling import (
    validate_agent_extension,
    validate_call_phone_number,
    validate_idempotency_key,
)

if TYPE_CHECKING:
    from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_call_permission.whatsapp_call_permission import (
        WhatsAppCallPermission,
    )


CALLING_API_ROLE = "WhatsApp Calling API"
_STATUS_MAP = {
    "Disabled": "unavailable",
    "Missing Agent Extension": "unavailable",
    "Unavailable": "unavailable",
    "Ready": "ready",
    "Permission Requested": "permission_pending",
    "No Permission": "permission_required",
    "Permission Accepted": "ready",
    "Permission Rejected": "permission_required",
    "PBX Queued": "pbx_queued",
    "Failed": "failed",
    "Cancelled": "failed",
}
_MAX_EXTERNAL_REFERENCE_LENGTH = 140


def _is_enabled(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    if isinstance(value, (int, float)):
        return bool(value)
    return False


def _require_calling_api_role() -> None:
    user = frappe.session.user
    roles = set(frappe.get_roles(user)) if user and user != "Guest" else set()
    if user == "Guest" or not ({CALLING_API_ROLE, "System Manager"} & roles):
        frappe.throw(
            _("You are not permitted to use the WhatsApp Calling API."),
            frappe.PermissionError,
            title=_("Not Permitted"),
        )


def _validate_external_reference(value: Any) -> str | None:
    if value in (None, ""):
        return None
    reference = str(value).strip()
    if (
        not reference
        or len(reference) > _MAX_EXTERNAL_REFERENCE_LENGTH
        or any(ord(character) < 32 for character in reference)
    ):
        frappe.throw(
            _("External Reference must be at most 140 printable characters."),
            title=_("Invalid External Reference"),
        )
    return reference


def _validate_client_context(
    *,
    phone_number: Any,
    whatsapp_account: Any,
    agent_extension: Any,
    source_app: Any,
    external_reference: Any,
) -> dict[str, str | None]:
    # Validate untrusted strings before any database or network operation.
    extension = validate_agent_extension(agent_extension)
    number = validate_call_phone_number(phone_number)
    account_name = str(whatsapp_account or "").strip()
    app_name = str(source_app or "").strip()
    if not account_name:
        frappe.throw(_("WhatsApp Account is required."))
    if not app_name:
        frappe.throw(_("Source App is required."))

    account_status = frappe.db.get_value("WhatsApp Account", account_name, "status")
    if account_status != "Active":
        frappe.throw(
            _("WhatsApp Account {0} does not exist or is not active.").format(
                account_name
            ),
            title=_("Inactive WhatsApp Account"),
        )

    app_enabled = frappe.db.get_value("WhatsApp Client App", app_name, "enabled")
    if not _is_enabled(app_enabled):
        frappe.throw(
            _("WhatsApp Client App {0} does not exist or is not enabled.").format(
                app_name
            ),
            title=_("Inactive Source App"),
        )

    return {
        "phone_number": number,
        "whatsapp_account": account_name,
        "agent_extension": extension,
        "source_app": app_name,
        "external_reference": _validate_external_reference(external_reference),
    }


def _permission_metadata(*, phone_number: str, whatsapp_account: str) -> dict[str, Any]:
    name = frappe.db.get_value(
        "WhatsApp Call Permission",
        {
            "phone_number": phone_number,
            "whatsapp_account": whatsapp_account,
        },
        "name",
    )
    if not name:
        return {
            "status": "No Permission",
            "expires_at": None,
            "last_checked_at": None,
        }
    permission = cast(
        "WhatsAppCallPermission",
        frappe.get_doc("WhatsApp Call Permission", str(name)),
    )
    return {
        "status": permission.permission_status,
        "expires_at": permission.expires_at,
        "last_checked_at": permission.last_checked_at,
    }


def _canonical_status(status: Any) -> str:
    return _STATUS_MAP.get(str(status or ""), "unavailable")


def _response(
    *,
    context: dict[str, str | None],
    service_result: dict[str, Any],
    idempotency_key: str | None = None,
) -> dict[str, Any]:
    status = _canonical_status(service_result.get("status"))
    permission = _permission_metadata(
        phone_number=str(context["phone_number"]),
        whatsapp_account=str(context["whatsapp_account"]),
    )
    pending_call_id = (
        service_result.get("pending_call") if status == "permission_pending" else None
    )
    if status == "permission_pending" and not pending_call_id:
        pending_call_id = service_result.get("call")
    call_id = service_result.get("call") if status in {"pbx_queued", "failed"} else None

    response = {
        "ok": bool(
            service_result.get(
                "ok", status in {"ready", "permission_pending", "pbx_queued"}
            )
        ),
        "status": status,
        "message": str(service_result.get("message") or ""),
        "retryable": bool(service_result.get("retryable", False)),
        "can_request_permission": bool(
            service_result.get(
                "can_request_permission", status == "permission_required"
            )
        ),
        "can_start_call": bool(service_result.get("can_call", status == "ready")),
        "permission": permission,
        "permission_status": permission["status"],
        "permission_expires_at": permission["expires_at"],
        "permission_last_checked_at": permission["last_checked_at"],
        "pending_call_id": pending_call_id,
        "call_id": call_id,
        "agent_extension": context["agent_extension"],
        "whatsapp_account": context["whatsapp_account"],
        "source_app": context["source_app"],
        "external_reference": context["external_reference"],
        "idempotency_key": idempotency_key,
    }
    if service_result.get("failure_reason"):
        response["failure_reason"] = service_result["failure_reason"]
    if service_result.get("idempotent_replay"):
        response["idempotent_replay"] = True
    return response


@frappe.whitelist(methods=["GET"])
def get_call_state(
    phone_number: str,
    whatsapp_account: str,
    agent_extension: str,
    source_app: str,
    external_reference: str | None = None,
) -> dict[str, Any]:
    _require_calling_api_role()
    context = _validate_client_context(
        phone_number=phone_number,
        whatsapp_account=whatsapp_account,
        agent_extension=agent_extension,
        source_app=source_app,
        external_reference=external_reference,
    )
    result = get_service_call_state(
        phone_number=str(context["phone_number"]),
        agent_extension=str(context["agent_extension"]),
        whatsapp_account=str(context["whatsapp_account"]),
    )
    result["ok"] = result.get("status") not in {
        "Disabled",
        "Missing Agent Extension",
        "Unavailable",
    }
    result["retryable"] = result.get("status") == "Unavailable"
    return _response(context=context, service_result=result)


def _mutation_context(
    *,
    phone_number: str,
    whatsapp_account: str,
    agent_extension: str,
    source_app: str,
    external_reference: str | None,
    idempotency_key: str,
) -> tuple[dict[str, str | None], str]:
    context = _validate_client_context(
        phone_number=phone_number,
        whatsapp_account=whatsapp_account,
        agent_extension=agent_extension,
        source_app=source_app,
        external_reference=external_reference,
    )
    return context, validate_idempotency_key(idempotency_key)


@frappe.whitelist(methods=["POST"])
def request_call_permission(
    phone_number: str,
    whatsapp_account: str,
    agent_extension: str,
    source_app: str,
    idempotency_key: str,
    external_reference: str | None = None,
) -> dict[str, Any]:
    _require_calling_api_role()
    context, key = _mutation_context(
        phone_number=phone_number,
        whatsapp_account=whatsapp_account,
        agent_extension=agent_extension,
        source_app=source_app,
        external_reference=external_reference,
        idempotency_key=idempotency_key,
    )
    result = request_service_call_permission(
        phone_number=str(context["phone_number"]),
        agent_extension=str(context["agent_extension"]),
        whatsapp_account=str(context["whatsapp_account"]),
        source_app=str(context["source_app"]),
        external_reference=context["external_reference"],
        idempotency_key=key,
    )
    return _response(context=context, service_result=result, idempotency_key=key)


@frappe.whitelist(methods=["POST"])
def start_outbound_call(
    phone_number: str,
    whatsapp_account: str,
    agent_extension: str,
    source_app: str,
    idempotency_key: str,
    external_reference: str | None = None,
) -> dict[str, Any]:
    _require_calling_api_role()
    context, key = _mutation_context(
        phone_number=phone_number,
        whatsapp_account=whatsapp_account,
        agent_extension=agent_extension,
        source_app=source_app,
        external_reference=external_reference,
        idempotency_key=idempotency_key,
    )
    result = start_service_outbound_call(
        phone_number=str(context["phone_number"]),
        agent_extension=str(context["agent_extension"]),
        whatsapp_account=str(context["whatsapp_account"]),
        source_app=str(context["source_app"]),
        external_reference=context["external_reference"],
        idempotency_key=key,
    )
    return _response(context=context, service_result=result, idempotency_key=key)
