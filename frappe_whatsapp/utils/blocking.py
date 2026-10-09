from __future__ import annotations

import json
from typing import Any, Literal, cast

import frappe
import requests
from frappe import _
from frappe.utils import now_datetime

from frappe_whatsapp.utils import format_number, get_whatsapp_account
from frappe_whatsapp.utils.identity import normalize_bsuid


BLOCKED_CONTACT_DOCTYPE = "WhatsApp Blocked Contact"
META_TIMEOUT_SECONDS = 30


class MetaBlockRequestError(frappe.ValidationError):
    def __init__(
            self, message: str, *,
            payload: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.payload = payload or {}


def _doctype_exists() -> bool:
    try:
        return bool(frappe.db.table_exists(BLOCKED_CONTACT_DOCTYPE))
    except Exception:
        return False


def normalize_block_number(contact_number: str | None) -> str:
    return format_number(str(contact_number or "").strip())


def _meta_user_value(contact_number: str) -> str:
    number = normalize_block_number(contact_number)
    if not number:
        return ""
    return f"+{number}"


def _blocked_contact_name(
        *, whatsapp_account: str, contact_number: str) -> str:
    return f"{normalize_block_number(contact_number)}-{whatsapp_account}"


def is_contact_blocked(
        *, whatsapp_account: str | None,
        contact_number: str | None = None,
        contact_profile: str | None = None,
        user_id: str | None = None) -> bool:
    """Return True when a contact is locally blocked for an account."""
    if not whatsapp_account:
        return False
    if not _doctype_exists():
        return False

    number = normalize_block_number(contact_number)
    for fieldname, value in (
        ("contact_profile", contact_profile),
        ("user_id", user_id),
        ("contact_number", number),
    ):
        if value and frappe.db.get_value(
            BLOCKED_CONTACT_DOCTYPE,
            {"whatsapp_account": whatsapp_account, fieldname: value,
             "is_blocked": 1},
            "name",
        ):
            return True
    return False


def _get_account(whatsapp_account: str | None = None) -> Any:
    account = (
        frappe.get_doc("WhatsApp Account", whatsapp_account)
        if whatsapp_account
        else get_whatsapp_account(account_type="incoming")
    )
    if not account:
        frappe.throw(_("WhatsApp Account is required."))
    return account


def _get_response_json(response) -> dict[str, Any]:
    try:
        payload = response.json() if response.content else {}
    except Exception:
        payload = {"raw_response": response.text}
    return payload if isinstance(payload, dict) else {"response": payload}


def _raise_meta_error(response, payload: dict[str, Any]) -> None:
    message = ""
    error = payload.get("error")
    if isinstance(error, dict):
        message = str(error.get("message") or "")
    message = message or response.text or _("Meta block request failed.")
    raise MetaBlockRequestError(
        _("{0} (HTTP {1})").format(message, response.status_code),
        payload=payload,
    )


def _call_meta_block_users(
    *,
    whatsapp_account: str,
    contact_number: str | None = None,
    user_id: str | None = None,
    action: Literal["block", "unblock"],
) -> dict[str, Any]:
    account = _get_account(whatsapp_account)
    token = account.get_password("token")
    if not token:
        frappe.throw(_("WhatsApp Account token is required."))
    if not getattr(account, "phone_id", None):
        frappe.throw(_("WhatsApp Account Phone ID is required."))

    normalized_user_id = normalize_bsuid(user_id, allow_parent=False) if user_id else ""
    user_value = _meta_user_value(str(contact_number or ""))
    if not (user_value or normalized_user_id):
        frappe.throw(_("A contact number or regular BSUID is required."))

    url = f"{account.url}/{account.version}/{account.phone_id}/block_users"
    response = requests.request(
        "POST" if action == "block" else "DELETE",
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        json={
            "messaging_product": "whatsapp",
            "block_users": [
                ({"user": user_value}
                 if user_value else {"user_id": normalized_user_id})
            ],
        },
        timeout=META_TIMEOUT_SECONDS,
    )
    payload = _get_response_json(response)
    if not response.ok:
        _raise_meta_error(response, payload)
    return payload


def _extract_meta_user(
    *,
    payload: dict[str, Any] | None,
    action: Literal["block", "unblock"],
) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}

    block_users = payload.get("block_users")
    if not isinstance(block_users, dict):
        return {}

    key = "added_users" if action == "block" else "removed_users"
    users = block_users.get(key)
    if isinstance(users, list):
        return next((user for user in users if isinstance(user, dict)), {})

    return {}


def _upsert_local_block(
    *,
    whatsapp_account: str,
    contact_number: str | None = None,
    user_id: str | None = None,
    contact_profile: str | None = None,
    is_blocked: bool,
    reason: str | None = None,
    source_app: str | None = None,
    source_message: str | None = None,
):
    number = normalize_block_number(contact_number)
    normalized_user_id = normalize_bsuid(user_id, allow_parent=False) if user_id else ""
    if not (number or normalized_user_id):
        frappe.throw(_("A contact number or regular BSUID is required."))
    filters = {"whatsapp_account": whatsapp_account}
    filters["contact_profile" if contact_profile else (
        "user_id" if normalized_user_id else "contact_number"
    )] = contact_profile or normalized_user_id or number
    values = {
        "is_blocked": 1 if is_blocked else 0,
        "reason": reason,
        "source_app": source_app,
        "source_message": source_message,
    }
    if number:
        values["contact_number"] = number
    if normalized_user_id:
        values["user_id"] = normalized_user_id
    if contact_profile:
        values["contact_profile"] = contact_profile
    if is_blocked:
        values["blocked_at"] = now_datetime()
        values["unblocked_at"] = None
    else:
        values["unblocked_at"] = now_datetime()

    name = frappe.db.get_value(BLOCKED_CONTACT_DOCTYPE, filters, "name")
    if name:
        name = str(name)
        frappe.db.set_value(
            BLOCKED_CONTACT_DOCTYPE,
            name,
            values,
            update_modified=True,
        )
        return frappe.get_doc(BLOCKED_CONTACT_DOCTYPE, name)

    doc = frappe.get_doc({
        "doctype": BLOCKED_CONTACT_DOCTYPE,
        "whatsapp_account": whatsapp_account,
        "contact_number": number or None,
        "user_id": normalized_user_id or None,
        "contact_profile": contact_profile,
        "meta_status": "Not Synced",
        **values,
    })
    doc.insert(ignore_permissions=True)
    return doc


def _record_meta_success(
    *,
    doc,
    payload: dict[str, Any],
    action: Literal["block", "unblock"],
) -> None:
    user = _extract_meta_user(payload=payload, action=action)
    doc.db_set({
        "meta_status": "Blocked" if action == "block" else "Unblocked",
        "meta_input": user.get("input"),
        "meta_wa_id": user.get("wa_id"),
        "last_error": "",
        "last_error_payload": None,
        "last_synced_at": now_datetime(),
    }, update_modified=True)


def _record_meta_failure(*, doc, error: Exception) -> dict[str, Any]:
    payload = getattr(error, "payload", {}) or {}
    response = getattr(error, "response", None)
    if response is not None:
        payload = _get_response_json(response)

    error_payload = (
        {"error": str(error), "payload": payload}
        if payload else {"error": str(error)}
    )
    doc.db_set({
        "meta_status": "Failed",
        "last_error": str(error),
        "last_error_payload": json.dumps(error_payload, default=str),
        "last_synced_at": now_datetime(),
    }, update_modified=True)
    return payload


def block_contact(
    *,
    whatsapp_account: str,
    contact_number: str | None = None,
    user_id: str | None = None,
    contact_profile: str | None = None,
    reason: str | None = None,
    source_app: str | None = None,
    source_message: str | None = None,
    sync_meta: bool = True,
) -> dict[str, Any]:
    """Block a contact locally first, then best-effort sync to Meta."""
    doc = _upsert_local_block(
        whatsapp_account=whatsapp_account,
        contact_number=contact_number,
        user_id=user_id,
        contact_profile=contact_profile,
        is_blocked=True,
        reason=reason,
        source_app=source_app,
        source_message=source_message,
    )

    meta: dict[str, Any] = {"ok": False, "skipped": True}
    if sync_meta:
        try:
            payload = _call_meta_block_users(
                whatsapp_account=whatsapp_account,
                contact_number=contact_number,
                user_id=user_id,
                action="block",
            )
            _record_meta_success(doc=doc, payload=payload, action="block")
            meta = {"ok": True, "payload": payload}
        except Exception as exc:
            payload = _record_meta_failure(doc=doc, error=exc)
            meta = {"ok": False, "error": str(exc), "payload": payload}

    return {
        "ok": True,
        "local_blocked": True,
        "name": doc.name,
        "contact_number": normalize_block_number(contact_number),
        "user_id": user_id,
        "contact_profile": contact_profile,
        "whatsapp_account": whatsapp_account,
        "meta": meta,
    }


def reapply_block_after_rotation(
    *, whatsapp_account: str, contact_profile: str, user_id: str
) -> None:
    """Reapply an active profile/account block to a rotated regular BSUID."""
    if not is_contact_blocked(
        whatsapp_account=whatsapp_account,
        contact_profile=contact_profile,
    ):
        return
    block_contact(
        whatsapp_account=whatsapp_account,
        user_id=user_id,
        contact_profile=contact_profile,
        reason="Reapplied after WhatsApp user ID rotation",
        sync_meta=True,
    )


def unblock_contact(
    *,
    whatsapp_account: str,
    contact_number: str | None = None,
    user_id: str | None = None,
    contact_profile: str | None = None,
    reason: str | None = None,
    source_app: str | None = None,
    source_message: str | None = None,
    sync_meta: bool = True,
) -> dict[str, Any]:
    doc = _upsert_local_block(
        whatsapp_account=whatsapp_account,
        contact_number=contact_number,
        user_id=user_id,
        contact_profile=contact_profile,
        is_blocked=False,
        reason=reason,
        source_app=source_app,
        source_message=source_message,
    )

    meta: dict[str, Any] = {"ok": False, "skipped": True}
    if sync_meta:
        try:
            payload = _call_meta_block_users(
                whatsapp_account=whatsapp_account,
                contact_number=contact_number,
                user_id=user_id,
                action="unblock",
            )
            _record_meta_success(doc=doc, payload=payload, action="unblock")
            meta = {"ok": True, "payload": payload}
        except Exception as exc:
            payload = _record_meta_failure(doc=doc, error=exc)
            meta = {"ok": False, "error": str(exc), "payload": payload}

    return {
        "ok": True,
        "local_blocked": False,
        "name": doc.name,
        "contact_number": normalize_block_number(contact_number),
        "user_id": user_id,
        "contact_profile": contact_profile,
        "whatsapp_account": whatsapp_account,
        "meta": meta,
    }


def list_local_blocked_contacts(
        *, whatsapp_account: str | None = None,
        limit: int = 100) -> list[dict[str, Any]]:
    if not _doctype_exists():
        return []

    filters: dict[str, Any] = {"is_blocked": 1}
    if whatsapp_account:
        filters["whatsapp_account"] = whatsapp_account

    return cast(list[dict[str, Any]], frappe.get_all(
        BLOCKED_CONTACT_DOCTYPE,
        filters=filters,
        fields=[
            "name",
            "whatsapp_account",
            "contact_number",
            "user_id",
            "contact_profile",
            "meta_status",
            "source_app",
            "source_message",
            "reason",
            "blocked_at",
            "last_synced_at",
        ],
        order_by="modified desc",
        limit=limit,
    ))


def list_meta_blocked_contacts(
        *, whatsapp_account: str,
        limit: int = 100) -> dict[str, Any]:
    account = _get_account(whatsapp_account)
    token = account.get_password("token")
    if not token:
        frappe.throw(_("WhatsApp Account token is required."))

    response = requests.get(
        f"{account.url}/{account.version}/{account.phone_id}/block_users",
        headers={"Authorization": f"Bearer {token}"},
        params={"limit": limit},
        timeout=META_TIMEOUT_SECONDS,
    )
    payload = _get_response_json(response)
    if not response.ok:
        _raise_meta_error(response, payload)
    return payload
