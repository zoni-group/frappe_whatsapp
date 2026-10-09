from __future__ import annotations

from typing import Any, cast

import frappe
from frappe import _

from frappe_whatsapp.utils.blocking import (
    BLOCKED_CONTACT_DOCTYPE,
    block_contact as _block_contact,
    list_local_blocked_contacts,
    list_meta_blocked_contacts,
    normalize_block_number,
    unblock_contact as _unblock_contact,
)


def _truthy(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() not in {"0", "false", "no", "off", ""}
    return bool(value)


def _require_block_permission() -> None:
    if (
        not frappe.has_permission(BLOCKED_CONTACT_DOCTYPE, "write")
        and not frappe.has_permission(BLOCKED_CONTACT_DOCTYPE, "create")
    ):
        frappe.throw(
            _("Not permitted to manage WhatsApp blocked contacts."),
            frappe.PermissionError,
        )


def _require_block_read_permission() -> None:
    if not frappe.has_permission(BLOCKED_CONTACT_DOCTYPE, "read"):
        frappe.throw(
            _("Not permitted to view WhatsApp blocked contacts."),
            frappe.PermissionError,
        )


def _resolve_from_message(
        *, message_name: str | None,
        contact_number: str | None,
        user_id: str | None,
        contact_profile: str | None,
        whatsapp_account: str | None,
        source_app: str | None) -> tuple[str | None, str | None, str | None, str, str | None]:
    if not message_name:
        if not ((contact_number or user_id) and whatsapp_account):
            frappe.throw(
                _("Provide message_name, or an identity and whatsapp_account."))
        return contact_number, user_id, contact_profile, str(whatsapp_account), source_app

    from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_message.whatsapp_message import WhatsAppMessage  # noqa: E501

    message = cast(WhatsAppMessage,
                   frappe.get_doc("WhatsApp Message", message_name))
    resolved_number = contact_number or message.get("from") or message.to
    resolved_user_id = (
        user_id or message.get("from_user_id")
        or message.get("recipient_user_id") or message.get("recipient")
    )
    if resolved_user_id and ".ENT." in str(resolved_user_id):
        resolved_user_id = None
    resolved_profile = contact_profile or message.get("contact_profile")
    resolved_account = whatsapp_account or message.whatsapp_account
    resolved_app = (
        source_app
        or cast(str | None, message.get("routed_app"))
        or cast(str | None, message.get("source_app"))
    )
    if not ((resolved_number or resolved_user_id) and resolved_account):
        frappe.throw(
            _("Could not resolve a blockable identity and WhatsApp Account from message."))
    return (
        str(resolved_number) if resolved_number else None,
        str(resolved_user_id) if resolved_user_id else None,
        str(resolved_profile) if resolved_profile else None,
        str(resolved_account),
        resolved_app,
    )


def _resolve_from_profile(
        *, profile_name: str, whatsapp_account: str | None = None
        ) -> tuple[str | None, str | None, str, str | None]:
    from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_profiles.whatsapp_profiles import WhatsAppProfiles  # noqa: E501

    profile = cast(
        WhatsAppProfiles,
        frappe.get_doc("WhatsApp Profiles", profile_name),
    )
    number = str(profile.number or "") or None
    user_id = str(profile.get("user_id") or "") or None
    account = whatsapp_account or cast(
        str | None,
        profile.get("whatsapp_account"),
    )
    if not (number or user_id):
        frappe.throw(_("WhatsApp Profile has no blockable identity."))
    return number, user_id, str(profile.name), account


def _get_local_block_state(
        *, contact_number: str, whatsapp_account: str,
        user_id: str | None = None,
        contact_profile: str | None = None) -> dict[str, Any]:
    number = normalize_block_number(contact_number)
    filters: dict[str, Any] = {"whatsapp_account": whatsapp_account}
    filters[
        "contact_profile" if contact_profile else (
            "user_id" if user_id else "contact_number"
        )
    ] = contact_profile or user_id or number
    records = frappe.get_all(
        BLOCKED_CONTACT_DOCTYPE,
        filters=filters,
        fields=[
            "name",
            "contact_number",
            "whatsapp_account",
            "is_blocked",
            "meta_status",
            "meta_wa_id",
            "reason",
            "blocked_at",
            "unblocked_at",
            "last_synced_at",
            "last_error",
        ],
        limit=1,
    )
    record = records[0] if records else None
    return {
        "is_blocked": bool(record and record.get("is_blocked")),
        "block_record": record,
    }


@frappe.whitelist()
def block_contact(
    contact_number: str | None = None,
    user_id: str | None = None,
    contact_profile: str | None = None,
    whatsapp_account: str | None = None,
    message_name: str | None = None,
    reason: str | None = None,
    source_app: str | None = None,
    sync_meta: int | str | bool = 1,
):
    """Block a WhatsApp contact locally and best-effort at Meta.

    Intended for authenticated clients such as Zoni CRM.  Passing
    ``message_name`` is the most reliable call shape because it lets this app
    resolve both the sender number and the WhatsApp Account from the inbound
    message Zoni already received.
    """
    _require_block_permission()
    number, resolved_user_id, profile, account, resolved_app = _resolve_from_message(
        message_name=message_name,
        contact_number=contact_number,
        user_id=user_id,
        contact_profile=contact_profile,
        whatsapp_account=whatsapp_account,
        source_app=source_app,
    )
    return _block_contact(
        whatsapp_account=account,
        contact_number=number,
        user_id=resolved_user_id,
        contact_profile=profile,
        reason=reason,
        source_app=resolved_app,
        source_message=message_name,
        sync_meta=_truthy(sync_meta),
    )


@frappe.whitelist()
def unblock_contact(
    contact_number: str | None = None,
    user_id: str | None = None,
    contact_profile: str | None = None,
    whatsapp_account: str | None = None,
    message_name: str | None = None,
    reason: str | None = None,
    source_app: str | None = None,
    sync_meta: int | str | bool = 1,
):
    _require_block_permission()
    number, resolved_user_id, profile, account, resolved_app = _resolve_from_message(
        message_name=message_name,
        contact_number=contact_number,
        user_id=user_id,
        contact_profile=contact_profile,
        whatsapp_account=whatsapp_account,
        source_app=source_app,
    )
    return _unblock_contact(
        whatsapp_account=account,
        contact_number=number,
        user_id=resolved_user_id,
        contact_profile=profile,
        reason=reason,
        source_app=resolved_app,
        source_message=message_name,
        sync_meta=_truthy(sync_meta),
    )


@frappe.whitelist()
def get_blocked_contacts(
        whatsapp_account: str | None = None,
        sync_meta: int | str | bool = 0,
        limit: int | str = 100):
    _require_block_permission()
    local = list_local_blocked_contacts(
        whatsapp_account=whatsapp_account,
        limit=int(limit or 100),
    )
    meta = None
    if _truthy(sync_meta):
        if not whatsapp_account:
            frappe.throw(
                _("whatsapp_account is required when sync_meta is enabled."))
        assert whatsapp_account is not None
        meta = list_meta_blocked_contacts(
            whatsapp_account=whatsapp_account,
            limit=int(limit or 100),
        )
    return {"local": local, "meta": meta}


@frappe.whitelist()
def get_profile_block_state(
        profile_name: str,
        whatsapp_account: str | None = None):
    _require_block_read_permission()
    number, user_id, profile, account = _resolve_from_profile(
        profile_name=profile_name,
        whatsapp_account=whatsapp_account,
    )
    if not account:
        return {
            "profile": profile_name,
            "contact_number": normalize_block_number(number),
            "user_id": user_id,
            "whatsapp_account": None,
            "requires_whatsapp_account": True,
            "is_blocked": False,
            "block_record": None,
        }

    state = _get_local_block_state(
        contact_number=number or "",
        whatsapp_account=account,
        user_id=user_id,
        contact_profile=profile,
    )
    return {
        "profile": profile_name,
        "contact_number": normalize_block_number(number),
        "whatsapp_account": account,
        "requires_whatsapp_account": False,
        **state,
    }


@frappe.whitelist()
def block_profile_contact(
    profile_name: str,
    whatsapp_account: str | None = None,
    reason: str | None = None,
    sync_meta: int | str | bool = 1,
):
    _require_block_permission()
    number, user_id, profile, account = _resolve_from_profile(
        profile_name=profile_name,
        whatsapp_account=whatsapp_account,
    )
    if not account:
        frappe.throw(_("Select a WhatsApp Account before blocking."))
    assert account is not None

    return _block_contact(
        whatsapp_account=account,
        contact_number=number,
        user_id=user_id,
        contact_profile=profile,
        reason=reason,
        sync_meta=_truthy(sync_meta),
    )


@frappe.whitelist()
def unblock_profile_contact(
    profile_name: str,
    whatsapp_account: str | None = None,
    reason: str | None = None,
    sync_meta: int | str | bool = 1,
):
    _require_block_permission()
    number, user_id, profile, account = _resolve_from_profile(
        profile_name=profile_name,
        whatsapp_account=whatsapp_account,
    )
    if not account:
        frappe.throw(_("Select a WhatsApp Account before unblocking."))
    assert account is not None

    return _unblock_contact(
        whatsapp_account=account,
        contact_number=number,
        user_id=user_id,
        contact_profile=profile,
        reason=reason,
        sync_meta=_truthy(sync_meta),
    )
