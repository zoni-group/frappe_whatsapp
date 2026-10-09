from __future__ import annotations

import hashlib
import re
from typing import TYPE_CHECKING, Any, Iterable, cast

import frappe

from frappe_whatsapp.utils import format_number

if TYPE_CHECKING:
    from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_profile_account_state.whatsapp_profile_account_state import (
        WhatsAppProfileAccountState,
    )


PROFILE_DOCTYPE = "WhatsApp Profiles"
ALIAS_DOCTYPE = "WhatsApp Profile Alias"
ALIAS_TYPES = {"phone", "user_id", "parent_user_id"}
BSUID_PATTERN = re.compile(
    r"^[A-Z]{2}\.(?:ENT\.)?[A-Za-z0-9]{1,128}$"
)


def normalize_phone(value: Any) -> str:
    formatted = format_number(str(value or "").strip())
    return re.sub(r"\D", "", formatted)


def normalize_bsuid(value: Any, *, allow_parent: bool = True) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        return ""
    if not BSUID_PATTERN.fullmatch(normalized):
        frappe.throw("Invalid WhatsApp business-scoped user ID.")
    if not allow_parent and ".ENT." in normalized:
        frappe.throw("A parent business-scoped user ID is not valid here.")
    return normalized


def normalize_alias(alias_type: str, value: Any) -> str:
    if alias_type == "phone":
        return normalize_phone(value)
    if alias_type not in ALIAS_TYPES:
        frappe.throw(f"Unsupported WhatsApp identity alias type: {alias_type}")
    return normalize_bsuid(
        value,
        allow_parent=alias_type == "parent_user_id",
    )


def get_identity_scope(whatsapp_account: str) -> str:
    portfolio_id = frappe.db.get_value(
        "WhatsApp Account", whatsapp_account, "business_portfolio_id"
    )
    if portfolio_id:
        return f"portfolio:{portfolio_id}"
    return f"account:{whatsapp_account}"


def alias_key(identity_scope: str, alias_type: str, value: str) -> str:
    raw = f"{identity_scope}\0{alias_type}\0{value}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def identity_from_webhook(
    message: dict[str, Any], contact: dict[str, Any] | None = None
) -> dict[str, str]:
    contact = contact if isinstance(contact, dict) else {}
    profile = contact.get("profile")
    profile = profile if isinstance(profile, dict) else {}
    return {
        "phone": str(
            message.get("from")
            or contact.get("wa_id")
            or contact.get("user_wa_id")
            or ""
        ),
        "user_id": str(
            message.get("from_user_id") or contact.get("user_id") or ""
        ),
        "parent_user_id": str(
            message.get("from_parent_user_id")
            or contact.get("parent_user_id")
            or ""
        ),
        "username": str(
            message.get("username")
            or contact.get("username")
            or profile.get("username")
            or ""
        ),
        "profile_name": str(profile.get("name") or ""),
    }


def _aliases(identity: dict[str, Any]) -> list[tuple[str, str]]:
    result: list[tuple[str, str]] = []
    for alias_type in ("phone", "user_id", "parent_user_id"):
        value = normalize_alias(alias_type, identity.get(alias_type))
        if value:
            result.append((alias_type, value))
    return result


def _profile_for_legacy_phone(
    phone: str, whatsapp_account: str
) -> str | None:
    if not phone:
        return None
    rows = frappe.get_all(
        PROFILE_DOCTYPE,
        filters={"number": phone},
        fields=[
            "name", "whatsapp_account", "identity_scope", "creation"
        ],
        order_by="creation asc",
    )
    exact = [row for row in rows if row.whatsapp_account == whatsapp_account]
    unscoped = [
        row for row in rows
        if not row.whatsapp_account and not row.identity_scope
    ]
    candidate = exact or unscoped[:1]
    return str(candidate[0].name) if candidate else None


def _merge_profiles(survivor: str, duplicates: Iterable[str]) -> None:
    """Make aliases and linked operational records point at one profile.

    Profiles are retained with ``merged_into`` instead of being deleted so the
    reconciliation is auditable and does not discard consent history.
    """
    duplicate_names = [name for name in duplicates if name != survivor]
    if not duplicate_names:
        return

    links = (
        ("WhatsApp Message", "contact_profile"),
        ("WhatsApp Conversation Route", "contact_profile"),
        ("WhatsApp Blocked Contact", "contact_profile"),
        ("WhatsApp Call", "contact_profile"),
        ("WhatsApp Call Permission", "contact_profile"),
        ("WhatsApp Contact", "whatsapp_profile"),
    )
    for duplicate in duplicate_names:
        _merge_account_consent_states(survivor, duplicate)
        for doctype, fieldname in links:
            if (
                frappe.db.table_exists(doctype)
                and frappe.db.has_column(doctype, fieldname)
            ):
                frappe.db.set_value(
                    doctype,
                    {fieldname: duplicate},
                    fieldname,
                    survivor,
                    update_modified=False,
                )

        if frappe.db.has_column(PROFILE_DOCTYPE, "merged_into"):
            duplicate_doc = frappe.get_doc(PROFILE_DOCTYPE, duplicate)
            duplicate_doc.db_set("merged_into", survivor, update_modified=False)
        frappe.db.set_value(
            ALIAS_DOCTYPE,
            {"whatsapp_profile": duplicate},
            "whatsapp_profile",
            survivor,
            update_modified=False,
        )

        restrictive_rows = frappe.get_all(
            PROFILE_DOCTYPE,
            filters={"name": duplicate},
            fields=["is_opted_out", "do_not_contact"],
            limit=1,
        )
        restrictive = restrictive_rows[0] if restrictive_rows else {}
        if restrictive.get("is_opted_out"):
            frappe.db.set_value(
                PROFILE_DOCTYPE, survivor, "is_opted_out", 1,
                update_modified=False,
            )
        if restrictive.get("do_not_contact"):
            frappe.db.set_value(
                PROFILE_DOCTYPE, survivor, "do_not_contact", 1,
                update_modified=False,
            )


def _merge_account_consent_states(survivor: str, duplicate: str) -> None:
    state_doctype = "WhatsApp Profile Account State"
    if not frappe.db.table_exists(state_doctype):
        return
    from frappe_whatsapp.utils.consent import get_or_create_account_state

    states = frappe.get_all(
        state_doctype,
        filters={"whatsapp_profile": duplicate},
        fields=["name", "whatsapp_account"],
    )
    for row in states:
        duplicate_state = cast(
            "WhatsAppProfileAccountState",
            frappe.get_doc(state_doctype, row.name),
        )
        survivor_state = get_or_create_account_state(
            survivor, str(row.whatsapp_account)
        )
        if duplicate_state.do_not_contact:
            survivor_state.do_not_contact = 1
            survivor_state.do_not_contact_reason = (
                survivor_state.do_not_contact_reason
                or duplicate_state.do_not_contact_reason
            )
        if duplicate_state.is_opted_out:
            survivor_state.is_opted_out = 1
            survivor_state.is_opted_in = 0
            survivor_state.consent_status = "Opted Out"
            survivor_state.opted_out_at = (
                survivor_state.opted_out_at
                or duplicate_state.opted_out_at
            )
            survivor_state.opted_out_reason = (
                survivor_state.opted_out_reason
                or duplicate_state.opted_out_reason
            )
        elif not duplicate_state.is_opted_in and survivor_state.is_opted_in:
            # Unknown or partial consent is more restrictive than an
            # affirmative opt-in when an explicit Meta event joins records.
            survivor_state.is_opted_in = 0
            survivor_state.consent_status = (
                duplicate_state.consent_status or "Unknown"
            )

        survivor_categories = {
            row.consent_category: row
            for row in survivor_state.get("category_consents") or []
        }
        for duplicate_category in (
            duplicate_state.get("category_consents") or []
        ):
            survivor_category = survivor_categories.get(
                duplicate_category.consent_category
            )
            if survivor_category:
                if not duplicate_category.consented:
                    survivor_category.consented = 0
                    survivor_category.consented_at = (
                        survivor_category.consented_at
                        or duplicate_category.consented_at
                    )
                continue
            survivor_state.append("category_consents", {
                "consent_category": duplicate_category.consent_category,
                "consented": duplicate_category.consented,
                "consented_at": duplicate_category.consented_at,
                "consent_method": duplicate_category.consent_method,
            })
        survivor_state.save(ignore_permissions=True)


def resolve_identity(
    *,
    whatsapp_account: str,
    identity: dict[str, Any],
    explicit_link: bool = True,
) -> Any:
    """Resolve or create a stable profile from identifiers supplied by Meta."""
    scope = get_identity_scope(whatsapp_account)
    normalized_aliases = _aliases(identity)
    if not normalized_aliases:
        frappe.throw("A phone number or business-scoped user ID is required.")

    alias_names = [
        alias_key(scope, alias_type, value)
        for alias_type, value in normalized_aliases
    ]
    rows = frappe.get_all(
        ALIAS_DOCTYPE,
        filters={"name": ["in", alias_names]},
        fields=["whatsapp_profile"],
    ) if alias_names else []
    profile_names = {str(row.whatsapp_profile) for row in rows}

    phone = dict(normalized_aliases).get("phone", "")
    if not profile_names and phone:
        legacy = _profile_for_legacy_phone(phone, whatsapp_account)
        if legacy:
            profile_names.add(legacy)

    if len(profile_names) > 1 and not explicit_link:
        frappe.log_error(
            title="WhatsApp identity conflict",
            message={
                "whatsapp_account": whatsapp_account,
                "identity_scope": scope,
                "aliases": normalized_aliases,
                "profiles": sorted(profile_names),
            },
        )
        frappe.throw("WhatsApp identifiers resolve to conflicting profiles.")

    if profile_names:
        survivor = str(frappe.get_all(
            PROFILE_DOCTYPE,
            filters={"name": ["in", sorted(profile_names)]},
            fields=["name"],
            order_by="creation asc",
            limit=1,
        )[0].name)
        if len(profile_names) > 1:
            _merge_profiles(survivor, profile_names)
        profile_doc = frappe.get_doc(PROFILE_DOCTYPE, survivor)
    else:
        profile_doc = frappe.get_doc({
            "doctype": PROFILE_DOCTYPE,
            "profile_name": identity.get("profile_name") or identity.get("username"),
            "number": phone or None,
            "user_id": dict(normalized_aliases).get("user_id") or None,
            "parent_user_id": dict(normalized_aliases).get("parent_user_id") or None,
            "username": identity.get("username") or None,
            "identity_scope": scope,
            "whatsapp_account": whatsapp_account,
        }).insert(ignore_permissions=True)

    updates = {
        "identity_scope": scope,
        "whatsapp_account": profile_doc.get("whatsapp_account") or whatsapp_account,
        "number": phone or profile_doc.get("number"),
        "user_id": dict(normalized_aliases).get("user_id") or profile_doc.get("user_id"),
        "parent_user_id": (
            dict(normalized_aliases).get("parent_user_id")
            or profile_doc.get("parent_user_id")
        ),
        "username": identity.get("username") or profile_doc.get("username"),
        "profile_name": identity.get("profile_name") or profile_doc.get("profile_name"),
    }
    changed = False
    for fieldname, value in updates.items():
        if value and profile_doc.get(fieldname) != value:
            profile_doc.set(fieldname, value)
            changed = True
    if changed:
        profile_doc.save(ignore_permissions=True)

    for alias_type, value in normalized_aliases:
        key = alias_key(scope, alias_type, value)
        if not frappe.db.exists(ALIAS_DOCTYPE, key):
            try:
                frappe.get_doc({
                    "doctype": ALIAS_DOCTYPE,
                    "name": key,
                    "alias_key": key,
                    "identity_scope": scope,
                    "alias_type": alias_type,
                    "alias_value": value,
                    "whatsapp_profile": profile_doc.name,
                    "is_current": 1,
                    "whatsapp_account": whatsapp_account,
                }).insert(ignore_permissions=True)
            except frappe.UniqueValidationError:
                # A concurrent event may have created the same scoped alias
                # after the initial lookup. The alias itself is proof that
                # both workers are describing one identity, so reconcile the
                # provisional profile instead of leaving a duplicate orphan.
                concurrent_profile = frappe.db.get_value(
                    ALIAS_DOCTYPE, key, "whatsapp_profile"
                )
                if concurrent_profile and concurrent_profile != profile_doc.name:
                    survivor = str(frappe.get_all(
                        PROFILE_DOCTYPE,
                        filters={
                            "name": [
                                "in",
                                [str(concurrent_profile), str(profile_doc.name)],
                            ]
                        },
                        fields=["name"],
                        order_by="creation asc",
                        limit=1,
                    )[0].name)
                    _merge_profiles(
                        survivor,
                        [str(concurrent_profile), str(profile_doc.name)],
                    )
                    profile_doc = frappe.get_doc(PROFILE_DOCTYPE, survivor)
        frappe.db.set_value(
            ALIAS_DOCTYPE,
            key,
            {"whatsapp_profile": profile_doc.name, "is_current": 1},
            update_modified=False,
        )
        frappe.db.set_value(
            ALIAS_DOCTYPE,
            {
                "whatsapp_profile": profile_doc.name,
                "alias_type": alias_type,
                "name": ["!=", key],
            },
            "is_current",
            0,
            update_modified=False,
        )

    profile_doc.reload()
    return profile_doc


def apply_user_id_update(
    *, whatsapp_account: str, update: dict[str, Any]
) -> Any:
    identity = {
        "phone": update.get("wa_id") or update.get("phone"),
        "user_id": update.get("current_user_id") or update.get("user_id"),
        "parent_user_id": (
            update.get("current_parent_user_id") or update.get("parent_user_id")
        ),
        "username": update.get("username"),
    }
    previous = {
        "phone": update.get("previous_wa_id") or update.get("previous_phone"),
        "user_id": update.get("previous_user_id"),
        "parent_user_id": update.get("previous_parent_user_id"),
    }
    scope = get_identity_scope(whatsapp_account)
    previous_keys = []
    for alias_type, raw_value in previous.items():
        value = normalize_alias(alias_type, raw_value)
        if value:
            previous_keys.append(alias_key(scope, alias_type, value))

    profile = None
    for key in previous_keys:
        profile_name = frappe.db.get_value(ALIAS_DOCTYPE, key, "whatsapp_profile")
        if profile_name:
            profile = frappe.get_doc(PROFILE_DOCTYPE, str(profile_name))
            break
    if profile:
        for alias_type, value in _aliases(identity):
            key = alias_key(scope, alias_type, value)
            if not frappe.db.exists(ALIAS_DOCTYPE, key):
                frappe.get_doc({
                    "doctype": ALIAS_DOCTYPE,
                    "name": key,
                    "alias_key": key,
                    "identity_scope": scope,
                    "alias_type": alias_type,
                    "alias_value": value,
                    "whatsapp_profile": profile.name,
                    "is_current": 1,
                    "whatsapp_account": whatsapp_account,
                }).insert(ignore_permissions=True)
        resolved = resolve_identity(
            whatsapp_account=whatsapp_account,
            identity=identity,
            explicit_link=True,
        )
        _enqueue_block_reapply(
            whatsapp_account=whatsapp_account,
            profile_name=str(resolved.name),
            user_id=str(resolved.get("user_id") or ""),
        )
        return resolved
    resolved = resolve_identity(
        whatsapp_account=whatsapp_account,
        identity=identity,
        explicit_link=True,
    )
    _enqueue_block_reapply(
        whatsapp_account=whatsapp_account,
        profile_name=str(resolved.name),
        user_id=str(resolved.get("user_id") or ""),
    )
    return resolved


def _enqueue_block_reapply(
    *, whatsapp_account: str, profile_name: str, user_id: str
) -> None:
    if not user_id:
        return
    from frappe_whatsapp.utils.blocking import is_contact_blocked

    if not is_contact_blocked(
        whatsapp_account=whatsapp_account,
        contact_profile=profile_name,
    ):
        return
    frappe.enqueue(
        "frappe_whatsapp.utils.blocking.reapply_block_after_rotation",
        queue="short",
        enqueue_after_commit=True,
        whatsapp_account=whatsapp_account,
        contact_profile=profile_name,
        user_id=user_id,
    )


def profile_identity(profile: Any) -> dict[str, Any]:
    phone = str(profile.get("number") or "") or None
    user_id = str(profile.get("user_id") or "") or None
    parent_user_id = str(profile.get("parent_user_id") or "") or None
    preferred = phone or user_id or parent_user_id
    return {
        "profile_id": str(profile.name),
        "phone": phone,
        "user_id": user_id,
        "parent_user_id": parent_user_id,
        "username": str(profile.get("username") or "") or None,
        "preferred_recipient": preferred,
    }


def set_meta_recipient(payload: dict[str, Any], *, to: Any, recipient: Any) -> str:
    phone = normalize_phone(to)
    bsuid = normalize_bsuid(recipient) if recipient else ""
    if phone:
        payload["to"] = phone
        payload.pop("recipient", None)
        return "phone"
    if bsuid:
        payload["recipient"] = bsuid
        payload.pop("to", None)
        return "parent_user_id" if ".ENT." in bsuid else "user_id"
    frappe.throw("A phone number or business-scoped user ID is required.")
    return ""
