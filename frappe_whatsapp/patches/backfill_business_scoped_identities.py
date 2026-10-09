from __future__ import annotations

from typing import cast

import frappe

from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_profiles.whatsapp_profiles import (
    WhatsAppProfiles,
)

from frappe_whatsapp.utils.identity import (
    ALIAS_DOCTYPE,
    alias_key,
    get_identity_scope,
    normalize_phone,
)


def execute() -> None:
    if not frappe.db.table_exists(ALIAS_DOCTYPE):
        return

    profiles = frappe.get_all(
        "WhatsApp Profiles",
        fields=["name", "number", "whatsapp_account", "identity_scope"],
        order_by="creation asc",
    )
    for profile in profiles:
        profile_name = str(profile.name)
        number = normalize_phone(profile.number)
        accounts = _accounts_for_profile(
            profile_name,
            number,
            str(profile.whatsapp_account or ""),
        )
        if not accounts:
            continue
        primary_account = (
            str(profile.whatsapp_account)
            if profile.whatsapp_account in accounts
            else accounts[0]
        )
        profile_by_scope = {
            get_identity_scope(primary_account): profile_name
        }
        for account in accounts:
            scope = get_identity_scope(account)
            target_profile = profile_by_scope.get(scope)
            if not target_profile:
                existing_alias_profile = (
                    frappe.db.get_value(
                        ALIAS_DOCTYPE,
                        alias_key(scope, "phone", number),
                        "whatsapp_profile",
                    )
                    if number else None
                )
                target_profile = str(existing_alias_profile or "") or (
                    _clone_profile_for_scope(profile_name, account, scope)
                )
                profile_by_scope[scope] = target_profile
                if target_profile != profile_name:
                    _relink_profile_records(
                        source_profile=profile_name,
                        target_profile=target_profile,
                        whatsapp_account=account,
                    )
            frappe.db.set_value(
                "WhatsApp Profiles",
                target_profile,
                {"whatsapp_account": account, "identity_scope": scope},
                update_modified=False,
            )
            _ensure_phone_alias(
                profile_name=target_profile,
                account=account,
                scope=scope,
                number=number,
            )
            _ensure_account_state(target_profile, account)

    _backfill_message_profiles()
    _backfill_operational_profile_links()
    _backfill_client_app_accounts()


def _infer_account(profile_name: str, number: str) -> str:
    account = frappe.db.get_value(
        "WhatsApp Message",
        {"contact_profile": profile_name},
        "whatsapp_account",
    ) if frappe.db.has_column("WhatsApp Message", "contact_profile") else None
    if account:
        return str(account)
    if number:
        account = frappe.db.get_value(
            "WhatsApp Message",
            [["WhatsApp Message", "from", "=", number]],
            "whatsapp_account",
        )
        if account:
            return str(account)
    return str(frappe.db.get_value(
        "WhatsApp Account", {"is_default_incoming": 1}, "name"
    ) or "")


def _accounts_for_profile(
    profile_name: str, number: str, configured_account: str
) -> list[str]:
    accounts = {configured_account} if configured_account else set()
    sources = (
        ("WhatsApp Message", "contact_profile", profile_name),
        ("WhatsApp Conversation Route", "contact_profile", profile_name),
        ("WhatsApp Blocked Contact", "contact_profile", profile_name),
        ("WhatsApp Call", "contact_profile", profile_name),
        ("WhatsApp Call Permission", "contact_profile", profile_name),
        ("WhatsApp Contact", "whatsapp_profile", profile_name),
    )
    for doctype, fieldname, value in sources:
        if (
            frappe.db.table_exists(doctype)
            and frappe.db.has_column(doctype, fieldname)
            and frappe.db.has_column(doctype, "whatsapp_account")
        ):
            accounts.update(
                str(account)
                for account in frappe.get_all(
                    doctype,
                    filters={fieldname: value},
                    pluck="whatsapp_account",
                )
                if account
            )
    if number:
        for fieldname in ("from", "to"):
            accounts.update(
                str(account)
                for account in frappe.get_all(
                    "WhatsApp Message",
                    filters={fieldname: number},
                    pluck="whatsapp_account",
                )
                if account
            )
        if frappe.db.table_exists("WhatsApp Conversation Route"):
            accounts.update(
                str(account)
                for account in frappe.get_all(
                    "WhatsApp Conversation Route",
                    filters={"contact_number": number},
                    pluck="whatsapp_account",
                )
                if account
            )
    if not accounts:
        inferred = _infer_account(profile_name, number)
        if inferred:
            accounts.add(inferred)
    return sorted(accounts)


def _clone_profile_for_scope(
    source_profile: str, account: str, scope: str
) -> str:
    source = frappe.get_doc("WhatsApp Profiles", source_profile)
    clone = cast(WhatsAppProfiles, frappe.copy_doc(source))
    clone.name = None
    clone.whatsapp_account = account
    clone.identity_scope = scope
    clone.merged_into = None
    clone.insert(ignore_permissions=True)
    return str(clone.name)


def _relink_profile_records(
    *, source_profile: str, target_profile: str, whatsapp_account: str
) -> None:
    links = (
        ("WhatsApp Message", "contact_profile"),
        ("WhatsApp Conversation Route", "contact_profile"),
        ("WhatsApp Blocked Contact", "contact_profile"),
        ("WhatsApp Call", "contact_profile"),
        ("WhatsApp Call Permission", "contact_profile"),
        ("WhatsApp Contact", "whatsapp_profile"),
    )
    for doctype, fieldname in links:
        if (
            frappe.db.table_exists(doctype)
            and frappe.db.has_column(doctype, fieldname)
            and frappe.db.has_column(doctype, "whatsapp_account")
        ):
            frappe.db.set_value(
                doctype,
                {
                    fieldname: source_profile,
                    "whatsapp_account": whatsapp_account,
                },
                fieldname,
                target_profile,
                update_modified=False,
            )


def _ensure_phone_alias(
    *, profile_name: str, account: str, scope: str, number: str
) -> None:
    if not number:
        return
    key = alias_key(scope, "phone", number)
    if frappe.db.exists(ALIAS_DOCTYPE, key):
        existing_profile = frappe.db.get_value(
            ALIAS_DOCTYPE, key, "whatsapp_profile"
        )
        if existing_profile != profile_name:
            frappe.log_error(
                title="WhatsApp identity migration collision",
                message={
                    "alias_key": key,
                    "number": number,
                    "profiles": [existing_profile, profile_name],
                },
            )
        return
    frappe.get_doc({
        "doctype": ALIAS_DOCTYPE,
        "name": key,
        "alias_key": key,
        "identity_scope": scope,
        "alias_type": "phone",
        "alias_value": number,
        "whatsapp_profile": profile_name,
        "is_current": 1,
        "whatsapp_account": account,
    }).insert(ignore_permissions=True)


def _ensure_account_state(profile_name: str, account: str) -> None:
    if not frappe.db.table_exists("WhatsApp Profile Account State"):
        return
    from frappe_whatsapp.utils.consent import get_or_create_account_state

    get_or_create_account_state(profile_name, account)


def _backfill_message_profiles() -> None:
    if not frappe.db.has_column("WhatsApp Message", "contact_profile"):
        return
    if frappe.db.has_column("WhatsApp Message", "client_event_queued"):
        frappe.db.sql(
            "UPDATE `tabWhatsApp Message` "
            "SET `client_event_queued` = 1"
        )
    messages = frappe.get_all(
        "WhatsApp Message",
        filters={"contact_profile": ["is", "not set"]},
        fields=["name", "type", "from", "to", "whatsapp_account"],
    )
    for message in messages:
        account = str(message.whatsapp_account or "")
        number = normalize_phone(
            message.get("from") if message.type == "Incoming" else message.to
        )
        if not (account and number):
            continue
        key = alias_key(get_identity_scope(account), "phone", number)
        profile = frappe.db.get_value(ALIAS_DOCTYPE, key, "whatsapp_profile")
        if profile:
            frappe.db.set_value(
                "WhatsApp Message", message.name, "contact_profile", profile,
                update_modified=False,
            )


def _backfill_client_app_accounts() -> None:
    if not frappe.db.table_exists("WhatsApp Client App Account"):
        return
    apps = frappe.get_all(
        "WhatsApp Client App", fields=["name", "outbound_default_account"]
    )
    for app in apps:
        accounts = set(frappe.get_all(
            "WhatsApp Account",
            filters={"whatsapp_client_app": app.name},
            pluck="name",
        ))
        if app.outbound_default_account:
            accounts.add(str(app.outbound_default_account))
        for account in accounts:
            if frappe.db.exists(
                "WhatsApp Client App Account",
                {"parent": app.name, "whatsapp_account": account},
            ):
                continue
            frappe.get_doc({
                "doctype": "WhatsApp Client App Account",
                "parent": app.name,
                "parenttype": "WhatsApp Client App",
                "parentfield": "allowed_accounts",
                "whatsapp_account": account,
            }).insert(ignore_permissions=True)


def _backfill_operational_profile_links() -> None:
    mappings = (
        ("WhatsApp Conversation Route", "contact_profile", "contact_number"),
        ("WhatsApp Blocked Contact", "contact_profile", "contact_number"),
        ("WhatsApp Call", "contact_profile", "phone_number"),
        ("WhatsApp Call Permission", "contact_profile", "phone_number"),
    )
    for doctype, profile_field, phone_field in mappings:
        if not (
            frappe.db.table_exists(doctype)
            and frappe.db.has_column(doctype, profile_field)
            and frappe.db.has_column(doctype, phone_field)
            and frappe.db.has_column(doctype, "whatsapp_account")
        ):
            continue
        rows = frappe.get_all(
            doctype,
            filters={profile_field: ["is", "not set"]},
            fields=["name", phone_field, "whatsapp_account"],
        )
        for row in rows:
            account = str(row.whatsapp_account or "")
            number = normalize_phone(row.get(phone_field))
            if not (account and number):
                continue
            profile = frappe.db.get_value(
                ALIAS_DOCTYPE,
                alias_key(get_identity_scope(account), "phone", number),
                "whatsapp_profile",
            )
            if profile:
                frappe.db.set_value(
                    doctype,
                    row.name,
                    profile_field,
                    profile,
                    update_modified=False,
                )
