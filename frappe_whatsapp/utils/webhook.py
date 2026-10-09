"""Webhook."""
import frappe
import hashlib
import hmac
import json
import re
import requests
from frappe.utils import cint
from frappe.utils.password import get_decrypted_password as \
    _get_decrypted_password
from werkzeug.wrappers import Response
from typing import cast, Any

from frappe_whatsapp.utils import get_whatsapp_account
from frappe_whatsapp.utils.blocking import is_contact_blocked
from frappe_whatsapp.utils.routing import resolve_incoming_routed_app, \
    forward_incoming_to_app_async
from frappe_whatsapp.utils.consent import (
    check_opt_out_keyword,
    check_opt_in_keyword,
    process_opt_out,
    process_opt_in,
    send_opt_out_confirmation,
    send_opt_in_confirmation,
)
from frappe_whatsapp.utils.campaign_attribution import normalize_referral


MEDIA_EXTENSION_BY_MIME = {
    "audio/aac": "aac",
    "audio/amr": "amr",
    "audio/mp4": "m4a",
    "audio/mpeg": "mp3",
    "audio/ogg": "ogg",
    "audio/opus": "opus",
    "audio/webm": "webm",
    "image/jpeg": "jpg",
    "image/png": "png",
    "image/webp": "webp",
    "video/mp4": "mp4",
    "video/3gpp": "3gp",
    "application/pdf": "pdf",
}

DEFAULT_MEDIA_EXTENSION_BY_TYPE = {
    "audio": "ogg",
    "document": "bin",
    "image": "jpg",
    "sticker": "webp",
    "video": "mp4",
}

_UNSUPPORTED_INCOMING_MESSAGE_TYPES = frozenset({"unsupported", "unknown"})
_BSUID_IN_TEXT = re.compile(
    r"\b[A-Z]{2}\.(?:ENT\.)?[A-Za-z0-9]{1,128}\b"
)


def _normalize_unsupported_log_value(
    value: Any,
    *,
    max_length: int = 500,
) -> str | None:
    if value is None:
        return None

    normalized = " ".join(str(value).split())
    if not normalized:
        return None
    return normalized[:max_length]


def _log_unsupported_incoming_message(
    *,
    message: dict,
    whatsapp_account: Any,
) -> None:
    unsupported = message.get("unsupported")
    if not isinstance(unsupported, dict):
        unsupported = {}

    raw_errors = message.get("errors")
    if isinstance(raw_errors, dict):
        raw_errors = [raw_errors]
    elif not isinstance(raw_errors, list):
        raw_errors = []

    errors = []
    for raw_error in raw_errors:
        if not isinstance(raw_error, dict):
            continue

        error_data = raw_error.get("error_data")
        details = (
            error_data.get("details")
            if isinstance(error_data, dict)
            else raw_error.get("details")
        )
        errors.append({
            "code": _normalize_unsupported_log_value(
                raw_error.get("code"), max_length=50),
            "title": _normalize_unsupported_log_value(
                raw_error.get("title"), max_length=200),
            "details": _normalize_unsupported_log_value(details),
        })

    diagnostics = {
        "message_id": _normalize_unsupported_log_value(
            message.get("id"), max_length=200),
        "whatsapp_account": _normalize_unsupported_log_value(
            getattr(whatsapp_account, "name", None), max_length=200),
        "message_type": _normalize_unsupported_log_value(
            message.get("type"), max_length=50),
        "unsupported_type": _normalize_unsupported_log_value(
            unsupported.get("type"), max_length=100),
        "raw_type": _normalize_unsupported_log_value(
            unsupported.get("raw_type"), max_length=100),
        "errors": errors,
    }
    frappe.logger("frappe_whatsapp").warning(
        "Unsupported WhatsApp message skipped: "
        f"{json.dumps(diagnostics, ensure_ascii=True)}"
    )


def _normalize_contact_text(value: Any) -> str | None:
    if value is None:
        return None

    normalized = " ".join(str(value).split())
    return normalized or None


def _contact_display_name(contact: dict[str, Any]) -> str:
    name = contact.get("name")
    if not isinstance(name, dict):
        return "Unnamed contact"

    formatted_name = _normalize_contact_text(name.get("formatted_name"))
    if formatted_name:
        return formatted_name

    name_parts = [
        _normalize_contact_text(name.get(fieldname))
        for fieldname in (
            "prefix",
            "first_name",
            "middle_name",
            "last_name",
            "suffix",
        )
    ]
    return " ".join(part for part in name_parts if part) or "Unnamed contact"


def _append_contact_items(
    lines: list[str],
    *,
    items: Any,
    value_field: str,
    label: str,
    fallback_field: str | None = None,
) -> None:
    if not isinstance(items, list):
        return

    seen_values: set[str] = set()
    for item in items:
        if not isinstance(item, dict):
            continue

        value = _normalize_contact_text(item.get(value_field))
        if not value and fallback_field:
            value = _normalize_contact_text(item.get(fallback_field))
        if not value or value in seen_values:
            continue

        seen_values.add(value)
        item_type = _normalize_contact_text(item.get("type"))
        item_label = f"{label} ({item_type})" if item_type else label
        lines.append(f"{item_label}: {value}")


def _format_contact_address(address: dict[str, Any]) -> str | None:
    parts = [
        _normalize_contact_text(address.get(fieldname))
        for fieldname in ("street", "city", "state", "zip")
    ]
    country = (
        _normalize_contact_text(address.get("country"))
        or _normalize_contact_text(address.get("country_code"))
    )
    parts.append(country)
    return ", ".join(part for part in parts if part) or None


def _format_shared_contacts(contacts: Any) -> str:
    """Return a readable summary for a Meta ``contacts`` message."""
    if not isinstance(contacts, list):
        contacts = []

    valid_contacts = [
        contact for contact in contacts if isinstance(contact, dict)
    ]
    if not valid_contacts:
        return (
            "A contact was shared, but no readable contact details "
            "were provided."
        )

    heading = (
        "Shared contact"
        if len(valid_contacts) == 1
        else f"Shared contacts ({len(valid_contacts)})"
    )
    sections: list[str] = []

    for index, contact in enumerate(valid_contacts, start=1):
        lines: list[str] = []
        if len(valid_contacts) > 1:
            lines.append(f"Contact {index}")
        lines.append(f"Name: {_contact_display_name(contact)}")

        _append_contact_items(
            lines,
            items=contact.get("phones"),
            value_field="phone",
            fallback_field="wa_id",
            label="Phone",
        )
        _append_contact_items(
            lines,
            items=contact.get("emails"),
            value_field="email",
            label="Email",
        )

        organization = contact.get("org")
        if isinstance(organization, dict):
            organization_parts = [
                _normalize_contact_text(organization.get(fieldname))
                for fieldname in ("company", "department", "title")
            ]
            organization_text = " — ".join(
                part for part in organization_parts if part
            )
            if organization_text:
                lines.append(f"Organization: {organization_text}")

        addresses = contact.get("addresses")
        if isinstance(addresses, list):
            seen_addresses: set[str] = set()
            for address in addresses:
                if not isinstance(address, dict):
                    continue
                address_text = _format_contact_address(address)
                if not address_text or address_text in seen_addresses:
                    continue
                seen_addresses.add(address_text)
                address_type = _normalize_contact_text(address.get("type"))
                address_label = (
                    f"Address ({address_type})"
                    if address_type else "Address"
                )
                lines.append(f"{address_label}: {address_text}")

        _append_contact_items(
            lines,
            items=contact.get("urls"),
            value_field="url",
            label="URL",
        )

        birthday = _normalize_contact_text(contact.get("birthday"))
        if birthday:
            lines.append(f"Birthday: {birthday}")

        sections.append("\n".join(lines))

    return f"{heading}\n\n" + "\n\n".join(sections)


def normalize_media_mime_type(mime_type: str | None) -> str:
    """Return a lower-case MIME value without parameters."""
    if not mime_type:
        return "application/octet-stream"

    normalized = str(mime_type).split(";", 1)[0].strip().lower()
    return normalized or "application/octet-stream"


def get_media_file_extension(
        mime_type: str | None, message_type: str | None = None) -> str:
    normalized = normalize_media_mime_type(mime_type)
    if normalized in MEDIA_EXTENSION_BY_MIME:
        return MEDIA_EXTENSION_BY_MIME[normalized]

    if "/" in normalized and normalized != "application/octet-stream":
        suffix = normalized.split("/", 1)[1].split("+", 1)[0]
        if suffix:
            return suffix

    return DEFAULT_MEDIA_EXTENSION_BY_TYPE.get(message_type or "", "bin")


@frappe.whitelist(allow_guest=True)
def webhook():
    """Meta webhook."""
    if frappe.request.method == "GET":
        return get()
    return post()


def get():
    """Get."""
    hub_challenge = frappe.form_dict.get("hub.challenge")
    verify_token = frappe.form_dict.get("hub.verify_token")
    webhook_verify_token = frappe.db.get_value(
        'WhatsApp Account',
        {"webhook_verify_token": verify_token},
        'webhook_verify_token'
    )
    if not webhook_verify_token:
        frappe.throw("No matching WhatsApp account")

    if frappe.form_dict.get("hub.verify_token") != webhook_verify_token:
        frappe.throw("Verify token does not match")

    return Response(hub_challenge, status=200)


def post():
    """POST: read raw body + signature header, then delegate to handler.

    Keeping this function thin makes it straightforward to test the validation
    and enqueue logic via ``_handle_post_body`` without needing a live request.
    """
    raw_body: bytes = frappe.request.get_data()
    sig_header: str = frappe.request.headers.get("X-Hub-Signature-256", "")
    return _handle_post_body(raw_body, sig_header)


def _handle_post_body(raw_body: bytes, sig_header: str) -> Response:
    """Validate signature and enqueue processing for a single webhook POST.

    Validates ``X-Hub-Signature-256`` against the ``app_secret`` stored on
    every active WhatsApp Account.  Rejects with HTTP 403 before logging or
    enqueueing anything if validation fails.
    """
    if not _verify_webhook_signature(raw_body, sig_header):
        return Response("Forbidden", status=403)

    # Signature is valid — parse the JSON body (Meta always sends JSON).
    data: dict = {}
    try:
        if raw_body:
            data = json.loads(raw_body)
    except Exception:
        pass

    # Store raw payload for troubleshooting (non-fatal DB write).
    try:
        frappe.get_doc({
            "doctype": "WhatsApp Notification Log",
            "template": "Webhook",
            "meta_data": json.dumps(data)
        }).insert(ignore_permissions=True)
    except Exception:
        frappe.log_error(
            frappe.get_traceback(),
            "WhatsApp webhook log insert failed")

    frappe.enqueue(
        "frappe_whatsapp.utils.webhook.process_webhook_payload",
        queue="short",
        data=data,
        enqueue_after_commit=True,
        job_id=f"whatsapp_webhook_process::{frappe.generate_hash(length=10)}"
    )

    return Response("ok", status=200)


def _get_active_app_secrets() -> list[str]:
    """Return unique plaintext app secrets from all active WhatsApp Accounts.

    Uses ``_get_decrypted_password`` (module-level alias for
    ``frappe.utils.password.get_decrypted_password``) so the values are never
    stored in plain text.  Accounts with no ``app_secret`` configured are
    silently skipped.  The module-level import also makes it straightforward
    to patch in tests.
    """
    accounts = frappe.get_all(
        "WhatsApp Account",
        filters={"status": "Active"},
        fields=["name"],
    )

    seen: set[str] = set()
    secrets: list[str] = []
    for account in accounts:
        try:
            secret = _get_decrypted_password(
                "WhatsApp Account",
                str(account.name),
                "app_secret",
                raise_exception=False,
            )
            if secret and secret not in seen:
                seen.add(secret)
                secrets.append(secret)
        except Exception:
            pass

    return secrets


def _verify_webhook_signature(raw_body: bytes, sig_header: str) -> bool:
    """Return True when ``X-Hub-Signature-256`` matches a configured app
    secret.

    Meta signs every webhook POST with ``HMAC-SHA256(app_secret, raw_body)``
    and includes the result as ``X-Hub-Signature-256: sha256=<hex>``.
    We validate against every active account's ``app_secret`` so deployments
    with multiple Meta apps still work.

    Returns False (and logs a warning) when no app secrets are configured,
    prompting operators to add the secret to their WhatsApp Account record.
    """
    if not sig_header.startswith("sha256="):
        return False

    provided_hex = sig_header[len("sha256="):]
    if not provided_hex:
        return False

    app_secrets = _get_active_app_secrets()
    if not app_secrets:
        frappe.log_error(
            "No App Secret is configured on any active WhatsApp Account. "
            "Add the Meta App Secret (App Settings → Basic) to the "
            "WhatsApp Account form to enable webhook signature validation.",
            "WhatsApp webhook: no app secrets configured",
        )
        return False

    for secret in app_secrets:
        expected_hex = hmac.new(
            secret.encode("utf-8"),
            raw_body,
            hashlib.sha256,
        ).hexdigest()
        if hmac.compare_digest(expected_hex, provided_hex):
            return True

    return False


def process_webhook_payload(data: dict):
    """Process every entry/change in a Meta webhook payload."""
    # Defensive: data can be string sometimes
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except Exception:
            data = {}

    # Normalize entries to a list, supporting both payload shapes Meta sends:
    #   list-shaped:  data["entry"] = [{"id": "...", "changes": [...]}, ...]
    #   dict-shaped:  data["entry"] = {"id": "...", "changes": [...]}
    # All subsequent code uses `entries` so both shapes are handled uniformly.
    raw_entry = data.get("entry")
    if isinstance(raw_entry, list):
        entries = raw_entry
    elif isinstance(raw_entry, dict):
        entries = [raw_entry]
    else:
        entries = []

    for entry in entries:
        if not isinstance(entry, dict):
            continue
        raw_changes = entry.get("changes") or []
        changes = raw_changes if isinstance(raw_changes, list) else [raw_changes]
        for change in changes:
            if not isinstance(change, dict):
                continue
            if change.get("field") in _TEMPLATE_WEBHOOK_FIELDS:
                entry_waba_id = str(entry.get("id") or "")
                if _is_trusted_waba_id(entry_waba_id):
                    update_status(change)
                else:
                    frappe.log_error(
                        f"Untrusted template webhook WABA ID: {entry_waba_id}",
                        "WhatsApp untrusted template webhook",
                    )
                continue

            value = change.get("value")
            if not isinstance(value, dict):
                continue
            phone_id = (value.get("metadata") or {}).get("phone_number_id")
            whatsapp_account = (
                get_whatsapp_account(phone_id)
                if phone_id
                else get_whatsapp_account(account_type="incoming")
            )
            if not whatsapp_account:
                frappe.log_error(
                    f"No WhatsApp Account for phone_number_id {phone_id or '<missing>'}",
                    "WhatsApp webhook account resolution failed",
                )
                continue

            contacts = value.get("contacts") or []
            contacts = contacts if isinstance(contacts, list) else []
            messages = value.get("messages") or []
            messages = messages if isinstance(messages, list) else []
            for message in messages:
                if not isinstance(message, dict):
                    continue
                contact = _match_webhook_contact(message, contacts)
                _process_incoming_message(
                    message=message,
                    contact=contact,
                    whatsapp_account=whatsapp_account,
                )

            if change.get("field") == "user_id_update":
                _process_user_id_update(value, whatsapp_account)
            if value.get("statuses") or not messages:
                update_status(change, whatsapp_account=whatsapp_account)


def _match_webhook_contact(
    message: dict[str, Any], contacts: list[dict[str, Any]]
) -> dict[str, Any]:
    if len(contacts) == 1:
        return contacts[0] if isinstance(contacts[0], dict) else {}
    message_user_id = str(message.get("from_user_id") or "")
    message_phone = str(message.get("from") or "")
    for contact in contacts:
        if not isinstance(contact, dict):
            continue
        if message_user_id and str(contact.get("user_id") or "") == message_user_id:
            return contact
        if message_phone and str(contact.get("wa_id") or "") == message_phone:
            return contact
    return {}


def _process_user_id_update(value: dict[str, Any], whatsapp_account: Any) -> None:
    from frappe_whatsapp.utils.identity import apply_user_id_update, profile_identity
    from frappe_whatsapp.utils.routing import forward_identity_update_to_app_async

    raw_updates = value.get("user_id_update") or value.get("user_id_updates") or value
    updates = raw_updates if isinstance(raw_updates, list) else [raw_updates]
    contacts = value.get("contacts") or []
    contact = contacts[0] if isinstance(contacts, list) and len(contacts) == 1 else {}
    contact = contact if isinstance(contact, dict) else {}
    contact_profile = contact.get("profile")
    contact_profile = contact_profile if isinstance(contact_profile, dict) else {}
    for update in updates:
        if not isinstance(update, dict):
            continue
        user_id_change = update.get("user_id")
        parent_user_id_change = update.get("parent_user_id")
        normalized_update = dict(update)
        if isinstance(user_id_change, dict):
            normalized_update.update({
                "previous_user_id": user_id_change.get("previous"),
                "current_user_id": user_id_change.get("current"),
            })
        if isinstance(parent_user_id_change, dict):
            normalized_update.update({
                "previous_parent_user_id": parent_user_id_change.get("previous"),
                "current_parent_user_id": parent_user_id_change.get("current"),
            })
        normalized_update["wa_id"] = (
            normalized_update.get("wa_id") or contact.get("wa_id")
        )
        normalized_update["username"] = (
            normalized_update.get("username")
            or contact.get("username")
            or contact_profile.get("username")
        )
        if not any(normalized_update.get(key) for key in (
            "user_id",
            "current_user_id",
            "previous_user_id",
            "parent_user_id",
            "current_parent_user_id",
            "previous_parent_user_id",
        )):
            continue
        profile = apply_user_id_update(
            whatsapp_account=str(whatsapp_account.name), update=normalized_update
        )
        forward_identity_update_to_app_async(
            whatsapp_account=str(whatsapp_account.name),
            profile_name=str(profile.name),
            previous_user_id=(
                str(normalized_update.get("previous_user_id") or "") or None
            ),
            previous_parent_user_id=(
                str(
                    normalized_update.get("previous_parent_user_id") or ""
                ) or None
            ),
            identity=profile_identity(profile),
            source_event_id=(
                str(
                    normalized_update.get("_source_event_id")
                    or normalized_update.get("timestamp")
                    or ""
                ) or None
            ),
        )


def _enqueue_language_detection(
        *, contact_number: str, whatsapp_account: str,
        text: str, message_doc_name: str,
        profile_name: str | None) -> None:
    """Enqueue language detection as a best-effort background job.

    Runs in the *short* queue after the current transaction commits so it
    never delays inbound message forwarding or consent processing.
    All failure handling lives inside ``update_profile_language`` itself.
    """
    frappe.enqueue(
        "frappe_whatsapp.utils.language_detection.update_profile_language",
        queue="short",
        enqueue_after_commit=True,
        contact_number=contact_number,
        whatsapp_account=whatsapp_account,
        text=text,
        message_doc_name=message_doc_name,
        profile_name=profile_name,
    )


def _process_incoming_message(
        *, message: dict, whatsapp_account, contact: dict | None = None,
        sender_profile_name: str | None = None):
    from frappe_whatsapp.utils.identity import (
        identity_from_webhook,
        profile_identity,
        resolve_identity,
    )

    message_type = message.get("type")
    if (
        isinstance(message_type, str)
        and message_type in _UNSUPPORTED_INCOMING_MESSAGE_TYPES
    ):
        _log_unsupported_incoming_message(
            message=message,
            whatsapp_account=whatsapp_account,
        )
        return
    if message_type == "system":
        system = message.get("system") or {}
        if str(system.get("type") or "") == "user_changed_user_id":
            previous_user_id = message.get("from_user_id")
            if not previous_user_id:
                current_user_id = str(system.get("user_id") or "")
                previous_user_id = next((
                    candidate
                    for candidate in _BSUID_IN_TEXT.findall(
                        str(system.get("body") or "")
                    )
                    if ".ENT." not in candidate
                    and candidate != current_user_id
                ), None)
            _process_user_id_update(
                {
                    **system,
                    "previous_wa_id": message.get("from"),
                    "previous_user_id": previous_user_id,
                    "previous_parent_user_id": message.get(
                        "from_parent_user_id"
                    ),
                    "_source_event_id": message.get("id"),
                },
                whatsapp_account,
            )
        else:
            _log_unsupported_incoming_message(
                message=message,
                whatsapp_account=whatsapp_account,
            )
        return

    msg_id = message.get("id")
    if msg_id and frappe.db.exists("WhatsApp Message", {"message_id": msg_id}):
        return

    if contact is None and sender_profile_name:
        contact = {"profile": {"name": sender_profile_name}}
    identity = identity_from_webhook(message, contact)
    profile = resolve_identity(
        whatsapp_account=str(whatsapp_account.name), identity=identity
    )
    normalized_identity = profile_identity(profile)
    contact_number = normalized_identity.get("phone")
    sender_profile_name = sender_profile_name or (
        identity.get("profile_name") or identity.get("username") or None
    )

    if is_contact_blocked(
            whatsapp_account=str(whatsapp_account.name),
            contact_number=contact_number,
            contact_profile=str(profile.name),
            user_id=normalized_identity.get("user_id")):
        return

    referral_fields = normalize_referral(message.get("referral"))

    routed_app = resolve_incoming_routed_app(
        whatsapp_account=str(whatsapp_account.name),
        contact_number=str(contact_number or ""),
        contact_profile=str(profile.name),
    )

    context = message.get("context")
    context_id = context.get("id") if isinstance(context, dict) else None

    is_reply = (
        isinstance(context, dict)
        and "id" in context
        and "forwarded" not in context
    )

    if not context_id:
        is_reply = False

    reply_to_message_id = str(context_id) if is_reply else None

    # ✅ Idempotency guard: don't insert duplicates
    identity_fields = {
        "from": contact_number,
        "contact_profile": profile.name,
        "from_user_id": normalized_identity.get("user_id"),
        "from_parent_user_id": normalized_identity.get("parent_user_id"),
        "username": normalized_identity.get("username"),
    }

    if message_type == "text":
        body_text = (message.get("text") or {}).get("body", "")

        doc = frappe.get_doc({
            "doctype": "WhatsApp Message",
            "type": "Incoming",
            **identity_fields,
            "message": body_text,
            "message_id": msg_id,
            "reply_to_message_id": reply_to_message_id,
            "is_reply": is_reply,
            "content_type": "text",
            "profile_name": sender_profile_name,
            "whatsapp_account": whatsapp_account.name,
            "routed_app": routed_app,
            **referral_fields,
        }).insert(ignore_permissions=True)

        # Check for opt-out / opt-in keywords
        _handle_consent_keywords(
            body_text=body_text,
            contact_number=str(contact_number or ""),
            contact_profile=str(profile.name),
            whatsapp_account_name=str(whatsapp_account.name),
            message_doc_name=str(doc.name),
            profile_name=sender_profile_name,
        )

        if contact_number:
            _enqueue_language_detection(
            contact_number=contact_number,
            whatsapp_account=str(whatsapp_account.name),
            text=body_text,
            message_doc_name=str(doc.name),
            profile_name=sender_profile_name,
        )

        forward_incoming_to_app_async(incoming_message_name=str(doc.name))

    elif message_type == "interactive":
        _handle_interactive(
            message=message,
            whatsapp_account=whatsapp_account,
            sender_profile_name=sender_profile_name,
            routed_app=routed_app,
            reply_to_message_id=reply_to_message_id,
            is_reply=is_reply,
            identity_fields=identity_fields,
        )

    elif message_type in ["image", "audio", "video", "document", "sticker"]:
        # Insert a stub message quickly, then download media async
        media_payload = message.get(message_type) or {}
        caption_text = media_payload.get("caption", "")
        msg_doc = frappe.get_doc({
            "doctype": "WhatsApp Message",
            "type": "Incoming",
            **identity_fields,
            "message_id": msg_id,
            "reply_to_message_id": reply_to_message_id,
            "is_reply": is_reply,
            "message": caption_text,
            "content_type": message_type,
            "profile_name": sender_profile_name,
            "whatsapp_account": whatsapp_account.name,
            "routed_app": routed_app,
            **referral_fields,
        })
        if (
            message_type == "audio"
            and msg_doc.meta.has_field("is_voice_note")
        ):
            msg_doc.set(
                "is_voice_note",
                1 if media_payload.get("voice") else 0,
            )

        msg_doc.insert(ignore_permissions=True)

        # Check for opt-out / opt-in keywords in caption (if any)
        _handle_consent_keywords(
            body_text=caption_text or "",
            contact_number=str(contact_number or ""),
            contact_profile=str(profile.name),
            whatsapp_account_name=str(whatsapp_account.name),
            message_doc_name=str(msg_doc.name),
            profile_name=sender_profile_name,
        )

        if caption_text and contact_number:
            _enqueue_language_detection(
                contact_number=contact_number,
                whatsapp_account=str(whatsapp_account.name),
                text=caption_text,
                message_doc_name=str(msg_doc.name),
                profile_name=sender_profile_name,
            )

        media_id = media_payload.get("id")
        if media_id:
            frappe.enqueue(
                "frappe_whatsapp.utils.webhook.download_and_attach_media",
                queue="long",
                whatsapp_account_name=whatsapp_account.name,
                message_docname=msg_doc.name,
                media_id=media_id,
                message_type=message_type,
                enqueue_after_commit=True
            )

    elif message_type == "contacts":
        shared_contacts = message.get("contacts") or []
        shared_contacts = (
            shared_contacts if isinstance(shared_contacts, list) else []
        )
        origins = [
            str(item.get("origin"))
            for item in shared_contacts
            if isinstance(item, dict) and item.get("origin")
        ]
        contact_origin = str(message.get("origin") or "") or (
            origins[0] if origins else None
        )
        body_text = _format_shared_contacts(shared_contacts)
        doc = frappe.get_doc({
            "doctype": "WhatsApp Message",
            "type": "Incoming",
            **identity_fields,
            "message_id": msg_id,
            "reply_to_message_id": reply_to_message_id,
            "is_reply": is_reply,
            "message": body_text,
            "content_type": "contact",
            "contact_payload": {"contacts": shared_contacts},
            "contact_origin": contact_origin,
            "profile_name": sender_profile_name,
            "whatsapp_account": whatsapp_account.name,
            "routed_app": routed_app,
            **referral_fields,
        }).insert(ignore_permissions=True)

        if contact_origin == "contact_request":
            _link_requested_contact_phone(
                message=message,
                identity=identity,
                whatsapp_account=str(whatsapp_account.name),
            )

        # Contact-card data is not sender-authored conversational text. Do not
        # use it for consent keyword matching or profile language detection.
        forward_incoming_to_app_async(incoming_message_name=str(doc.name))

    else:
        raw_body = message.get(message_type)
        body_text = ""
        if isinstance(raw_body, dict):
            body_text = str(raw_body.get("text") or raw_body.get("body") or "")
        elif isinstance(raw_body, str):
            body_text = raw_body

        doc = frappe.get_doc({
            "doctype": "WhatsApp Message",
            "type": "Incoming",
            **identity_fields,
            "message_id": msg_id,
            "reply_to_message_id": reply_to_message_id,
            "is_reply": is_reply,
            "message": body_text,
            "content_type": message_type or "unknown",
            "profile_name": sender_profile_name,
            "whatsapp_account": whatsapp_account.name,
            "routed_app": routed_app,
            **referral_fields,
        }).insert(ignore_permissions=True)

        # Check for opt-out / opt-in keywords if message contains text-like
        # body
        _handle_consent_keywords(
            body_text=body_text or "",
            contact_number=str(contact_number or ""),
            contact_profile=str(profile.name),
            whatsapp_account_name=str(whatsapp_account.name),
            message_doc_name=str(doc.name),
            profile_name=sender_profile_name,
        )

        if body_text and contact_number:
            _enqueue_language_detection(
                contact_number=contact_number,
                whatsapp_account=str(whatsapp_account.name),
                text=body_text,
                message_doc_name=str(doc.name),
                profile_name=sender_profile_name,
            )

        forward_incoming_to_app_async(incoming_message_name=str(doc.name))


def _handle_consent_keywords(
        *, body_text: str, contact_number: str,
        whatsapp_account_name: str, message_doc_name: str,
        profile_name: str | None,
        contact_profile: str | None = None) -> None:
    """Detect opt-out or opt-in keywords and update profile consent.

    Text that matches neither an opt-out nor an opt-in keyword is silently
    ignored — no consent change is made.  In particular, a ``NO`` quick-reply
    to a consent-request template falls through here without any action:
    declining a consent invitation is not the same as opting out, and the
    contact's status intentionally stays Unknown.  To implement explicit
    NO-handling (e.g. sending a "you will not receive further messages" reply),
    add it here after the opt-in check.
    """
    # Check opt-out first (takes priority over opt-in)
    keyword_match = check_opt_out_keyword(
        body_text, whatsapp_account=whatsapp_account_name)

    if keyword_match:
        process_opt_out(
            contact_number=contact_number,
            whatsapp_account=whatsapp_account_name,
            message_doc_name=message_doc_name,
            keyword_match=keyword_match,
            profile_name=profile_name,
            contact_profile=contact_profile,
        )
        if contact_number:
            send_opt_out_confirmation(
                contact_number=contact_number,
                whatsapp_account_name=whatsapp_account_name,
            )
        return

    # Check opt-in
    if check_opt_in_keyword(body_text):
        process_opt_in(
            contact_number=contact_number,
            whatsapp_account=whatsapp_account_name,
            message_doc_name=message_doc_name,
            profile_name=profile_name,
            contact_profile=contact_profile,
        )
        if contact_number:
            send_opt_in_confirmation(
                contact_number=contact_number,
                whatsapp_account_name=whatsapp_account_name,
            )


def _link_requested_contact_phone(
    *, message: dict[str, Any], identity: dict[str, Any], whatsapp_account: str
) -> None:
    """Link a phone voluntarily returned by REQUEST_CONTACT_INFO."""
    shared_contacts = message.get("contacts") or []
    if not isinstance(shared_contacts, list):
        return
    phones: list[str] = []
    for shared_contact in shared_contacts:
        if not isinstance(shared_contact, dict):
            continue
        for phone in shared_contact.get("phones") or []:
            if isinstance(phone, dict):
                value = phone.get("phone") or phone.get("wa_id")
                if value:
                    phones.append(str(value))
    if not phones:
        return
    from frappe_whatsapp.utils.identity import resolve_identity

    resolve_identity(
        whatsapp_account=whatsapp_account,
        identity={**identity, "phone": phones[0]},
        explicit_link=True,
    )


def _handle_interactive(
        *, message, whatsapp_account, sender_profile_name,
        routed_app, reply_to_message_id, is_reply, identity_fields):
    interactive = message.get("interactive") or {}
    interactive_type = interactive.get("type")
    referral_fields = normalize_referral(message.get("referral"))

    if interactive_type == "call_permission_reply":
        permission_reply = interactive.get("call_permission_reply") or {}
        response = str(permission_reply.get("response") or "").lower()
        summary_message = (
            "Call permission accepted"
            if response == "accept"
            else "Call permission rejected"
        )
        doc = frappe.get_doc({
            "doctype": "WhatsApp Message",
            "type": "Incoming",
            **identity_fields,
            "message": summary_message,
            "message_id": message.get("id"),
            "reply_to_message_id": reply_to_message_id,
            "is_reply": is_reply,
            "content_type": "button",
            "profile_name": sender_profile_name,
            "whatsapp_account": whatsapp_account.name,
            "routed_app": routed_app,
            **referral_fields,
        }).insert(ignore_permissions=True)

        from frappe_whatsapp.utils.calling import handle_call_permission_reply
        handle_call_permission_reply(
            contact_number=str(message.get("from") or ""),
            recipient=str(identity_fields.get("from_user_id") or "") or None,
            contact_profile=str(identity_fields.get("contact_profile") or "") or None,
            whatsapp_account_name=str(whatsapp_account.name),
            response=response,
            is_permanent=cint(permission_reply.get("is_permanent") or 0) == 1,
            expiration_timestamp=permission_reply.get(
                "expiration_timestamp"),
            response_source=permission_reply.get("response_source"),
            context_message_id=reply_to_message_id,
            message_doc_name=str(doc.name),
        )
        return

    # button/list
    if interactive_type in ("button_reply", "list_reply"):
        payload = interactive.get(interactive_type) or {}
        payload_text = (
            str(payload.get("title") or payload.get("id") or "")
        )
        doc = frappe.get_doc({
            "doctype": "WhatsApp Message",
            "type": "Incoming",
            **identity_fields,
            "message": payload.get("id"),
            "message_id": message.get("id"),
            "reply_to_message_id": reply_to_message_id,
            "is_reply": is_reply,
            "content_type": "button",
            "profile_name": sender_profile_name,
            "whatsapp_account": whatsapp_account.name,
            "routed_app": routed_app,
            **referral_fields,
        }).insert(ignore_permissions=True)

        # Check for opt-out / opt-in keywords based on reply text/id
        _handle_consent_keywords(
            body_text=payload_text,
            contact_number=str(identity_fields.get("from") or ""),
            contact_profile=str(
                identity_fields.get("contact_profile") or ""
            ) or None,
            whatsapp_account_name=str(whatsapp_account.name),
            message_doc_name=str(doc.name),
            profile_name=sender_profile_name,
        )

        # Detect language from the human-readable title only (not payload id)
        button_title = str(payload.get("title") or "")
        if button_title and identity_fields.get("from"):
            _enqueue_language_detection(
                contact_number=str(identity_fields.get("from") or ""),
                whatsapp_account=str(whatsapp_account.name),
                text=button_title,
                message_doc_name=str(doc.name),
                profile_name=sender_profile_name,
            )

        forward_incoming_to_app_async(incoming_message_name=str(doc.name))

    # flows
    elif interactive_type == "nfm_reply":
        nfm_reply = interactive.get("nfm_reply") or {}
        response_json_str = nfm_reply.get("response_json", "{}")

        try:
            flow_response = json.loads(response_json_str)
        except json.JSONDecodeError:
            flow_response = {}

        summary_parts = [f"{k}: {v}" for k, v in flow_response.items() if v]
        summary_message = ", ".join(
            summary_parts) if summary_parts else "Flow completed"

        doc = frappe.get_doc({
            "doctype": "WhatsApp Message",
            "type": "Incoming",
            **identity_fields,
            "message": summary_message,
            "message_id": message.get("id"),
            "reply_to_message_id": reply_to_message_id,
            "is_reply": is_reply,
            "content_type": "flow",
            "flow_response": json.dumps(flow_response),
            "profile_name": sender_profile_name,
            "whatsapp_account": whatsapp_account.name,
            "routed_app": routed_app,
            **referral_fields,
        }).insert(ignore_permissions=True)

        # publish realtime async too (optional)
        frappe.enqueue(
            "frappe_whatsapp.utils.webhook.publish_flow_realtime",
            queue="short",
            phone=message.get("from"),
            message_id=message.get("id"),
            flow_response=flow_response,
            whatsapp_account=whatsapp_account.name,
            enqueue_after_commit=True
        )

        forward_incoming_to_app_async(incoming_message_name=str(doc.name))


# Template-related webhook fields that should trigger a full sync from Meta.
#
# Meta emits these field values on the `changes` object:
#   message_template_status_update — APPROVED/REJECTED/PENDING after edits or
#       review-cycle changes. This is also the signal for content edits: when
#       an operator edits a template in Business Manager, Meta puts it into
#       PENDING review and sends this event (there is no separate
#       "content_changed" field).
#   message_template_quality_update — quality-score changes (HIGH/MEDIUM/LOW).
#   template_category_update — Meta reclassifies a template's category
#       (e.g. UTILITY → MARKETING); the template record needs re-syncing.
#
# No additional template-change field is documented by Meta at this time.
_TEMPLATE_WEBHOOK_FIELDS = frozenset({
    "message_template_status_update",
    "message_template_quality_update",
    "template_category_update",
})


def _is_trusted_waba_id(waba_id: str) -> bool:
    """Return True when the given WABA ID maps to a configured local account.

    The caller must pass the ID of the *specific* entry whose change is being
    processed (not any other entry in the payload), so that a trusted later
    entry cannot authorize an untrusted earlier one.

    Meta template webhook payloads carry the WhatsApp Business Account ID
    (WABA ID) in ``entry[].id``.  We verify it against the ``business_id``
    field of configured WhatsApp Accounts before allowing a template sync.

    **Preferred approach (not yet implemented):** validate Meta's
    ``X-Hub-Signature-256`` request header using an App Secret stored per
    account.  That requires adding an ``app_secret`` Password field to the
    WhatsApp Account doctype.  Until that field exists, this WABA-ownership
    check is the current defensive fallback.
    """
    if not waba_id:
        return False
    return bool(frappe.db.exists("WhatsApp Account", {"business_id": waba_id}))


def _sync_templates_from_webhook() -> None:
    """Internal worker for template events accepted by the trusted webhook."""
    from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_templates.whatsapp_templates import fetch  # noqa: E501

    previous_user = cast(str, frappe.session.user)
    try:
        frappe.set_user("Administrator")
        fetch()
    finally:
        frappe.set_user(previous_user)


def _enqueue_template_sync() -> None:
    """Enqueue a background template sync from Meta.

    Uses a stable job_id combined with deduplicate=True so a burst of
    template webhook events results in at most one queued sync job.
    Frappe passes deduplicate to RQ which skips enqueueing when a job
    with the same id is already pending.
    """
    frappe.enqueue(
        "frappe_whatsapp.utils.webhook._sync_templates_from_webhook",
        queue="long",
        job_id="whatsapp_template_sync",
        deduplicate=True,
        enqueue_after_commit=True,
    )


def update_status(data, whatsapp_account=None):
    """Update status hook."""
    value = data.get("value")
    if not isinstance(value, dict):
        return

    field = data.get("field")

    if field == "message_template_status_update":
        update_template_status(value)

    elif field == "messages":
        update_message_status(value, whatsapp_account=whatsapp_account)

    if field in _TEMPLATE_WEBHOOK_FIELDS:
        _enqueue_template_sync()


def update_template_status(data):
    """Update template status."""
    if not data.get("event") or not data.get("message_template_id"):
        return
    frappe.db.sql(
        """UPDATE `tabWhatsApp Templates`
        SET status = %(event)s
        WHERE id = %(message_template_id)s""",
        data
    )


def _extract_status_error_fields(
        status_payload: dict[str, Any]) -> dict[str, Any]:
    """Map Meta status errors to WhatsApp Message fields."""
    error_fields: dict[str, Any] = {
        "status_error_code": None,
        "status_error_title": None,
        "status_error_message": None,
        "status_error_details": None,
        "status_error_href": None,
        "status_error_payload": None,
    }

    errors = status_payload.get("errors")
    if not isinstance(errors, list) or not errors:
        # Some failed callbacks omit `errors`; keep the raw status payload
        # so operators still have context for troubleshooting.
        if str(status_payload.get("status") or "").lower() == "failed":
            error_fields["status_error_payload"] = {
                "status_payload": status_payload}
        return error_fields

    # Frappe JSON fields reject raw Python lists during document validation.
    # Preserve the full Meta payload by wrapping the array in an object.
    error_fields["status_error_payload"] = {"errors": errors}
    first_error = next((err for err in errors if isinstance(err, dict)), None)
    if not first_error:
        return error_fields

    code = first_error.get("code")
    if code is not None:
        error_fields["status_error_code"] = str(code)

    for source_key, target_key in (
            ("title", "status_error_title"),
            ("message", "status_error_message"),
            ("href", "status_error_href")):
        value = first_error.get(source_key)
        if value is not None:
            error_fields[target_key] = str(value)

    error_data = first_error.get("error_data")
    if isinstance(error_data, dict):
        details = error_data.get("details")
        if details is not None:
            error_fields["status_error_details"] = str(details)

    return error_fields


def update_message_status(data, whatsapp_account=None):
    """Update message status."""
    statuses = data.get("statuses")
    if not statuses or not isinstance(statuses, list):
        return

    from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_message.whatsapp_message import WhatsAppMessage  # noqa
    from frappe_whatsapp.utils.identity import profile_identity, resolve_identity

    contacts = data.get("contacts") or []
    contacts = contacts if isinstance(contacts, list) else []

    for status_payload in statuses:
        if not isinstance(status_payload, dict):
            continue

        msg_id = status_payload.get("id")
        status = status_payload.get("status")
        if not msg_id or not status:
            continue

        conversation = (status_payload.get("conversation") or {}).get("id")
        name = frappe.db.get_value(
            "WhatsApp Message", filters={"message_id": msg_id})
        if not name:
            continue

        doc = cast(
            WhatsAppMessage,
            frappe.get_doc("WhatsApp Message", str(name)))
        account_name = str(
            getattr(whatsapp_account, "name", None)
            or doc.whatsapp_account
            or ""
        )
        status_contact = {}
        for candidate in contacts:
            if not isinstance(candidate, dict):
                continue
            if (
                status_payload.get("recipient_user_id")
                and candidate.get("user_id")
                == status_payload.get("recipient_user_id")
            ) or (
                status_payload.get("recipient_id")
                and candidate.get("wa_id")
                == status_payload.get("recipient_id")
            ):
                status_contact = candidate
                break
        if not status_contact and len(contacts) == 1:
            status_contact = contacts[0]
        status_contact = status_contact if isinstance(status_contact, dict) else {}
        status_profile = status_contact.get("profile")
        status_profile = status_profile if isinstance(status_profile, dict) else {}
        raw_phone = status_contact.get("wa_id") or status_payload.get("recipient_id")
        raw_user_id = (
            status_payload.get("recipient_user_id")
            or status_contact.get("user_id")
        )
        identity = {
            "phone": raw_phone if str(raw_phone or "").isdigit() else None,
            "user_id": raw_user_id,
            "parent_user_id": (
                status_payload.get("recipient_parent_user_id")
                or status_contact.get("parent_user_id")
            ),
            "username": (
                status_contact.get("username")
                or status_profile.get("username")
            ),
            "profile_name": status_profile.get("name"),
        }
        if account_name and any(
            identity.get(key)
            for key in ("phone", "user_id", "parent_user_id")
        ):
            profile = resolve_identity(
                whatsapp_account=account_name,
                identity=identity,
                explicit_link=True,
            )
            resolved = profile_identity(profile)
            doc.contact_profile = profile.name
            doc.recipient_user_id = raw_user_id or resolved.get("user_id")
            doc.recipient_parent_user_id = (
                identity.get("parent_user_id")
                or resolved.get("parent_user_id")
            )
            if not doc.to and resolved.get("phone"):
                doc.to = resolved["phone"]
        if doc.meta.has_field("status_recipient_phone"):
            doc.status_recipient_phone = (
                str(raw_phone) if str(raw_phone or "").isdigit() else None
            )
        if doc.meta.has_field("status_contacts"):
            doc.status_contacts = json.dumps({"contacts": contacts})
        doc.status = status
        if conversation:
            doc.conversation_id = conversation

        error_fields = _extract_status_error_fields(status_payload)
        for fieldname, value in error_fields.items():
            if doc.meta.has_field(fieldname):
                doc.set(fieldname, value)

        doc.save(ignore_permissions=True)
        frappe.db.commit()


def download_and_attach_media(
        whatsapp_account_name: str,
        message_docname: str, media_id: str, message_type: str):
    try:
        from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_account.whatsapp_account import WhatsAppAccount  # noqa
        whatsapp_account = cast(
            WhatsAppAccount,
            frappe.get_doc("WhatsApp Account", whatsapp_account_name))

        contact_number = frappe.db.get_value(
            "WhatsApp Message",
            message_docname,
            "from",
        )
        if is_contact_blocked(
                whatsapp_account=whatsapp_account_name,
                contact_number=str(contact_number or "")):
            return

        token = whatsapp_account.get_password("token")
        base_url = f"{whatsapp_account.url}/{whatsapp_account.version}/"

        headers = {"Authorization": f"Bearer {token}"}

        # 1) Get media metadata to retrieve url/mime
        r = requests.get(f"{base_url}{media_id}/", headers=headers, timeout=30)
        r.raise_for_status()
        media_data = r.json()

        media_url = media_data.get("url")
        mime_type = normalize_media_mime_type(media_data.get("mime_type"))
        file_extension = get_media_file_extension(
            mime_type, message_type=message_type)

        # 2) Download content
        r2 = requests.get(media_url, headers=headers, timeout=60)
        r2.raise_for_status()

        file_data = r2.content
        file_name = (
            f"whatsapp-{message_type}-"
            f"{frappe.generate_hash(length=10)}.{file_extension}"
        )

        # 3) Attach to WhatsApp Message
        from frappe.core.doctype.file.file import File
        file_doc = cast(File, frappe.get_doc({
            "doctype": "File",
            "file_name": file_name,
            "attached_to_doctype": "WhatsApp Message",
            "attached_to_name": message_docname,
            "attached_to_field": "attach",
            "content": file_data,
            "file_type": file_extension.upper(),
        }))
        file_doc.save(ignore_permissions=True)

        frappe.db.set_value(
            "WhatsApp Message",
            message_docname,
            "attach",
            file_doc.file_url)
        forward_incoming_to_app_async(incoming_message_name=message_docname)
    except Exception:
        frappe.db.rollback()
        frappe.log_error(
            frappe.get_traceback(),
            ("WhatsApp media download failed for "
             f"{message_type} {message_docname}")
        )


def publish_flow_realtime(
        phone: str, message_id: str, flow_response: dict,
        whatsapp_account: str):
    """Publish a realtime event when a WhatsApp Flow response is received.

    This allows the frontend to react to incoming flow responses.
    """
    frappe.publish_realtime(
        event="whatsapp_flow_response",
        message={
            "phone": phone,
            "message_id": message_id,
            "flow_response": flow_response,
            "whatsapp_account": whatsapp_account,
        },
        doctype="WhatsApp Message",
        after_commit=True
    )
