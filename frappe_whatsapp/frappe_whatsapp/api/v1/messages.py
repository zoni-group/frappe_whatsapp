from __future__ import annotations

import hashlib
import json
from typing import Any

import frappe
from frappe import _
from frappe.utils.file_lock import LockTimeoutError
from frappe.utils.synchronization import filelock


def _client_app(source_app: str | None = None):
    filters: dict[str, Any] = {"enabled": 1, "api_user": frappe.session.user}
    if source_app:
        filters["name"] = source_app
    rows = frappe.get_all(
        "WhatsApp Client App", filters=filters, fields=["name"], limit=2
    )
    if len(rows) != 1:
        frappe.throw(
            _("The authenticated user must be bound to exactly one enabled WhatsApp Client App."),
            frappe.PermissionError,
        )
    return frappe.get_doc("WhatsApp Client App", rows[0].name)


def _require_allowed_account(app: Any, whatsapp_account: str | None) -> str:
    account = str(whatsapp_account or app.outbound_default_account or "")
    if not account:
        frappe.throw(_("WhatsApp Account is required."))
    allowed = {
        str(row.whatsapp_account)
        for row in (app.get("allowed_accounts") or [])
        if row.whatsapp_account
    }
    if account not in allowed:
        frappe.throw(
            _("WhatsApp Account is not allowed for this client app."),
            frappe.PermissionError,
        )
    return account


def _result(doc: Any, *, replay: bool = False) -> dict[str, Any]:
    result = {
        "ok": str(doc.status or "").lower() not in {"failed", "error"},
        "message_name": doc.name,
        "provider_message_id": doc.message_id,
        "external_reference": doc.external_reference,
        "status": doc.status,
        "recipient_type": "phone" if doc.to else (
            "parent_user_id" if ".ENT." in str(doc.recipient or "") else "user_id"
        ),
        "recipient": doc.to or doc.recipient,
        "idempotent_replay": bool(replay),
    }
    return result


def _idempotency_lock_name(idempotency_key: str) -> str:
    return f"whatsapp-client-send-{idempotency_key[:24]}"


def _existing_message(idempotency_key: str) -> str | None:
    name = frappe.db.get_value(
        "WhatsApp Message",
        {"client_idempotency_key": idempotency_key},
        "name",
    )
    return str(name) if name else None


@frappe.whitelist()
def send(
    *,
    external_reference: str,
    content_type: str,
    to: str | None = None,
    recipient: str | None = None,
    whatsapp_account: str | None = None,
    source_app: str | None = None,
    message: str | None = None,
    attachment_url: str | None = None,
    template: str | None = None,
    body_param: dict[str, Any] | str | None = None,
    buttons: list[dict[str, Any]] | str | None = None,
) -> dict[str, Any]:
    if not external_reference or len(external_reference) > 191:
        frappe.throw(_("A valid external_reference is required."))
    if not (to or recipient):
        frappe.throw(_("Provide to or recipient."))
    app = _client_app(source_app)
    account = _require_allowed_account(app, whatsapp_account)
    idempotency_key = hashlib.sha256(
        f"{app.name}\0{account}\0{external_reference}".encode("utf-8")
    ).hexdigest()
    existing = _existing_message(idempotency_key)
    if existing:
        return _result(
            frappe.get_doc("WhatsApp Message", existing), replay=True
        )

    allowed_types = {
        "text", "document", "image", "video", "audio", "sticker",
        "interactive", "contact_request", "template",
    }
    if content_type not in allowed_types:
        frappe.throw(_("Unsupported content_type."))
    values: dict[str, Any] = {
        "doctype": "WhatsApp Message",
        "type": "Outgoing",
        "to": to,
        "recipient": recipient,
        "whatsapp_account": account,
        "source_app": app.name,
        "external_reference": external_reference,
        "client_idempotency_key": idempotency_key,
        "content_type": "text" if content_type == "template" else content_type,
        "message": message,
        "attach": attachment_url,
        "use_template": 1 if content_type == "template" else 0,
        "template": template,
        "body_param": (
            body_param if isinstance(body_param, str)
            else json.dumps(body_param) if body_param is not None else None
        ),
        "buttons": (
            buttons if isinstance(buttons, str)
            else json.dumps(buttons) if buttons is not None else None
        ),
    }
    lock_context = filelock(
        _idempotency_lock_name(idempotency_key), timeout=30
    )
    release_after_transaction = False
    try:
        lock_context.__enter__()
        try:
            # Another worker may have completed while this request waited for
            # the lock. Recheck before the WhatsApp Message insert performs
            # the external Meta send in its before_insert hook.
            existing = _existing_message(idempotency_key)
            if existing:
                return _result(
                    frappe.get_doc("WhatsApp Message", existing), replay=True
                )
            try:
                doc = frappe.get_doc(values).insert(ignore_permissions=True)
            except frappe.UniqueValidationError:
                existing = _existing_message(idempotency_key)
                if not existing:
                    raise
                return _result(
                    frappe.get_doc("WhatsApp Message", existing), replay=True
                )

            def release_lock() -> None:
                lock_context.__exit__(None, None, None)

            # Keep the cross-process lock until the idempotency row is
            # visible to other DB connections.
            frappe.db.after_commit.add(release_lock)
            frappe.db.after_rollback.add(release_lock)
            release_after_transaction = True
            return _result(doc)
        finally:
            if not release_after_transaction:
                lock_context.__exit__(None, None, None)
    except LockTimeoutError:
        frappe.throw(
            _(
                "Another request with this external_reference is still "
                "being processed. Retry shortly."
            )
        )
    raise RuntimeError("Unreachable")


@frappe.whitelist()
def request_contact_info(
    *,
    external_reference: str,
    to: str | None = None,
    recipient: str | None = None,
    whatsapp_account: str | None = None,
    source_app: str | None = None,
    message: str | None = None,
) -> dict[str, Any]:
    return send(
        external_reference=external_reference,
        content_type="contact_request",
        to=to,
        recipient=recipient,
        whatsapp_account=whatsapp_account,
        source_app=source_app,
        message=message or _("Please share your contact information."),
    )
