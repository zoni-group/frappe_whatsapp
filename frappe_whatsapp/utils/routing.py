from __future__ import annotations

import json
import mimetypes
from posixpath import basename
from typing import TYPE_CHECKING, Any, TypedDict, cast
from urllib.parse import urlencode, urlparse

import frappe
from frappe.core.doctype.document_share_key.document_share_key import (
    is_expired,
)
from frappe.integrations.utils import make_post_request
from frappe.utils import get_url, now_datetime
from frappe_whatsapp.utils import format_number

if TYPE_CHECKING:
    from ..frappe_whatsapp.doctype.whatsapp_message.whatsapp_message import (
        WhatsAppMessage,
    )


ROUTE_DOCTYPE = "WhatsApp Conversation Route"
FORWARDED_INCOMING_CACHE_PREFIX = "frappe_whatsapp:incoming_forwarded:"
PRIVATE_FILE_PREFIX = "/private/files/"


class AttachmentFileData(TypedDict):
    name: str
    file_name: str | None
    file_type: str | None
    file_url: str | None
    is_private: bool | int


def _get_route_doc_name(*, whatsapp_account: str, contact_number: str) -> str:
    return f"{contact_number}-{whatsapp_account}"


def _upsert_conversation_route(
    *,
    whatsapp_account: str,
    contact_number: str,
    source_app: str,
    last_outgoing_message: str | None = None,
    update_last_outgoing: bool = False,
) -> None:
    contact = format_number(contact_number)
    if not (whatsapp_account and contact and source_app):
        return

    doc_name = _get_route_doc_name(
        whatsapp_account=whatsapp_account,
        contact_number=contact,
    )
    values: dict[str, Any] = {
        "last_source_app": source_app,
    }
    if update_last_outgoing:
        values["last_outgoing_message"] = last_outgoing_message
        values["last_outgoing_at"] = now_datetime()

    existing = frappe.db.exists(dt=ROUTE_DOCTYPE, dn=doc_name)
    if existing:
        frappe.db.set_value(
            ROUTE_DOCTYPE,
            existing,
            values,
            update_modified=False,
        )
        return

    doc = frappe.get_doc(
        {
            "doctype": ROUTE_DOCTYPE,
            "name": doc_name,
            "whatsapp_account": whatsapp_account,
            "contact_number": contact,
            **values,
        }
    )
    doc.insert(ignore_permissions=True)


def set_last_sender_app(
    *,
    whatsapp_account: str,
    to_number: str,
    source_app: str,
    message_name: str | None = None,
):
    _upsert_conversation_route(
        whatsapp_account=whatsapp_account,
        contact_number=to_number,
        source_app=source_app,
        last_outgoing_message=message_name,
        update_last_outgoing=True,
    )


def get_last_sender_app(
        *, whatsapp_account: str, contact_number: str) -> str | None:

    contact = format_number(contact_number)
    if not (whatsapp_account and contact):
        return

    doc_name = _get_route_doc_name(
        whatsapp_account=whatsapp_account,
        contact_number=contact,
    )

    last_app = frappe.db.get_value(
        ROUTE_DOCTYPE,
        doc_name,
        "last_source_app"
    )
    if not last_app:
        return None
    return str(last_app)


def resolve_incoming_routed_app(
    *,
    whatsapp_account: str,
    contact_number: str,
) -> str | None:
    last_app = get_last_sender_app(
        whatsapp_account=whatsapp_account,
        contact_number=contact_number,
    )
    if last_app:
        return last_app

    default_app = frappe.db.get_value(
        "WhatsApp Account",
        whatsapp_account,
        "whatsapp_client_app",
    )
    if not default_app:
        return None

    _upsert_conversation_route(
        whatsapp_account=whatsapp_account,
        contact_number=contact_number,
        source_app=str(default_app),
    )
    return str(default_app)


def _get_forwarded_message_cache_key(message_name: str) -> str:
    return f"{FORWARDED_INCOMING_CACHE_PREFIX}{message_name}"


def _incoming_message_already_forwarded(message_name: str) -> bool:
    return bool(frappe.cache().get_value(
        _get_forwarded_message_cache_key(message_name)))


def _mark_incoming_message_forwarded(message_name: str) -> None:
    frappe.cache().set_value(
        _get_forwarded_message_cache_key(message_name),
        1,
        expires_in_sec=30 * 24 * 60 * 60
    )


def _get_attach_value(*, incoming_message_doc: WhatsAppMessage) -> str | None:
    attach = incoming_message_doc.get("attach")
    if not isinstance(attach, str) or not attach:
        return None
    return attach


def _get_attachment_file(
    *,
    incoming_message_doc: WhatsAppMessage,
) -> AttachmentFileData | None:
    attach = _get_attach_value(incoming_message_doc=incoming_message_doc)
    if not attach:
        return None

    files = cast(
        list[AttachmentFileData],
        frappe.get_all(
            "File",
            filters={
                "attached_to_doctype": incoming_message_doc.doctype,
                "attached_to_name": incoming_message_doc.name,
                "attached_to_field": "attach",
                "file_url": attach,
            },
            fields=[
                "name",
                "file_name",
                "file_type",
                "file_url",
                "is_private",
            ],
            limit=1,
        ),
    )
    if not files:
        return None

    return files[0]


def _is_absolute_url(url: str) -> bool:
    return url.startswith(("http://", "https://"))


def _is_private_attachment_url(url: str) -> bool:
    path = urlparse(url).path if _is_absolute_url(url) else url
    return path.startswith(PRIVATE_FILE_PREFIX)


def _build_absolute_url(url: str) -> str:
    if _is_absolute_url(url):
        return url

    if not url.startswith("/"):
        url = f"/{url}"

    return get_url(url)


def _build_shared_attachment_url(
    *,
    incoming_message_doc: WhatsAppMessage,
) -> str:
    query = urlencode({
        "message_name": incoming_message_doc.name,
        "key": incoming_message_doc.get_document_share_key(),
    })
    share_path = (
        "/api/method/frappe_whatsapp.utils.routing"
        f".download_shared_attachment?{query}"
    )
    return get_url(share_path)


def _get_attachment_name(
    *,
    attach: str | None,
    attachment_file: AttachmentFileData | None,
) -> str | None:
    if attachment_file and attachment_file.get("file_name"):
        return str(attachment_file["file_name"])

    if not attach:
        return None

    attachment_path = (
        urlparse(attach).path if _is_absolute_url(attach) else attach
    )
    file_name = basename(attachment_path)
    return file_name or None


def _get_attachment_mime_type(
        *, attach: str | None, attachment_name: str | None) -> str | None:
    mime_type = mimetypes.guess_type(attachment_name or "")[0]
    if mime_type:
        return mime_type

    mime_type = mimetypes.guess_type(attach or "")[0]
    if mime_type:
        return mime_type

    return None


def _get_attachment_url(
    *,
    incoming_message_doc: WhatsAppMessage,
    attachment_file: AttachmentFileData | None,
) -> str | None:
    attach = _get_attach_value(incoming_message_doc=incoming_message_doc)
    if not attach:
        return None

    if (
        attachment_file
        and attachment_file.get("is_private")
    ) or _is_private_attachment_url(str(attach)):
        return _build_shared_attachment_url(
            incoming_message_doc=incoming_message_doc
        )

    return _build_absolute_url(str(attach))


def serialize_incoming_message_for_forwarding(
    *,
    incoming_message_doc: WhatsAppMessage,
) -> dict[str, Any]:
    reload_method = getattr(incoming_message_doc, "reload", None)
    if getattr(incoming_message_doc, "name", None) and callable(reload_method):
        reload_method()

    attach = _get_attach_value(incoming_message_doc=incoming_message_doc)
    attachment_file = _get_attachment_file(
        incoming_message_doc=incoming_message_doc
    )
    attachment_name = _get_attachment_name(
        attach=attach,
        attachment_file=attachment_file,
    )

    return {
        "name": incoming_message_doc.name,
        "from": incoming_message_doc.get("from"),
        "to": incoming_message_doc.to,
        "profile_name": incoming_message_doc.get("profile_name"),
        "whatsapp_account": incoming_message_doc.whatsapp_account,
        "content_type": incoming_message_doc.content_type,
        "message": incoming_message_doc.message,
        "message_id": incoming_message_doc.message_id,
        "timestamp": str(incoming_message_doc.creation),
        "attach": attach,
        "attachment_url": _get_attachment_url(
            incoming_message_doc=incoming_message_doc,
            attachment_file=attachment_file,
        ),
        "has_attachment": bool(attach),
        "attachment_name": attachment_name,
        "attachment_mime_type": _get_attachment_mime_type(
            attach=attach,
            attachment_name=attachment_name,
        ),
    }


def forward_incoming_to_app(*, incoming_message_doc):
    routed_app = incoming_message_doc.get("routed_app")
    if not routed_app:
        routed_app = resolve_incoming_routed_app(
            whatsapp_account=str(
                incoming_message_doc.get("whatsapp_account") or ""
            ),
            contact_number=str(incoming_message_doc.get("from") or ""),
        )
    if not routed_app:
        return

    if _incoming_message_already_forwarded(incoming_message_doc.name):
        return

    from ..frappe_whatsapp.doctype.whatsapp_client_app import (
        whatsapp_client_app,
    )

    app = cast(
        whatsapp_client_app.WhatsAppClientApp,
        frappe.get_doc(
            "WhatsApp Client App",
            routed_app))
    if not app.enabled or not app.inbound_webhook_url:
        return

    payload = {
        "event": "whatsapp.incoming",
        "message": serialize_incoming_message_for_forwarding(
            incoming_message_doc=incoming_message_doc)
    }

    # best practice: enqueue to avoid slowing webhook response
    make_post_request(
        app.inbound_webhook_url,
        data=json.dumps(payload),
        headers={
            "Content-Type": "application/json",
            # Add signature or auth headers if needed
            "X-WhatsApp-App-ID": app.app_id or ""
        })
    _mark_incoming_message_forwarded(incoming_message_doc.name)


def forward_incoming_to_app_async(*, incoming_message_name: str):
    frappe.enqueue(
        "frappe_whatsapp.utils.routing.forward_incoming_to_app_by_name",
        queue="short",
        incoming_message_name=incoming_message_name,
        enqueue_after_commit=True
    )


def forward_incoming_to_app_by_name(*, incoming_message_name: str):
    incoming_message_doc = frappe.get_doc(
        "WhatsApp Message", incoming_message_name)
    forward_incoming_to_app(
        incoming_message_doc=incoming_message_doc)


def _validate_share_key(*, doc: WhatsAppMessage, key: str) -> bool:
    document_key_expiry = frappe.db.get_value(
        "Document Share Key",
        filters={
            "reference_doctype": doc.doctype,
            "reference_docname": doc.name,
            "key": key,
        },
        fieldname="expires_on",
    )
    if document_key_expiry is not None:
        if is_expired(document_key_expiry):
            raise frappe.exceptions.LinkExpired
        return True

    if frappe.get_system_settings("allow_older_web_view_links") and (
            key == doc.get_signature()):
        return True

    return False


@frappe.whitelist(allow_guest=True)
def download_shared_attachment(message_name: str, key: str):
    incoming_message_doc = cast(
        WhatsAppMessage,
        frappe.get_doc("WhatsApp Message", message_name),
    )
    if not _validate_share_key(doc=incoming_message_doc, key=key):
        raise frappe.PermissionError

    attachment_file = _get_attachment_file(
        incoming_message_doc=incoming_message_doc)
    if not attachment_file or not attachment_file.get("file_url"):
        raise frappe.DoesNotExistError

    file_url = attachment_file["file_url"]
    if not file_url:
        raise frappe.DoesNotExistError

    if attachment_file.get("is_private"):
        from frappe.utils.response import send_private_file

        return send_private_file(file_url.split("/private", 1)[1])

    from werkzeug.utils import redirect

    return redirect(_build_absolute_url(file_url))
