from __future__ import annotations

import json
import hashlib
import mimetypes
from posixpath import basename
from typing import TYPE_CHECKING, Any, TypedDict, cast
from urllib.parse import urlencode, urlparse

import frappe
from frappe.core.doctype.document_share_key.document_share_key import (
    is_expired,
)
from frappe.utils import get_url, now_datetime
from frappe_whatsapp.utils import format_number
from frappe_whatsapp.utils.campaign_attribution import (
    resolve_message_attribution,
    serialize_referral,
)

if TYPE_CHECKING:
    from ..frappe_whatsapp.doctype.whatsapp_message.whatsapp_message import (
        WhatsAppMessage,
    )


ROUTE_DOCTYPE = "WhatsApp Conversation Route"
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
    contact_number: str = "",
    contact_profile: str | None = None,
    source_app: str,
    last_outgoing_message: str | None = None,
    update_last_outgoing: bool = False,
) -> None:
    contact = format_number(contact_number)
    if not (whatsapp_account and (contact or contact_profile) and source_app):
        return
    filters: dict[str, Any] = {"whatsapp_account": whatsapp_account}
    filters[
        "contact_profile" if contact_profile else "contact_number"
    ] = contact_profile or contact
    values: dict[str, Any] = {
        "last_source_app": source_app,
    }
    if update_last_outgoing:
        values["last_outgoing_message"] = last_outgoing_message
        values["last_outgoing_at"] = now_datetime()

    existing = frappe.db.get_value(ROUTE_DOCTYPE, filters, "name")
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
            "name": (
                _get_route_doc_name(
                    whatsapp_account=whatsapp_account,
                    contact_number=contact,
                ) if contact else None
            ),
            "whatsapp_account": whatsapp_account,
            "contact_number": contact,
            "contact_profile": contact_profile,
            **values,
        }
    )
    doc.insert(ignore_permissions=True)


def set_last_sender_app(
    *,
    whatsapp_account: str,
    to_number: str = "",
    contact_profile: str | None = None,
    source_app: str,
    message_name: str | None = None,
):
    _upsert_conversation_route(
        whatsapp_account=whatsapp_account,
        contact_number=to_number,
        contact_profile=contact_profile,
        source_app=source_app,
        last_outgoing_message=message_name,
        update_last_outgoing=True,
    )


def get_last_sender_app(
        *, whatsapp_account: str, contact_number: str = "",
        contact_profile: str | None = None) -> str | None:

    contact = format_number(contact_number)
    if not (whatsapp_account and (contact or contact_profile)):
        return
    last_app = frappe.db.get_value(
        ROUTE_DOCTYPE,
        {
            "whatsapp_account": whatsapp_account,
            ("contact_profile" if contact_profile else "contact_number"):
                contact_profile or contact,
        },
        "last_source_app"
    )
    if not last_app:
        return None
    return str(last_app)


def resolve_incoming_routed_app(
    *,
    whatsapp_account: str,
    contact_number: str = "",
    contact_profile: str | None = None,
) -> str | None:
    last_app = get_last_sender_app(
        whatsapp_account=whatsapp_account,
        contact_number=contact_number,
        contact_profile=contact_profile,
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
        contact_profile=contact_profile,
        source_app=str(default_app),
    )
    return str(default_app)


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
        "from_user_id": incoming_message_doc.get("from_user_id"),
        "from_parent_user_id": incoming_message_doc.get("from_parent_user_id"),
        "username": incoming_message_doc.get("username"),
        "contact_profile": incoming_message_doc.get("contact_profile"),
        "identity": _message_identity(incoming_message_doc),
        "to": incoming_message_doc.to,
        "profile_name": incoming_message_doc.get("profile_name"),
        "whatsapp_account": incoming_message_doc.whatsapp_account,
        "content_type": incoming_message_doc.content_type,
        "is_voice_note": (
            1 if incoming_message_doc.get("content_type") == "audio"
            and incoming_message_doc.get("is_voice_note") else 0
        ),
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
        "referral": serialize_referral(incoming_message_doc),
        "contact_payload": _structured_json(
            incoming_message_doc.get("contact_payload")
        ),
        "contact_origin": incoming_message_doc.get("contact_origin"),
    }


def _structured_json(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return value


def _message_identity(incoming_message_doc: WhatsAppMessage) -> dict[str, Any]:
    profile_name = incoming_message_doc.get("contact_profile")
    if profile_name:
        from frappe_whatsapp.utils.identity import profile_identity
        return profile_identity(
            frappe.get_doc("WhatsApp Profiles", str(profile_name))
        )
    return {
        "profile_id": None,
        "phone": incoming_message_doc.get("from"),
        "user_id": incoming_message_doc.get("from_user_id"),
        "parent_user_id": incoming_message_doc.get("from_parent_user_id"),
        "username": incoming_message_doc.get("username"),
        "preferred_recipient": (
            incoming_message_doc.get("from")
            or incoming_message_doc.get("from_user_id")
            or incoming_message_doc.get("from_parent_user_id")
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
            contact_profile=str(
                incoming_message_doc.get("contact_profile") or ""
            ) or None,
        )
    if not routed_app:
        _mark_client_event_queued(incoming_message_doc)
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
        _mark_client_event_queued(incoming_message_doc)
        return

    payload = {
        "message": serialize_incoming_message_for_forwarding(
            incoming_message_doc=incoming_message_doc)
    }
    from frappe_whatsapp.utils.client_delivery import queue_client_event
    stable_source = str(
        incoming_message_doc.message_id or incoming_message_doc.name
    )
    event_id = hashlib.sha256(
        f"whatsapp.incoming:{stable_source}".encode("utf-8")
    ).hexdigest()[:32]
    queue_client_event(
        client_app=str(app.name),
        whatsapp_account=str(incoming_message_doc.whatsapp_account),
        event_type="whatsapp.incoming",
        event_id=event_id,
        payload=payload,
    )
    _mark_client_event_queued(incoming_message_doc)


def _mark_client_event_queued(incoming_message_doc: Any) -> None:
    name = getattr(incoming_message_doc, "name", None)
    if name and frappe.db.exists("WhatsApp Message", name):
        frappe.db.set_value(
            "WhatsApp Message",
            name,
            "client_event_queued",
            1,
            update_modified=False,
        )


def recover_unqueued_incoming_events() -> None:
    """Recover an inbound event when its initial Redis job was lost."""
    if not frappe.db.has_column("WhatsApp Message", "client_event_queued"):
        return
    rows = frappe.get_all(
        "WhatsApp Message",
        filters={
            "type": "Incoming",
            "client_event_queued": 0,
            "routed_app": ["is", "set"],
        },
        fields=["name", "content_type", "attach"],
        order_by="creation asc",
        limit=100,
    )
    media_types = {"image", "audio", "video", "document", "sticker"}
    for row in rows:
        if row.content_type in media_types and not row.attach:
            continue
        forward_incoming_to_app_by_name(incoming_message_name=str(row.name))


def forward_identity_update_to_app_async(
    *, whatsapp_account: str, profile_name: str,
    previous_user_id: str | None,
    previous_parent_user_id: str | None,
    identity: dict[str, Any],
    source_event_id: str | None = None,
) -> None:
    client_app = frappe.db.get_value(
        "WhatsApp Account", whatsapp_account, "whatsapp_client_app"
    )
    if not client_app:
        return
    event_source = json.dumps(
        [whatsapp_account, profile_name, previous_user_id,
         identity.get("user_id"), previous_parent_user_id,
         identity.get("parent_user_id"), source_event_id],
        separators=(",", ":"),
    )
    event_id = hashlib.sha256(event_source.encode("utf-8")).hexdigest()[:32]
    from frappe_whatsapp.utils.client_delivery import queue_client_event
    queue_client_event(
        client_app=str(client_app),
        whatsapp_account=whatsapp_account,
        event_type="whatsapp.identity_updated",
        event_id=event_id,
        payload={
            "identity": identity,
            "previous_user_id": previous_user_id,
            "previous_parent_user_id": previous_parent_user_id,
        },
    )


def forward_incoming_to_app_async(*, incoming_message_name: str):
    queue = (
        "default"
        if frappe.db.has_column("WhatsApp Message", "referral_source_type")
        and frappe.db.get_value(
            "WhatsApp Message",
            incoming_message_name,
            "referral_source_type",
        ) == "ad"
        else "short"
    )
    frappe.enqueue(
        "frappe_whatsapp.utils.routing.forward_incoming_to_app_by_name",
        queue=queue,
        incoming_message_name=incoming_message_name,
        enqueue_after_commit=True
    )


def forward_incoming_to_app_by_name(*, incoming_message_name: str):
    incoming_message_doc = frappe.get_doc(
        "WhatsApp Message", incoming_message_name)
    resolve_message_attribution(incoming_message_doc)
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
