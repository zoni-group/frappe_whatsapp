import json
import frappe
from frappe.utils import now_datetime
from frappe_whatsapp.utils import format_number
from frappe.integrations.utils import make_post_request
from typing import cast

ROUTE_DOCTYPE = "WhatsApp Conversation Route"


def set_last_sender_app(
        *, whatsapp_account: str, to_number: str, source_app: str,
        message_name: str | None = None):
    contact = format_number(to_number)
    if not (whatsapp_account and contact and source_app):
        return

    doc_name = contact + "-" + whatsapp_account

    existing = frappe.db.exists(
            dt=ROUTE_DOCTYPE,
            dn=doc_name
        )

    values = {
        "last_source_app": source_app,
        "last_outgoing_message": message_name,
        "last_outgoing_at": now_datetime(),
    }

    if existing:
        frappe.db.set_value(
            ROUTE_DOCTYPE,
            existing, values,
            update_modified=False)
    else:
        doc = frappe.get_doc({
            "doctype": ROUTE_DOCTYPE,
            "whatsapp_account": whatsapp_account,
            "contact_number": contact,
            **values
        })
        doc.insert(ignore_permissions=True)


def get_last_sender_app(
        *, whatsapp_account: str, contact_number: str) -> str | None:

    contact = format_number(contact_number)
    if not (whatsapp_account and contact):
        return

    doc_name = contact + "-" + whatsapp_account

    last_app = frappe.db.get_value(
        ROUTE_DOCTYPE,
        doc_name,
        "last_source_app"
    )
    if not last_app:
        return None
    return str(last_app)


def forward_incoming_to_app(*, incoming_message_doc):
    routed_app = incoming_message_doc.get("routed_app")
    if not routed_app:
        return

    from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_client_app.whatsapp_client_app import WhatsAppClientApp  # noqa: E501

    app = cast(
        WhatsAppClientApp,
        frappe.get_doc(
            "WhatsApp Client App",
            routed_app))
    if not app.enabled or not app.inbound_webhook_url:
        return

    payload = {
        "event": "whatsapp.incoming",
        "message": {
            "name": incoming_message_doc.name,
            "from": incoming_message_doc.get("from"),
            "to": incoming_message_doc.to,
            "whatsapp_account": incoming_message_doc.whatsapp_account,
            "content_type": incoming_message_doc.content_type,
            "message": incoming_message_doc.message,
            "message_id": incoming_message_doc.message_id,
            "timestamp": str(incoming_message_doc.creation),
        }
    }

    # best practice: enqueue to avoid slowing webhook response
    make_post_request(
        app.inbound_webhook_url,
        data=json.dumps(payload),
        headers={
            "Content-Type": "application/json",
            # Add signature or auth headers if needed
            "X-WhatsaApp-App-ID": app.app_id or ""
        })
