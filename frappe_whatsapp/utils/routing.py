import frappe
from frappe.utils import now_datetime
from frappe_whatsapp.utils import format_number

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
