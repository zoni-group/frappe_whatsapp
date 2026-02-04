"""Webhook."""
import frappe
import json
import requests
from werkzeug.wrappers import Response
from typing import cast

from frappe_whatsapp.utils import get_whatsapp_account
from frappe_whatsapp.utils.routing import get_last_sender_app, \
    forward_incoming_to_app_async


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
    """POST: accept quickly, enqueue processing."""
    # Keep it lightweight: get JSON / form_dict
    data = frappe.local.form_dict
    try:
        # if it's a JSON body, form_dict can be empty; try request JSON too
        if not data and hasattr(frappe.request, "get_json"):
            data = frappe.request.get_json() or {}
    except Exception:
        pass

    # Optional: store raw payload for troubleshooting (this is a DB write).
    # If you want "fastest possible", log only on errors, or log via
    # enqueue too.
    try:
        frappe.get_doc({
            "doctype": "WhatsApp Notification Log",
            "template": "Webhook",
            "meta_data": json.dumps(data)
        }).insert(ignore_permissions=True)
    except Exception:
        # never block the webhook because logging failed
        frappe.log_error(
            frappe.get_traceback(),
            "WhatsApp webhook log insert failed")

    # ✅ Enqueue heavy work
    frappe.enqueue(
        "frappe_whatsapp.utils.webhook.process_webhook_payload",
        queue="short",
        data=data,
        enqueue_after_commit=True,
        job_name="whatsapp_webhook_process"
    )

    # ✅ Return immediately
    return Response("ok", status=200)


def process_webhook_payload(data: dict):
    """Runs in background worker. Contains the old post() logic."""
    # Defensive: data can be string sometimes
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except Exception:
            data = {}

    messages = []
    phone_id = None

    try:
        messages = data["entry"][0]["changes"][0]["value"].get("messages", [])
        phone_id = (
            data.get("entry", [{}])[0]
            .get("changes", [{}])[0]
            .get("value", {})
            .get("metadata", {})
            .get("phone_number_id")
        )
    except Exception:
        # fallback shapes
        try:
            messages = data["entry"]["changes"][0]["value"].get("messages", [])
        except Exception:
            messages = []

    sender_profile_name = next(
        (
            contact.get("profile", {}).get("name")
            for entry in data.get("entry", [])
            for change in entry.get("changes", [])
            for contact in change.get("value", {}).get("contacts", [])
        ),
        None,
    )

    whatsapp_account = get_whatsapp_account(phone_id) if phone_id else None
    if not whatsapp_account:
        return

    from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_account.whatsapp_account import WhatsAppAccount  # noqa
    whatsapp_account = cast(WhatsAppAccount, whatsapp_account)

    if messages:
        for message in messages:
            _process_incoming_message(
                message=message,
                whatsapp_account=whatsapp_account,
                sender_profile_name=sender_profile_name
            )
    else:
        # status updates / template status updates
        changes = None
        try:
            changes = data["entry"][0]["changes"][0]
        except Exception:
            try:
                changes = data["entry"]["changes"][0]
            except Exception:
                changes = None

        if changes:
            update_status(changes)


def _process_incoming_message(
        *, message: dict, whatsapp_account, sender_profile_name: str | None):

    contact_number = message.get("from")
    if not contact_number:
        return
    last_app = get_last_sender_app(
        whatsapp_account=str(whatsapp_account.name),
        contact_number=contact_number
    )

    message_type = message.get("type")
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
    msg_id = message.get("id")
    if msg_id and frappe.db.exists("WhatsApp Message", {"message_id": msg_id}):
        return

    if message_type == "text":
        doc = frappe.get_doc({
            "doctype": "WhatsApp Message",
            "type": "Incoming",
            "from": message.get("from"),
            "message": (message.get("text") or {}).get("body"),
            "message_id": msg_id,
            "reply_to_message_id": reply_to_message_id,
            "is_reply": is_reply,
            "content_type": "text",
            "profile_name": sender_profile_name,
            "whatsapp_account": whatsapp_account.name,
            "routed_app": last_app,
        }).insert(ignore_permissions=True)

        # ✅ forward to client app in background
        forward_incoming_to_app_async(incoming_message_name=str(doc.name))

    elif message_type == "interactive":
        _handle_interactive(
            message=message,
            whatsapp_account=whatsapp_account,
            sender_profile_name=sender_profile_name,
            last_app=last_app,
            reply_to_message_id=reply_to_message_id,
            is_reply=is_reply
        )

    elif message_type in ["image", "audio", "video", "document"]:
        # Insert a stub message quickly, then download media async
        msg_doc = frappe.get_doc({
            "doctype": "WhatsApp Message",
            "type": "Incoming",
            "from": message.get("from"),
            "message_id": msg_id,
            "reply_to_message_id": reply_to_message_id,
            "is_reply": is_reply,
            "message": message.get(message_type, {}).get("caption", ""),
            "content_type": message_type,
            "profile_name": sender_profile_name,
            "whatsapp_account": whatsapp_account.name,
            "routed_app": last_app,
        }).insert(ignore_permissions=True)

        forward_incoming_to_app_async(incoming_message_name=str(msg_doc.name))

        media_id = (message.get(message_type) or {}).get("id")
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

    else:
        doc = frappe.get_doc({
            "doctype": "WhatsApp Message",
            "type": "Incoming",
            "from": message.get("from"),
            "message_id": msg_id,
            "reply_to_message_id": reply_to_message_id,
            "is_reply": is_reply,
            "message": message.get(message_type),
            "content_type": message_type or "unknown",
            "profile_name": sender_profile_name,
            "whatsapp_account": whatsapp_account.name,
            "routed_app": last_app,
        }).insert(ignore_permissions=True)

        forward_incoming_to_app_async(incoming_message_name=str(doc.name))


def _handle_interactive(
        *, message, whatsapp_account, sender_profile_name,
        last_app, reply_to_message_id, is_reply):
    interactive = message.get("interactive") or {}
    interactive_type = interactive.get("type")

    # button/list
    if interactive_type in ("button_reply", "list_reply"):
        payload = interactive.get(interactive_type) or {}
        doc = frappe.get_doc({
            "doctype": "WhatsApp Message",
            "type": "Incoming",
            "from": message.get("from"),
            "message": payload.get("id"),
            "message_id": message.get("id"),
            "reply_to_message_id": reply_to_message_id,
            "is_reply": is_reply,
            "content_type": "button",
            "profile_name": sender_profile_name,
            "whatsapp_account": whatsapp_account.name,
            "routed_app": last_app,
        }).insert(ignore_permissions=True)
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
            "from": message.get("from"),
            "message": summary_message,
            "message_id": message.get("id"),
            "reply_to_message_id": reply_to_message_id,
            "is_reply": is_reply,
            "content_type": "flow",
            "flow_response": json.dumps(flow_response),
            "profile_name": sender_profile_name,
            "whatsapp_account": whatsapp_account.name,
            "routed_app": last_app,
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


def update_status(data):
    """Update status hook."""
    if data.get("field") == "message_template_status_update":
        update_template_status(data['value'])

    elif data.get("field") == "messages":
        update_message_status(data['value'])


def update_template_status(data):
    """Update template status."""
    frappe.db.sql(
        """UPDATE `tabWhatsApp Templates`
        SET status = %(event)s
        WHERE id = %(message_template_id)s""",
        data
    )


def update_message_status(data):
    """Update message status."""
    id = data['statuses'][0]['id']
    status = data['statuses'][0]['status']
    conversation = data['statuses'][0].get('conversation', {}).get('id')
    name = frappe.db.get_value("WhatsApp Message", filters={"message_id": id})
    if not name:
        return

    from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_message.whatsapp_message import WhatsAppMessage  # noqa
    doc = cast(WhatsAppMessage, frappe.get_doc("WhatsApp Message", str(name)))
    doc.status = status
    if conversation:
        doc.conversation_id = conversation
    doc.save(ignore_permissions=True)


def download_and_attach_media(
        whatsapp_account_name: str,
        message_docname: str, media_id: str, message_type: str):
    from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_account.whatsapp_account import WhatsAppAccount  # noqa
    whatsapp_account = cast(
        WhatsAppAccount,
        frappe.get_doc("WhatsApp Account", whatsapp_account_name))
    token = whatsapp_account.get_password("token")
    base_url = f"{whatsapp_account.url}/{whatsapp_account.version}/"

    headers = {"Authorization": f"Bearer {token}"}

    # 1) Get media metadata to retrieve url/mime
    r = requests.get(f"{base_url}{media_id}/", headers=headers, timeout=30)
    r.raise_for_status()
    media_data = r.json()

    media_url = media_data.get("url")
    mime_type = media_data.get("mime_type") or "application/octet-stream"
    file_extension = (mime_type.split("/")[-1] or "bin")

    # 2) Download content
    r2 = requests.get(media_url, headers=headers, timeout=60)
    r2.raise_for_status()

    file_data = r2.content
    file_name = f"{frappe.generate_hash(length=10)}.{file_extension}"

    # 3) Attach to WhatsApp Message
    from frappe.core.doctype.file.file import File
    file_doc = cast(File, frappe.get_doc({
        "doctype": "File",
        "file_name": file_name,
        "attached_to_doctype": "WhatsApp Message",
        "attached_to_name": message_docname,
        "attached_to_field": "attach",
        "content": file_data,
    }))
    file_doc.save(ignore_permissions=True)

    frappe.db.set_value(
        "WhatsApp Message",
        message_docname,
        "attach",
        file_doc.file_url)


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
