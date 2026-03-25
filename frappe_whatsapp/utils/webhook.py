"""Webhook."""
import frappe
import hashlib
import hmac
import json
import requests
from frappe.utils.password import get_decrypted_password as _get_decrypted_password
from werkzeug.wrappers import Response
from typing import cast, Any

from frappe_whatsapp.utils import get_whatsapp_account
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
    """Return True when ``X-Hub-Signature-256`` matches a configured app secret.

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
    """Runs in background worker. Contains the old post() logic."""
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

    # Extract the first valid change together with its parent entry's WABA ID.
    # Keeping them paired means the trust check and log message below both
    # refer to the exact same entry — a trusted later entry cannot authorize
    # an untrusted earlier one.
    changes = None
    entry_waba_id = ""
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        changes_list = entry.get("changes") or []
        if isinstance(changes_list, list) and changes_list:
            first_change = changes_list[0]
        elif isinstance(changes_list, dict):
            first_change = changes_list
        else:
            continue
        if isinstance(first_change, dict):
            changes = first_change
            entry_waba_id = str(entry.get("id") or "")
            break

    # Template-related events (status, quality, category) do NOT carry a
    # phone_number_id in their payload — Meta omits metadata entirely.
    # Handle them here, before the account-resolution step, so they are
    # never silently dropped by the whatsapp_account guard below.
    #
    # Security: validate the WABA ID of the *specific* entry being processed
    # against configured local accounts.  See _is_trusted_waba_id() for
    # details and the preferred upgrade path (X-Hub-Signature-256).
    if changes and changes.get("field") in _TEMPLATE_WEBHOOK_FIELDS:
        if _is_trusted_waba_id(entry_waba_id):
            update_status(changes)
        else:
            frappe.log_error(
                (
                    f"Template webhook event ignored: entry WABA ID "
                    f"'{entry_waba_id}' does not match any configured "
                    "WhatsApp Account (business_id). Possible spoofed request."
                ),
                "WhatsApp untrusted template webhook",
            )
        return

    messages = []
    phone_id = None

    try:
        messages = entries[0]["changes"][0]["value"].get("messages", []) or []
        phone_id = (
            entries[0]["changes"][0]
            .get("value", {})
            .get("metadata", {})
            .get("phone_number_id")
        )
    except Exception:
        messages = []

    sender_profile_name = next(
        (
            contact.get("profile", {}).get("name")
            for entry in entries
            for change in (entry.get("changes") or [])
            for contact in (change.get("value", {}).get("contacts") or [])
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
        # Message delivery status updates (field == "messages")
        if changes:
            update_status(changes)


def _process_incoming_message(
        *, message: dict, whatsapp_account, sender_profile_name: str | None):

    contact_number = message.get("from")
    if not contact_number:
        return
    routed_app = resolve_incoming_routed_app(
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
        body_text = (message.get("text") or {}).get("body", "")

        doc = frappe.get_doc({
            "doctype": "WhatsApp Message",
            "type": "Incoming",
            "from": message.get("from"),
            "message": body_text,
            "message_id": msg_id,
            "reply_to_message_id": reply_to_message_id,
            "is_reply": is_reply,
            "content_type": "text",
            "profile_name": sender_profile_name,
            "whatsapp_account": whatsapp_account.name,
            "routed_app": routed_app,
        }).insert(ignore_permissions=True)

        # Check for opt-out / opt-in keywords
        _handle_consent_keywords(
            body_text=body_text,
            contact_number=contact_number,
            whatsapp_account_name=str(whatsapp_account.name),
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
            is_reply=is_reply
        )

    elif message_type in ["image", "audio", "video", "document", "sticker"]:
        # Insert a stub message quickly, then download media async
        caption_text = (message.get(message_type) or {}).get("caption", "")
        msg_doc = frappe.get_doc({
            "doctype": "WhatsApp Message",
            "type": "Incoming",
            "from": message.get("from"),
            "message_id": msg_id,
            "reply_to_message_id": reply_to_message_id,
            "is_reply": is_reply,
            "message": caption_text,
            "content_type": message_type,
            "profile_name": sender_profile_name,
            "whatsapp_account": whatsapp_account.name,
            "routed_app": routed_app,
        }).insert(ignore_permissions=True)

        # Check for opt-out / opt-in keywords in caption (if any)
        _handle_consent_keywords(
            body_text=caption_text or "",
            contact_number=contact_number,
            whatsapp_account_name=str(whatsapp_account.name),
            message_doc_name=str(msg_doc.name),
            profile_name=sender_profile_name,
        )

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
        raw_body = message.get(message_type)
        body_text = ""
        if isinstance(raw_body, dict):
            body_text = str(raw_body.get("text") or raw_body.get("body") or "")
        elif isinstance(raw_body, str):
            body_text = raw_body

        doc = frappe.get_doc({
            "doctype": "WhatsApp Message",
            "type": "Incoming",
            "from": message.get("from"),
            "message_id": msg_id,
            "reply_to_message_id": reply_to_message_id,
            "is_reply": is_reply,
            "message": body_text,
            "content_type": message_type or "unknown",
            "profile_name": sender_profile_name,
            "whatsapp_account": whatsapp_account.name,
            "routed_app": routed_app,
        }).insert(ignore_permissions=True)

        # Check for opt-out / opt-in keywords if message contains text-like
        # body
        _handle_consent_keywords(
            body_text=body_text or "",
            contact_number=contact_number,
            whatsapp_account_name=str(whatsapp_account.name),
            message_doc_name=str(doc.name),
            profile_name=sender_profile_name,
        )

        forward_incoming_to_app_async(incoming_message_name=str(doc.name))


def _handle_consent_keywords(
        *, body_text: str, contact_number: str,
        whatsapp_account_name: str, message_doc_name: str,
        profile_name: str | None) -> None:
    """Detect opt-out or opt-in keywords and update profile consent."""
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
        )
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
        )
        send_opt_in_confirmation(
            contact_number=contact_number,
            whatsapp_account_name=whatsapp_account_name,
        )


def _handle_interactive(
        *, message, whatsapp_account, sender_profile_name,
        routed_app, reply_to_message_id, is_reply):
    interactive = message.get("interactive") or {}
    interactive_type = interactive.get("type")

    # button/list
    if interactive_type in ("button_reply", "list_reply"):
        payload = interactive.get(interactive_type) or {}
        payload_text = (
            str(payload.get("title") or payload.get("id") or "")
        )
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
            "routed_app": routed_app,
        }).insert(ignore_permissions=True)

        # Check for opt-out / opt-in keywords based on reply text/id
        _handle_consent_keywords(
            body_text=payload_text,
            contact_number=message.get("from"),
            whatsapp_account_name=str(whatsapp_account.name),
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
            "from": message.get("from"),
            "message": summary_message,
            "message_id": message.get("id"),
            "reply_to_message_id": reply_to_message_id,
            "is_reply": is_reply,
            "content_type": "flow",
            "flow_response": json.dumps(flow_response),
            "profile_name": sender_profile_name,
            "whatsapp_account": whatsapp_account.name,
            "routed_app": routed_app,
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


def _enqueue_template_sync() -> None:
    """Enqueue a background template sync from Meta.

    Uses a stable job_id combined with deduplicate=True so a burst of
    template webhook events results in at most one queued sync job.
    Frappe passes deduplicate to RQ which skips enqueueing when a job
    with the same id is already pending.
    """
    frappe.enqueue(
        "frappe_whatsapp.frappe_whatsapp.doctype."
        "whatsapp_templates.whatsapp_templates.fetch",
        queue="long",
        job_id="whatsapp_template_sync",
        deduplicate=True,
        enqueue_after_commit=True,
    )


def update_status(data):
    """Update status hook."""
    value = data.get("value")
    if not isinstance(value, dict):
        return

    field = data.get("field")

    if field == "message_template_status_update":
        update_template_status(value)

    elif field == "messages":
        update_message_status(value)

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


def update_message_status(data):
    """Update message status."""
    statuses = data.get("statuses")
    if not statuses or not isinstance(statuses, list):
        return

    from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_message.whatsapp_message import WhatsAppMessage  # noqa

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
