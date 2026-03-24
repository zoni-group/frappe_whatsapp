"""frappe_whatsapp/utils/status_notifier.py

Centralized outbound status-notification subsystem.

Responsibilities
----------------
- Detect material status changes on outgoing WhatsApp Messages.
- Write a durable log entry (WhatsApp Status Webhook Log) as an outbox
  record so nothing is lost if the initial HTTP call fails.
- Enqueue async delivery jobs *after* the current DB transaction commits,
  so the log row is already visible to the background worker.
- Deliver webhook POSTs to WhatsApp Client App.status_webhook_url.
- Update the log with the delivery outcome and expose failed entries to a
  scheduled retry job.

Event model
-----------
Two Frappe doc_events hooks (registered in hooks.py) are the only entry
points into this subsystem:

    on_whatsapp_message_after_insert(doc)
        Fires once when a new outgoing message is first inserted.  Handles
        the initial ``Success`` / ``Failed`` status set during before_insert.

    on_whatsapp_message_on_update(doc)
        Fires on every subsequent save.  Uses get_doc_before_save() to
        detect status transitions coming from Meta delivery callbacks
        (``sent``, ``delivered``, ``read``, ``failed``) or from any other
        code path that calls doc.save().

Delivery state machine
----------------------
Each log row moves through the following states::

    Pending ──► Processing ──► Delivered   (happy path)
    Pending ──► Processing ──► Failed ──► Processing ──► ...  (retry loop)
    Pending ──► Processing ──► Skipped     (app disabled mid-flight)

``Pending``
    Written by the outbox on first detection of a material status change.
    The initial ``enqueue_after_commit`` job transitions it to Processing.

``Processing``
    An active lease is held by one worker.  ``claim_expires_at`` marks when
    the lease expires; NULL or past → reclaimable by the next worker.

``Delivered`` / ``Failed`` / ``Skipped``
    Terminal (or semi-terminal for Failed) states.  ``claim_expires_at`` is
    cleared on write — it carries no meaning outside Processing.

Adding request signing
----------------------
To add per-app HMAC-SHA256 request signing in the future:

1. Add a ``webhook_secret`` Password field to WhatsApp Client App.
2. In ``deliver_status_notification``, read::

       secret = app_doc.get_password("webhook_secret")

   and, if non-empty, compute::

       import hmac, hashlib
       sig = hmac.new(secret.encode(), body_bytes, hashlib.sha256).hexdigest()

   then add ``"X-WhatsApp-Signature": f"sha256={sig}"`` to the headers.
   The ``body_bytes`` must be the exact bytes sent as the request body.
"""
from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_client_app.whatsapp_client_app import (  # noqa: E501
        WhatsAppClientApp,
    )
    from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_status_webhook_log.whatsapp_status_webhook_log import (  # noqa: E501
        WhatsAppStatusWebhookLog,
    )

import frappe
import requests
from frappe.utils import add_to_date, now_datetime

STATUS_WEBHOOK_LOG_DOCTYPE = "WhatsApp Status Webhook Log"

# Cease retrying after this many total delivery attempts (initial + retries).
MAX_RETRY_ATTEMPTS = 5

# Hours to wait before each successive retry (indexed by attempt number).
# Attempt 1 is the initial enqueue; retries start from attempt 2.
_RETRY_BACKOFF_HOURS = [0, 1, 2, 4, 8]

# Minutes after log creation before the scheduler treats a Pending row as
# stale and eligible for recovery.  Covers the case where the initial
# enqueue_after_commit job is lost (worker crash, Redis restart, etc.).
PENDING_RECOVERY_MINUTES = 15

# How long a Processing claim is considered valid.  If a worker crashes
# after committing the claim and before writing Delivered/Failed, the
# scheduler will re-enqueue the row once this TTL has elapsed.
CLAIM_TIMEOUT_MINUTES = 5

# Dotted path used by both maybe_enqueue and the retry scheduler.
_DELIVER_FN = (
    "frappe_whatsapp.utils.status_notifier.deliver_status_notification"
)

# Internal statuses that carry error-detail sub-fields on the message.
_FAILED_STATUSES = frozenset({"failed", "Failed"})

# Map raw internal / Meta statuses to stable normalized values.
_NORMALIZE_MAP: dict[str, str] = {
    "success": "accepted",    # Meta accepted our outbound API request
    "failed": "failed",
    "sent": "sent",
    "delivered": "delivered",
    "read": "read",
    "marked as read": "read",
}


# ── Status helpers ─────────────────────────────────────────────────────────


def _normalize_status(raw: str | None) -> str:
    """Return a stable normalized status string for the outbound payload."""
    if not raw:
        return "unknown"
    return _NORMALIZE_MAP.get(raw.lower(), raw.lower())


def _get_last_sql_row_count() -> int:
    """Return the connection-local SQL ROW_COUNT() as a safe integer.

    ``frappe.db.sql`` is typed broadly enough that direct ``[0][0]`` access
    triggers static-analysis errors. Normalize the result into a runtime-
    checked scalar instead of indexing blindly.
    """
    rows = list(frappe.db.sql("SELECT ROW_COUNT()", as_list=True) or [])
    if not rows:
        return 0

    first_row = rows[0]
    if not isinstance(first_row, (list, tuple)) or not first_row:
        return 0

    value = first_row[0]
    if value is None:
        return 0

    return int(value)


def _build_event_id(
    message_name: str,
    current_status: str | None,
    error_code: str | None = None,
    error_title: str | None = None,
    error_message: str | None = None,
    error_details: str | None = None,
    error_href: str | None = None,
) -> str:
    """Stable idempotency key.

    The same (message_name, current_status, <all error fields>) tuple
    always produces the same event_id, so duplicate calls for the same
    logical event are silently dropped by the outbox.

    All five error fields are included so that any enrichment Meta sends
    in a follow-up ``failed`` callback (new code, title, message, details,
    or href) generates a distinct event_id and a fresh notification.
    """
    parts = [
        message_name,
        current_status or "",
        error_code or "",
        error_title or "",
        error_message or "",
        error_details or "",
        error_href or "",
    ]
    return hashlib.sha256(":".join(parts).encode()).hexdigest()[:32]


def _is_material_change(
    doc: Any,
    previous_doc: Any | None,
) -> tuple[bool, str | None]:
    """Return ``(is_material, previous_status_value)``.

    A change is material when:
    - The ``status`` field value changes, OR
    - The message is already in a failed state and error-detail fields
      change (Meta sometimes enriches error information in follow-up
      callbacks for the same failed status).
    """
    if previous_doc is None:
        # Initial insert path – always material when a status is set.
        return bool(getattr(doc, "status", None)), None

    prev_status: str | None = getattr(previous_doc, "status", None)
    curr_status: str | None = getattr(doc, "status", None)

    if prev_status != curr_status:
        return True, prev_status

    # Same status, but check for enriched error detail on failed messages.
    if curr_status and curr_status.lower() == "failed":
        for field in (
            "status_error_code",
            "status_error_message",
            "status_error_title",
            "status_error_details",
            "status_error_href",
        ):
            prev_val = getattr(previous_doc, field, None)
            curr_val = getattr(doc, field, None)
            if prev_val != curr_val:
                return True, prev_status

    return False, prev_status


# ── Client app lookup ──────────────────────────────────────────────────────


def _get_app_doc(source_app: str) -> Any | None:
    """Return the client app document if it is enabled and configured.

    Returns None (silently) if the app does not exist, is disabled, or has
    no status_webhook_url — all conditions under which notifications are
    intentionally skipped.
    """
    try:
        app = cast(
            "WhatsAppClientApp",
            frappe.get_doc("WhatsApp Client App", source_app))
    except frappe.DoesNotExistError:
        return None

    if not app.enabled or not app.status_webhook_url:
        return None

    return app


# ── Payload builder ────────────────────────────────────────────────────────


def _build_payload(
    doc: Any,
    previous_status: str | None,
    app_doc: Any,
    event_id: str,
) -> dict:
    """Build the stable webhook payload delivered to the client app."""
    current_status: str | None = getattr(doc, "status", None)

    payload: dict[str, Any] = {
        "event": "whatsapp.message_status",
        "event_id": event_id,
        "occurred_at": str(now_datetime()),
        "app_id": getattr(app_doc, "app_id", None) or "",
        "message": {
            "name": doc.name,
            "message_id": getattr(doc, "message_id", None) or "",
            "external_reference": getattr(
                doc, "external_reference", None) or "",
            "source_app": getattr(doc, "source_app", None) or "",
            "to": getattr(doc, "to", None) or "",
            "whatsapp_account": getattr(doc, "whatsapp_account", None) or "",
            "previous_status": previous_status or "",
            "current_status": current_status or "",
            "normalized_status": _normalize_status(current_status),
            "conversation_id": getattr(doc, "conversation_id", None) or "",
            "content_type": getattr(doc, "content_type", None) or "",
            "type": "Outgoing",
        },
    }

    # Include an error block when the message failed or carries error fields.
    error_block: dict[str, str] = {}
    error_code = getattr(doc, "status_error_code", None)
    if error_code:
        error_block["code"] = str(error_code)
    for attr, key in (
        ("status_error_title", "title"),
        ("status_error_message", "message"),
        ("status_error_details", "details"),
        ("status_error_href", "href"),
    ):
        val = getattr(doc, attr, None)
        if val:
            error_block[key] = str(val)

    if error_block or (current_status or "") in _FAILED_STATUSES:
        payload["error"] = error_block

    return payload


# ── Outbox management ──────────────────────────────────────────────────────


def _create_log_if_new(
    doc: Any,
    previous_status: str | None,
    app_doc: Any,
) -> str | None:
    """Write a WhatsApp Status Webhook Log for this event.

    Race-safe idempotency: we attempt the INSERT unconditionally and treat
    a unique-constraint collision on ``event_id`` as a clean no-op.  This
    eliminates the TOCTOU window that a pre-insert EXISTS check would leave
    between two concurrent workers processing the same callback.

    Returns the new log name, or None if the event was already logged.
    """
    current_status: str | None = getattr(doc, "status", None)
    event_id = _build_event_id(
        doc.name,
        current_status,
        error_code=getattr(doc, "status_error_code", None),
        error_title=getattr(doc, "status_error_title", None),
        error_message=getattr(doc, "status_error_message", None),
        error_details=getattr(doc, "status_error_details", None),
        error_href=getattr(doc, "status_error_href", None),
    )

    payload = _build_payload(doc, previous_status, app_doc, event_id)

    log = frappe.get_doc(
        {
            "doctype": STATUS_WEBHOOK_LOG_DOCTYPE,
            "message_name": doc.name,
            "source_app": getattr(doc, "source_app", None),
            "event_id": event_id,
            "delivery_status": "Pending",
            "previous_status": previous_status or "",
            "current_status": current_status or "",
            "payload": json.dumps(payload),
            "attempts": 0,
            # Recovery deadline: if the initial enqueue_after_commit job is
            # lost (worker crash, Redis restart), the hourly scheduler will
            # re-enqueue this row once next_retry_at is in the past.
            "next_retry_at": add_to_date(
                now_datetime(), minutes=PENDING_RECOVERY_MINUTES
            ),
        }
    )
    # ignore_links=True: always written from a server-side hook where the
    # linked message is guaranteed to exist; skip application-level link
    # validation to keep the hot path lean.
    try:
        log.insert(ignore_permissions=True, ignore_links=True)
    except frappe.UniqueValidationError:
        # Concurrent duplicate event — the unique constraint on event_id
        # absorbed the race.  Treat as a silent no-op.
        return None

    return str(log.name)


# ── Public entry points ────────────────────────────────────────────────────


def maybe_enqueue_status_notification(
    doc: Any,
    previous_status: str | None,
) -> None:
    """Create an outbox log entry and enqueue async delivery.

    Safe to call from any document lifecycle hook.  Bails early when:
    - The message is not Outgoing.
    - There is no source_app set on the message.
    - The referenced client app is disabled or has no status_webhook_url.
    """
    if getattr(doc, "type", None) != "Outgoing":
        return
    if not getattr(doc, "source_app", None):
        return

    app_doc = _get_app_doc(doc.source_app)
    if not app_doc:
        return

    log_name = _create_log_if_new(doc, previous_status, app_doc)
    if not log_name:
        return  # Duplicate event — already logged.

    frappe.enqueue(
        _DELIVER_FN,
        queue="short",
        log_name=log_name,
        enqueue_after_commit=True,
    )


def deliver_status_notification(log_name: str) -> None:
    """Deliver the webhook POST for a status log entry.

    Called by the background worker on first attempt and by the retry
    scheduler on subsequent attempts.  Updates the log with the outcome.

    Concurrency safety: an atomic UPDATE-WHERE claim transitions the row
    to Processing and stamps claim_expires_at = now + CLAIM_TIMEOUT_MINUTES
    before any HTTP work begins.  Only the worker that wins this UPDATE
    proceeds; concurrent workers return without posting.  The claim is
    committed immediately so the lock is released before the HTTP call.

    Crash recovery: if a worker crashes after committing the claim but
    before writing Delivered/Failed, the row stays in Processing.  The
    retry scheduler detects the expired claim_expires_at and re-enqueues.
    The re-enqueued job re-claims via the same atomic UPDATE (which accepts
    Processing rows whose claim_expires_at is in the past).
    """
    now_ts = now_datetime()
    claim_expires = add_to_date(now_ts, minutes=CLAIM_TIMEOUT_MINUTES)

    # Atomic claim: transitions Pending/Failed → Processing, or re-claims
    # a stale Processing row whose lease has expired or was never set.
    # NULL claim_expires_at (e.g. a row that was set to Processing by an
    # older code path without stamping a lease) is treated as immediately
    # reclaimable — equivalent to an already-expired claim.
    frappe.db.sql(
        "UPDATE `tabWhatsApp Status Webhook Log`"
        " SET `delivery_status` = 'Processing',"
        "     `claim_expires_at` = %s"
        " WHERE `name` = %s"
        " AND ("
        "   `delivery_status` IN ('Pending', 'Failed')"
        "   OR ("
        "     `delivery_status` = 'Processing'"
        "     AND (`claim_expires_at` IS NULL OR `claim_expires_at` <= %s)"
        "   )"
        " )",
        [claim_expires, log_name, now_ts],
    )
    claimed = _get_last_sql_row_count()
    # Commit immediately: releases the lock before the blocking HTTP call
    # and lets the scheduler observe the fresh claim_expires_at.
    frappe.db.commit()

    if not claimed:
        # Another worker holds a valid claim, or the row is in a terminal
        # state (Delivered/Skipped) — safe to ignore.
        return

    log = cast(
        "WhatsAppStatusWebhookLog",
        frappe.get_doc(STATUS_WEBHOOK_LOG_DOCTYPE, log_name))

    app_doc = _get_app_doc(str(log.source_app))
    if not app_doc:
        frappe.db.set_value(
            STATUS_WEBHOOK_LOG_DOCTYPE,
            log_name,
            # Clear the lease — claim_expires_at has no meaning outside
            # Processing state.
            {"delivery_status": "Skipped", "claim_expires_at": None},
        )
        return

    payload = (
        json.loads(log.payload)
        if isinstance(log.payload, str)
        else (log.payload or {})
    )
    new_attempts = (log.attempts or 0) + 1
    backoff_idx = min(new_attempts, len(_RETRY_BACKOFF_HOURS) - 1)
    backoff_hours = _RETRY_BACKOFF_HOURS[backoff_idx]

    try:
        resp = requests.post(
            app_doc.status_webhook_url,
            json=payload,
            headers={
                "Content-Type": "application/json",
                "X-WhatsApp-App-ID": getattr(app_doc, "app_id", None) or "",
                "X-Event-ID": log.event_id or "",
            },
            timeout=10,
        )

        success = resp.ok
        next_retry = (
            None
            if success
            else add_to_date(now_datetime(), hours=backoff_hours)
        )

        frappe.db.set_value(
            STATUS_WEBHOOK_LOG_DOCTYPE,
            log_name,
            {
                "delivery_status": "Delivered" if success else "Failed",
                "attempts": new_attempts,
                "last_attempted_at": now_datetime(),
                "response_code": str(resp.status_code),
                "response_body": (resp.text or "")[:500],
                "error": (
                    ""
                    if success
                    else f"HTTP {resp.status_code}: {(resp.text or '')[:100]}"
                ),
                "next_retry_at": next_retry,
                # Lease is released — claim_expires_at only means something
                # while the row is in Processing state.
                "claim_expires_at": None,
            },
        )

        if not success:
            frappe.log_error(
                (
                    f"Status webhook POST failed for log {log_name}: "
                    f"HTTP {resp.status_code}\n{resp.text[:200]}"
                ),
                "WhatsApp Status Notifier",
            )

    except Exception:
        next_retry = add_to_date(now_datetime(), hours=backoff_hours)
        frappe.db.set_value(
            STATUS_WEBHOOK_LOG_DOCTYPE,
            log_name,
            {
                "delivery_status": "Failed",
                "attempts": new_attempts,
                "last_attempted_at": now_datetime(),
                "error": frappe.get_traceback()[:200],
                "next_retry_at": next_retry,
                "claim_expires_at": None,
            },
        )
        frappe.log_error(
            frappe.get_traceback(),
            f"WhatsApp Status Notifier exception for log {log_name}",
        )


def retry_failed_status_notifications() -> None:
    """Scheduled task: re-enqueue overdue and stale-claimed deliveries.

    Runs hourly.  Handles three recovery cases:

    1. Failed rows due for retry
       delivery_status=Failed, attempts < MAX, next_retry_at past/unset.

    2. Pending rows whose initial job was lost (Redis restart, crash
       before enqueue_after_commit fired, etc.)
       delivery_status=Pending, attempts < MAX, next_retry_at past.
       Each outbox row is created with next_retry_at = now +
       PENDING_RECOVERY_MINUTES, so legitimate in-flight rows are skipped.

    3. Processing rows with an expired claim (worker crashed after claim
       commit but before writing Delivered/Failed)
       delivery_status=Processing, attempts < MAX,
       claim_expires_at past/unset.
       deliver_status_notification() will atomically re-claim such rows
       via the expired-claim branch in its UPDATE WHERE clause.
    """
    now = now_datetime()

    # ── Case 1 & 2: Failed / Pending ───────────────────────────────────
    candidates = frappe.get_all(
        STATUS_WEBHOOK_LOG_DOCTYPE,
        filters={
            "delivery_status": ["in", ["Failed", "Pending"]],
            "attempts": ["<", MAX_RETRY_ATTEMPTS],
        },
        fields=["name", "next_retry_at"],
    )
    for row in candidates:
        next_retry = row.get("next_retry_at")
        if next_retry and next_retry > now:
            continue
        frappe.enqueue(
            _DELIVER_FN, queue="short", log_name=row["name"]
        )

    # ── Case 3: stale Processing claims ────────────────────────────────
    stale = frappe.get_all(
        STATUS_WEBHOOK_LOG_DOCTYPE,
        filters={
            "delivery_status": "Processing",
            "attempts": ["<", MAX_RETRY_ATTEMPTS],
        },
        fields=["name", "claim_expires_at"],
    )
    for row in stale:
        expires = row.get("claim_expires_at")
        if expires and expires > now:
            continue  # Valid claim — leave the worker alone.
        frappe.enqueue(
            _DELIVER_FN, queue="short", log_name=row["name"]
        )


# ── Post-migrate index maintenance ─────────────────────────────────────────


def ensure_status_log_indexes() -> None:
    """Create the composite indexes for the retry scan if they do not exist.

    Called via the ``after_migrate`` hook so indexes are created (once)
    whenever ``bench migrate`` runs.  Safe to call repeatedly — per-index
    existence checks make each step a no-op when already present.

    Two indexes are maintained:

    ``idx_status_retry_scan``
        Covers the Failed/Pending query in
        ``retry_failed_status_notifications``:
        (delivery_status, attempts, next_retry_at)

    ``idx_status_claim_scan``
        Covers the stale-Processing query in the same function:
        (delivery_status, attempts, claim_expires_at)
    """
    table = f"tab{STATUS_WEBHOOK_LOG_DOCTYPE}"
    indexes = (
        (
            "idx_status_retry_scan",
            ["delivery_status", "attempts", "next_retry_at"],
        ),
        (
            "idx_status_claim_scan",
            ["delivery_status", "attempts", "claim_expires_at"],
        ),
    )

    for index_name, fields in indexes:
        if not frappe.db.has_index(table, index_name):
            # Use Frappe's helper so the DDL runs across a safe commit
            # boundary during migrate instead of tripping the implicit
            # commit guard on raw ALTER TABLE.
            frappe.db.add_index(
                STATUS_WEBHOOK_LOG_DOCTYPE,
                fields,
                index_name=index_name,
            )


# ── Frappe doc_events handlers ─────────────────────────────────────────────


def on_whatsapp_message_after_insert(
    doc: Any, method: str | None = None
) -> None:
    """doc_events hook: fires after a new WhatsApp Message is inserted.

    Handles the initial outgoing status (e.g. ``Success`` or ``Failed``)
    set during ``before_insert`` by the Meta API call.
    """
    if getattr(doc, "type", None) != "Outgoing":
        return
    if not getattr(doc, "status", None):
        return
    maybe_enqueue_status_notification(doc, previous_status=None)


def on_whatsapp_message_on_update(
    doc: Any, method: str | None = None
) -> None:
    """doc_events hook: fires after a WhatsApp Message is updated (saved).

    Handles Meta delivery callbacks (``sent``, ``delivered``, ``read``,
    ``failed``) and any other status transitions that come via doc.save().
    Skips silently on initial insert (handled by after_insert).
    """
    if getattr(doc, "type", None) != "Outgoing":
        return

    previous_doc = doc.get_doc_before_save()
    if previous_doc is None:
        # Initial insert path: after_insert already handles this.
        return

    changed, previous_status = _is_material_change(doc, previous_doc)
    if not changed:
        return

    maybe_enqueue_status_notification(doc, previous_status=previous_status)
