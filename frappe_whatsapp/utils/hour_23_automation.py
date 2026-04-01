"""Hour-23 follow-up automation.

Sends a consent-request or status follow-up template to contacts whose
latest inbound message falls in the final hour of the service window
(i.e. window_hours-1 <= hours_since_last_incoming < window_hours).

Entry points (both registered as hourly scheduler jobs):
  run_hour_23_automation()       — normal candidate scan and send.
  recover_stale_hour_23_claims() — recovery for claimed-but-unsent rows.

Idempotency and crash-safety
-----------------------------
Sending is a three-phase operation:

  Phase 1 — claim:
    Insert (or atomically re-claim) a ``WhatsApp Hour 23 Automation Log``
    row with send_status='Pending' and a short-lived lease
    (claim_expires_at = now + CLAIM_LEASE_MINUTES).  Commit immediately so
    the claim is visible to concurrent workers before any network call.
    Only one worker can win the unique ``anchor_message`` constraint.

  Phase 2 — send and commit outbound message:
    Call msg_doc.insert() with ``reference_doctype="WhatsApp Hour 23
    Automation Log"`` and ``reference_name=log_name`` set on the outbound
    message.  **Immediately after the insert, call frappe.db.commit() to
    durably persist the outbound message row before updating the log.**
    This minimises the window between Meta accepting the send and the log
    reflecting it.

  Phase 3 — finalise log:
    Set send_status='Sent', outgoing_message, and sent_at on the log row
    and clear claim_expires_at.

Delivery-guarantee limitation
-------------------------------
``WhatsApp Message.before_insert()`` performs the actual HTTP POST to Meta
*before* the document row is written to the database.  A process crash or
transaction rollback after the API call but before Phase 2's
``frappe.db.commit()`` will result in the message being delivered but no
durable DB record of it.  This narrow window cannot be eliminated without
changes to the WhatsApp Message controller.

For the wider window (Phase 2 committed, Phase 3 not yet committed):
``recover_stale_hour_23_claims()`` calls ``_reconcile_if_already_sent()``
at the start of each recovery attempt.  That helper queries for a
``WhatsApp Message`` whose ``reference_name`` equals the log row name.  If
one is found, the log row is finalised to ``Sent`` without any re-send.

If a worker crashes between Phase 1 and Phase 2, the row stays in
'Pending' with an expired claim_expires_at.  recover_stale_hour_23_claims()
runs hourly, finds these orphaned rows, atomically re-claims them (via the
same UPDATE-WHERE stale-lease check used by status_notifier.py), and
re-attempts Phase 2.

Recovery eligibility
---------------------
Before re-sending, ``_retry_stale_claim()`` re-evaluates the contact's
*current* state using the same shared helpers as the primary path
(``_load_contact_state`` and ``_check_template_shape``).  If the contact
or template is no longer eligible, the row is marked ``Skipped`` (terminal)
and ``claim_expires_at`` is cleared so the row never appears as stale again.

Terminal conditions → ``Skipped``:
  - contact is now DNC or opted-out
  - recomputed template/type from current lang map does not match stored
  - template is no longer APPROVED or has an unsupported shape

Retryable condition → stays ``Pending`` with an expired lease:
  - transient send failure (``msg_doc.insert()`` raises); the exception
    propagates, the lease expires, and the next recovery run picks the row
    up again.
"""
import frappe
from frappe import _
from frappe.utils import now_datetime, add_to_date
from typing import Any, cast

from frappe_whatsapp.utils import format_number

# How long a freshly inserted Pending claim is considered live.
# If a worker crashes between the commit and msg_doc.insert(), the row
# stays in Pending. recover_stale_hour_23_claims() re-claims it once
# CLAIM_LEASE_MINUTES have elapsed.
CLAIM_LEASE_MINUTES = 10


def run_hour_23_automation() -> None:
    """Hourly scheduler entry point for the hour-23 follow-up automation."""
    settings = frappe.get_cached_doc("WhatsApp Compliance Settings")
    if not getattr(settings, "enable_hour_23_follow_up", 0):
        return

    window_hours = int(getattr(settings, "window_hours", 24) or 24)
    marketing_consent_category = (
        getattr(settings, "marketing_consent_category", None) or None
    )
    lang_map = _build_language_map(settings)

    if not lang_map:
        frappe.log_error(
            "Hour-23 automation skipped: no language map rows configured "
            "in WhatsApp Compliance Settings.",
            "WhatsApp Hour-23 Automation",
        )
        return

    candidates = _get_candidates(window_hours)
    for cand in candidates:
        try:
            _process_candidate(
                candidate=cand,
                lang_map=lang_map,
                marketing_consent_category=marketing_consent_category,
            )
        except Exception:
            frappe.log_error(
                frappe.get_traceback(),
                _(
                    "Hour-23 automation error for {0}"
                ).format(cand.get("contact_number")),
            )


# ── Helpers ──────────────────────────────────────────────────────────────────

def _build_language_map(settings) -> dict[str, Any]:
    """Build ``{language_code: row}`` from the child-table rows.

    Keys are normalised to lower-case ISO codes.  Only rows with a non-empty
    language_code are included.
    """
    rows = getattr(settings, "hour_23_language_map", None) or []
    result: dict[str, Any] = {}
    for row in rows:
        code = (getattr(row, "language_code", None) or "").strip().lower()
        if code:
            result[code] = row
    return result


def _get_candidates(window_hours: int) -> list[dict]:
    """Return contacts whose latest inbound message is in the final window
    hour.

    Eligible range:
    ``window_hours - 1 ≤ hours_since_last_incoming < window_hours``

    Uses the latest incoming message per ``(from, whatsapp_account)`` as the
    anchor, consistent with how ``consent.py`` computes the service window.
    """
    now = now_datetime()
    upper_bound = add_to_date(now, hours=-(window_hours - 1))
    lower_bound = add_to_date(now, hours=-window_hours)

    return frappe.db.sql(  # type: ignore[return-value]
        """
        SELECT
            m.`from`           AS contact_number,
            m.whatsapp_account AS whatsapp_account,
            m.name             AS anchor_message,
            m.creation         AS anchor_time
        FROM `tabWhatsApp Message` m
        INNER JOIN (
            SELECT `from`, whatsapp_account, MAX(creation) AS max_creation
            FROM `tabWhatsApp Message`
            WHERE type = 'Incoming'
              AND `from` IS NOT NULL
              AND `from` != ''
            GROUP BY `from`, whatsapp_account
        ) latest
            ON  m.`from`           = latest.`from`
            AND m.whatsapp_account = latest.whatsapp_account
            AND m.creation         = latest.max_creation
        WHERE m.type = 'Incoming'
          AND m.creation >  %(lower_bound)s
          AND m.creation <= %(upper_bound)s
        """,
        {"lower_bound": lower_bound, "upper_bound": upper_bound},
        as_dict=True,
    )


def _get_sql_row_count() -> int:
    """Return the MySQL ROW_COUNT() for the last DML statement."""
    rows = list(frappe.db.sql("SELECT ROW_COUNT()", as_list=True) or [])
    if not rows:
        return 0
    first_row = rows[0]
    if not isinstance(first_row, (list, tuple)) or not first_row:
        return 0
    value = first_row[0]
    return 0 if value is None else int(value)


def _claim_anchor(
    anchor_message: str,
    whatsapp_account: str,
    contact_number: str,
    automation_type: str,
    template_name: str,
) -> str | None:
    """Atomically claim the anchor before sending.

    **Path 1 — fresh anchor:** INSERT a new log row with
    ``send_status='Pending'`` and a lease expiry, then commit.  Returns the
    new doc name.  Only one concurrent worker can win the unique
    ``anchor_message`` constraint.

    **Path 2 — stale re-claim:** If the INSERT hits ``UniqueValidationError``
    (the row already exists), attempt an atomic ``UPDATE … WHERE
    send_status='Pending' AND (claim_expires_at IS NULL OR claim_expires_at
    <= now)``.  This recovers rows left in Pending by a previously crashed
    worker whose lease has expired.  Returns the existing doc name when the
    UPDATE succeeds; returns ``None`` when another worker holds a live claim
    or the row is already ``Sent``.

    **Unexpected exceptions** (anything other than UniqueValidationError) are
    logged *and re-raised* so the caller's error handler records them rather
    than silently dropping them as if they were a benign lost race.
    """
    now_ts = now_datetime()
    claim_expires = add_to_date(now_ts, minutes=CLAIM_LEASE_MINUTES)

    # Path 1: Fresh INSERT claim.
    try:
        log_doc = frappe.get_doc(
            {
                "doctype": "WhatsApp Hour 23 Automation Log",
                "whatsapp_account": whatsapp_account,
                "contact_number": contact_number,
                "anchor_message": anchor_message,
                "automation_type": automation_type,
                "template": template_name,
                "send_status": "Pending",
                "claim_expires_at": claim_expires,
            }
        )
        log_doc.insert(ignore_permissions=True)
        frappe.db.commit()
        return str(log_doc.name)
    except frappe.exceptions.UniqueValidationError:
        frappe.db.rollback()
        # Row already exists — fall through to stale re-claim.
    except Exception:
        frappe.db.rollback()
        frappe.log_error(
            frappe.get_traceback(),
            _(
                "Hour-23 automation: unexpected error claiming anchor {0}"
            ).format(anchor_message),
        )
        raise

    # Path 2: Atomic stale re-claim.
    frappe.db.sql(
        "UPDATE `tabWhatsApp Hour 23 Automation Log`"
        "   SET `claim_expires_at` = %s"
        " WHERE `anchor_message` = %s"
        "   AND `send_status` = 'Pending'"
        "   AND (`claim_expires_at` IS NULL OR `claim_expires_at` <= %s)",
        [claim_expires, anchor_message, now_ts],
    )
    reclaimed = _get_sql_row_count()
    frappe.db.commit()

    if not reclaimed:
        # Another worker holds a live claim, or row is already Sent.
        return None

    log_name = frappe.db.get_value(
        "WhatsApp Hour 23 Automation Log",
        {"anchor_message": anchor_message, "send_status": "Pending"},
        "name",
    )
    return str(log_name) if log_name else None


def _resolve_template_row(lang_map: dict, detected_language: str | None):
    """Resolve the language-map row for this contact.

    Tries an exact match on the normalised ISO code first; falls back to the
    English (``en``) row if no match is found or the language is unset.
    Returns ``None`` only when the ``en`` row is also absent.
    """
    code = (detected_language or "").strip().lower()
    if code and code in lang_map:
        return lang_map[code]
    return lang_map.get("en")


def _has_marketing_consent(
        profile, marketing_consent_category: str | None) -> bool:
    """Return True if the contact has marketing consent.

    When a category is configured, checks the category-specific consent row;
    otherwise falls back to the master ``is_opted_in`` flag.
    """
    if marketing_consent_category:
        cat_consented = frappe.db.get_value(
            "WhatsApp Profile Consent",
            {
                "parent": profile.name,
                "consent_category": marketing_consent_category,
            },
            "consented",
        )
        return bool(cat_consented)
    return bool(getattr(profile, "is_opted_in", 0))


def _load_contact_state(
    contact_number: str,
    marketing_consent_category: str | None,
) -> tuple[str | None, bool] | None:
    """Load the contact's current profile state.

    Returns ``(detected_language, has_consent)`` when the contact may be
    messaged, or ``None`` when their profile is marked do-not-contact or
    opted-out.

    Used by **both** the primary send path and the recovery path so the two
    cannot drift: any change to DNC/consent logic applies everywhere.
    """
    number = format_number(contact_number)
    profile_rows = frappe.db.get_all(
        "WhatsApp Profiles",
        filters={"number": number},
        fields=["name", "do_not_contact", "is_opted_out", "is_opted_in",
                "detected_language"],
        limit=1,
    )

    if profile_rows:
        profile = frappe.get_doc("WhatsApp Profiles", profile_rows[0].name)
        if getattr(profile, "do_not_contact", 0) or getattr(
                profile, "is_opted_out", 0):
            return None
        detected_language: str | None = getattr(
            profile, "detected_language", None)
        has_consent = _has_marketing_consent(
            profile, marketing_consent_category)
    else:
        # No profile → unknown consent; treat as no-consent.
        detected_language = None
        has_consent = False

    return (detected_language, has_consent)


def _check_template_shape(
    template: Any,
    template_name: str,
    automation_type: str,
) -> str | None:
    """Validate that the template can be sent by this automation.

    Returns ``None`` when all checks pass, or a concise human-readable
    reason string when the template cannot be sent.  The caller is
    responsible for logging and deciding the consequence.

    Used by **both** the primary send path and the recovery path so the
    two cannot drift from each other.
    """
    status = (getattr(template, "status", "") or "").strip().upper()
    if status != "APPROVED":
        return _(
            "template '{0}' is not APPROVED (status: {1})"
        ).format(template_name, status or "Unknown")

    if automation_type == "consent_request" and not getattr(
        template, "is_consent_request", 0
    ):
        return _(
            "consent template '{0}' is not marked is_consent_request=1"
        ).format(template_name)

    if getattr(template, "sample_values", None) or getattr(
        template, "field_names", None
    ):
        return _(
            "template '{0}' requires body parameters which the automation "
            "cannot supply"
        ).format(template_name)

    if getattr(template, "header_type", None) in ("IMAGE", "DOCUMENT"):
        return _(
            "template '{0}' requires a media header which the automation "
            "cannot supply"
        ).format(template_name)

    for btn in getattr(template, "buttons", None) or []:
        if (getattr(btn, "button_type", "") == "Visit Website"
                and getattr(btn, "url_type", "") == "Dynamic"):
            return _(
                "template '{0}' has a dynamic URL button which the automation "
                "cannot supply"
            ).format(template_name)

    return None


def _mark_log_skipped(log_name: str) -> None:
    """Mark a log row as Skipped (terminal) and clear the claim lease.

    ``Skipped`` means a business-rule check failed during recovery and the
    row should never be retried.  ``claim_expires_at`` is cleared so the
    row no longer appears in the stale-claim scan.

    The commit is intentional: ``Skipped`` is a terminal state and must be
    crash-durable.  Without it, a worker crash after ``set_value`` but before
    the surrounding transaction commits would leave the row ``Pending``,
    causing ``recover_stale_hour_23_claims()`` to retry it indefinitely.
    """
    frappe.db.set_value(
        "WhatsApp Hour 23 Automation Log",
        log_name,
        {"send_status": "Skipped", "claim_expires_at": None},
    )
    frappe.db.commit()


def _post_claim_checks(
    log_name: str,
    current_automation_type: str,
    current_template_name: str,
    anchor_message: str,
    *,
    stored_automation_type: str | None = None,
    stored_template_name: str | None = None,
) -> bool:
    """Shared post-claim safety gate used by both ``_process_candidate`` and
    ``_retry_stale_claim``.

    Returns ``True`` when the caller should stop processing (either the log
    row has been finalised to ``Sent`` or has been marked ``Skipped``).
    Returns ``False`` when it is safe to proceed with the send.

    **Step A — reconciliation:**
    If a ``WhatsApp Message`` already exists for this log row (identified by
    ``reference_name == log_name``), the message was delivered in a prior
    attempt but the log was never finalised (Phase-2-committed /
    Phase-3-not-committed crash).  The log row is updated to ``Sent`` and
    the function returns ``True`` so no duplicate is sent.

    **Step B — stored-metadata consistency:**
    The log row's stored ``automation_type`` / ``template`` are compared
    against the caller's current decision.  A mismatch means the template or
    consent type changed between the original claim and this attempt; sending
    the stale combination would create an inconsistent audit trail.
    The row is marked ``Skipped`` and ``True`` is returned.

    *Recovery path* passes
    ``stored_automation_type`` / ``stored_template_name``
    from the already-loaded stale row dict — no extra DB query.

    *Primary path* omits those kwargs; the helper reads the stored values
    from the DB.  If the row is not found (only happens in test environments
    where ``_claim_anchor`` is mocked), the check is skipped defensively so
    existing tests are not broken.

    Note: because this check runs *after* ``_load_contact_state()`` in the
    recovery path, a contact who became DNC/opted-out between a prior send
    (Phase 2) and the recovery run will be caught by the DNC check and marked
    ``Skipped`` rather than being reconciled to ``Sent``.  This is an
    accepted edge-case trade-off for the cleaner shared-helper design; the
    duplicate is not re-sent in either case.
    """
    # Step A: reconcile if already sent.
    if _reconcile_if_already_sent(log_name):
        return True

    # Step B: verify stored metadata matches the current decision.
    if stored_automation_type is None or stored_template_name is None:
        stored_automation_type = cast(str | None, frappe.db.get_value(
            "WhatsApp Hour 23 Automation Log", log_name, "automation_type"
        ))
        stored_template_name = cast(str | None, frappe.db.get_value(
            "WhatsApp Hour 23 Automation Log", log_name, "template"
        ))

    # stored_* are still None when the row is not in the DB (test
    # environments where _claim_anchor is mocked).  Skip the check rather
    # than false-positive-skipping real sends.
    if stored_automation_type is None or stored_template_name is None:
        return False

    if (stored_automation_type != current_automation_type
            or stored_template_name != current_template_name):
        frappe.log_error(
            _(
                "Hour-23 automation: stored template/type ({0}/{1}) does not "
                "match current decision ({2}/{3}) for log {4}. "
                "Skipping anchor {5}."
            ).format(
                stored_automation_type, stored_template_name,
                current_automation_type, current_template_name,
                log_name, anchor_message,
            ),
            "WhatsApp Hour-23 Automation",
        )
        _mark_log_skipped(log_name)
        return True

    return False


def _reconcile_if_already_sent(log_name: str) -> bool:
    """Detect and recover the Phase-2-committed / Phase-3-not-committed seam.

    Every automated outbound ``WhatsApp Message`` is created with
    ``reference_doctype="WhatsApp Hour 23 Automation Log"`` and
    ``reference_name=log_name``.  An explicit ``frappe.db.commit()`` is
    issued immediately after ``msg_doc.insert()`` (Phase 2) so that the
    outbound row is durable before the log is finalised (Phase 3).

    If a worker crashed or the transaction rolled back between Phase 2 and
    Phase 3, the outbound message will exist in the DB but the log row will
    still be ``Pending``.  This function detects that state, finalises the
    log to ``Sent``, and returns ``True`` so the caller skips a re-send.

    Returns ``False`` when no prior outbound message is found (normal path:
    proceed with the send).

    Note: a crash between the Meta API call (inside ``before_insert()``) and
    Phase 2's ``frappe.db.commit()`` cannot be detected here — the outbound
    message row does not yet exist.  That narrow window is undocumented
    at-most-once vs. at-least-once ambiguity inherent in the WhatsApp
    Message controller design.
    """
    _filter = {
        "reference_doctype": "WhatsApp Hour 23 Automation Log",
        "reference_name": log_name,
        "type": "Outgoing",
    }
    msg_name = cast(str | None, frappe.db.get_value(
        "WhatsApp Message", _filter, "name"
    ))
    if not msg_name:
        return False
    msg_creation = cast(str | None, frappe.db.get_value(
        "WhatsApp Message", msg_name, "creation"
    ))

    frappe.db.set_value(
        "WhatsApp Hour 23 Automation Log",
        log_name,
        {
            "outgoing_message": msg_name,
            "sent_at": msg_creation,
            "send_status": "Sent",
            "claim_expires_at": None,
        },
    )
    frappe.db.commit()
    return True


def _process_candidate(
    *,
    candidate: dict,
    lang_map: dict,
    marketing_consent_category: str | None,
) -> None:
    """Process a single candidate contact.

    Decision flow:
    1. Load profile — skip if DNC or opted-out.
    2. Resolve language-map row — log and skip if none found.
    3. Select template based on marketing-consent status.
    4. Validate template (approved, parameterless, no media header).
    5. Claim idempotency — insert log row before sending; skip if another
       worker already claimed this anchor (unique constraint on
       anchor_message).
    5a. Post-claim safety gate (shared with recovery): reconcile if a prior
       send already exists for this log row; verify stored metadata matches
       current decision (guards against reclaimed-row drift).
    6. Create outgoing WhatsApp Message (normal insert/send path).
    7. Update log row with outgoing_message and sent_at.
    """
    contact_number: str = candidate["contact_number"]
    whatsapp_account: str = candidate["whatsapp_account"]
    anchor_message: str = candidate["anchor_message"]

    # 1. Profile / safety check
    contact_state = _load_contact_state(
        contact_number, marketing_consent_category)
    if contact_state is None:
        return
    detected_language, has_consent = contact_state

    # 2. Resolve language row
    template_row = _resolve_template_row(lang_map, detected_language)
    if not template_row:
        frappe.log_error(
            _(
                "Hour-23 automation: no language mapping (not even an English "
                "fallback) for contact {0}. Skipping."
            ).format(contact_number),
            "WhatsApp Hour-23 Automation",
        )
        return

    # 3. Select template
    if has_consent:
        template_name = getattr(template_row,
                                "status_follow_up_template", None)
        automation_type = "status_follow_up"
    else:
        template_name = getattr(template_row, "consent_template", None)
        automation_type = "consent_request"

    if not template_name:
        frappe.log_error(
            _(
                "Hour-23 automation: no template configured for language "
                "'{0}' / type '{1}' (contact: {2}). Skipping."
            ).format(detected_language or "en",
                     automation_type, contact_number),
            "WhatsApp Hour-23 Automation",
        )
        return

    # 4. Validate template shape
    template = frappe.get_doc("WhatsApp Templates", template_name)
    shape_error = _check_template_shape(template,
                                        template_name, automation_type)
    if shape_error:
        frappe.log_error(
            _(
                "Hour-23 automation: {0}. Skipping contact {1}."
            ).format(shape_error, contact_number),
            "WhatsApp Hour-23 Automation",
        )
        return

    # 5. Claim idempotency — insert log row before sending.
    # Only one concurrent worker can win; the rest get UniqueValidationError
    # on the anchor_message unique index and return None here.
    log_name = _claim_anchor(
        anchor_message=anchor_message,
        whatsapp_account=whatsapp_account,
        contact_number=contact_number,
        automation_type=automation_type,
        template_name=template_name,
    )
    if log_name is None:
        return

    # 5a. Post-claim safety: reconcile if already sent; verify stored metadata
    # matches the current decision (guards against reclaimed-row drift).
    if _post_claim_checks(log_name, automation_type,
                          template_name, anchor_message):
        return

    # 6. Send via normal WhatsApp Message insert/send path.
    # reference_doctype/reference_name link the outbound message back to this
    # log row so that _reconcile_if_already_sent() can detect a prior send if
    # the process crashes before Phase 3 (log finalisation).
    # These fields are safe to set even though send_template() reads them for
    # template parameters: that code path is guarded by template.sample_values
    # which _check_template_shape() already rejects above.
    msg_doc = frappe.get_doc(
        {
            "doctype": "WhatsApp Message",
            "type": "Outgoing",
            "use_template": 1,
            "message_type": "Template",
            "content_type": "text",
            "template": template_name,
            "to": contact_number,
            "whatsapp_account": whatsapp_account,
            "reference_doctype": "WhatsApp Hour 23 Automation Log",
            "reference_name": log_name,
        }
    )
    msg_doc.insert(ignore_permissions=True)
    # Phase 2 commit: make the outbound message row durable before updating
    # the log.  If the process crashes after this point, recovery will find
    # the existing WhatsApp Message via reference_name and reconcile the log
    # to Sent without re-sending (see _reconcile_if_already_sent).
    frappe.db.commit()

    # 7. Mark log row as Sent and clear the lease.
    frappe.db.set_value(
        "WhatsApp Hour 23 Automation Log",
        log_name,
        {
            "outgoing_message": msg_doc.name,
            "sent_at": now_datetime(),
            "send_status": "Sent",
            "claim_expires_at": None,
        },
    )


# ── Recovery ────────────────────────────────────────────────────────────────


def recover_stale_hour_23_claims() -> None:
    """Hourly recovery: re-attempt sends for claimed-but-unsent log rows.

    A row is *stale* when ``send_status='Pending'`` and
    ``claim_expires_at`` is in the past (or NULL), meaning the worker that
    claimed it crashed or failed between the commit and ``msg_doc.insert()``.

    For each stale row, ``_retry_stale_claim()`` atomically re-claims it and
    re-evaluates current eligibility before re-sending.  Business-rule
    failures mark the row ``Skipped`` (terminal).  Transient send failures
    propagate as exceptions, are logged here, and leave the row ``Pending``
    so the next hourly run retries.
    """
    settings = frappe.get_cached_doc("WhatsApp Compliance Settings")
    if not getattr(settings, "enable_hour_23_follow_up", 0):
        return

    marketing_consent_category = (
        getattr(settings, "marketing_consent_category", None) or None
    )
    lang_map = _build_language_map(settings)

    now = now_datetime()
    stale = frappe.db.get_all(
        "WhatsApp Hour 23 Automation Log",
        filters={"send_status": "Pending"},
        fields=[
            "name",
            "anchor_message",
            "whatsapp_account",
            "contact_number",
            "automation_type",
            "template",
            "claim_expires_at",
        ],
    )

    for row in stale:
        expires = row.get("claim_expires_at")
        if expires and expires > now:
            continue  # Live claim — another worker is in-flight, skip.
        try:
            _retry_stale_claim(
                row,
                marketing_consent_category=marketing_consent_category,
                lang_map=lang_map,
            )
        except Exception:
            frappe.log_error(
                frappe.get_traceback(),
                _(
                    "Hour-23 recovery error for anchor {0}"
                ).format(row.get("anchor_message", "unknown")),
            )


def _retry_stale_claim(
    row: dict,
    *,
    marketing_consent_category: str | None,
    lang_map: dict,
) -> None:
    """Re-attempt send for a single stale Pending log row.

    Decision flow mirrors ``_process_candidate`` to prevent the two paths
    from diverging:

    1. Atomically re-claim the row via ``_claim_anchor()``.  Returns early
       (no log update) if another worker already holds the lease.
    1a. **Reconciliation first** — before any current-state checks, query for
       a ``WhatsApp Message`` already linked to this log row via
       ``reference_name``.  If one exists, a prior Phase-2 commit succeeded
       but Phase 3 (log finalisation) did not.  Finalise the log to ``Sent``
       with the existing message and return immediately.  This step MUST
       precede all eligibility checks: a contact who became DNC between
       Phase 2 and this recovery run should still have the log row recorded
       as ``Sent`` (not ``Skipped``), and the ``outgoing_message`` link must
       be set so ``_maybe_do_category_opt_in`` can find it on a YES reply.
    2. Re-check profile eligibility via ``_load_contact_state()``.  If the
       contact is now DNC or opted-out → ``Skipped`` (terminal).
    3. Recompute the expected ``automation_type`` / ``template_name`` from the
       *current* language map and contact language — the same logic as
       ``_process_candidate``.  If there is no mapping or no template, mark
       ``Skipped``.
    3a. Post-claim safety gate (shared with ``_process_candidate``): Step A
       (reconcile) is a no-op here because step 1a already ran; Step B
       verifies the recomputed type/template matches stored values.  Passes
       stored values directly so no extra DB query.
    4. Re-validate template shape via ``_check_template_shape()``.  Any
       failure (not APPROVED, params, media, dynamic URL) → ``Skipped``.
    5. Send.  Transient failures (``msg_doc.insert()`` raises) propagate to
       the caller so the row remains ``Pending`` and retries on the next run.
    6. Mark ``Sent`` and clear the lease.
    """
    anchor_message: str = row["anchor_message"]
    whatsapp_account: str = row["whatsapp_account"]
    contact_number: str = row["contact_number"]
    automation_type: str = row["automation_type"]
    template_name: str = row["template"]

    # 1. Atomic re-claim.
    log_name = _claim_anchor(
        anchor_message=anchor_message,
        whatsapp_account=whatsapp_account,
        contact_number=contact_number,
        automation_type=automation_type,
        template_name=template_name,
    )
    if log_name is None:
        return  # Another worker holds the claim, or row is already Sent.

    # 1a. Reconciliation — must run before any current-state eligibility check.
    # If Phase 2 committed the outbound message but Phase 3 (log finalisation)
    # did not complete, finalise here and exit.  Placing this before the DNC
    # and mapping checks ensures a previously-sent message always records as
    # Sent (not Skipped) and that outgoing_message is set for
    # _maybe_do_category_opt_in lookups on YES replies.
    if _reconcile_if_already_sent(log_name):
        return

    # 2. Re-check current profile / safety state.
    contact_state = _load_contact_state(contact_number,
                                        marketing_consent_category)
    if contact_state is None:
        frappe.log_error(
            _(
                "Hour-23 recovery: contact {0} is now DNC or opted-out. "
                "Skipping anchor {1}."
            ).format(contact_number, anchor_message),
            "WhatsApp Hour-23 Automation",
        )
        _mark_log_skipped(log_name)
        return

    detected_language, has_consent = contact_state

    # 3. Recompute the expected template using the current language map.
    # This catches contact language changes and lang-map reconfiguration.
    template_row = _resolve_template_row(lang_map, detected_language)
    if not template_row:
        frappe.log_error(
            _(
                "Hour-23 recovery: no language mapping for contact {0} "
                "(language: {1}). Skipping anchor {2}."
            ).format(contact_number, detected_language or "unknown",
                     anchor_message),
            "WhatsApp Hour-23 Automation",
        )
        _mark_log_skipped(log_name)
        return

    if has_consent:
        current_template_name = getattr(template_row,
                                        "status_follow_up_template", None)
        current_automation_type = "status_follow_up"
    else:
        current_template_name = getattr(template_row, "consent_template", None)
        current_automation_type = "consent_request"

    if not current_template_name:
        frappe.log_error(
            _(
                "Hour-23 recovery: no template configured for language "
                "'{0}' / type '{1}' (contact: {2}). Skipping anchor {3}."
            ).format(
                detected_language or "en", current_automation_type,
                contact_number, anchor_message,
            ),
            "WhatsApp Hour-23 Automation",
        )
        _mark_log_skipped(log_name)
        return

    # 3a. Post-claim safety gate: reconcile if already sent; verify stored
    # metadata matches the recomputed decision.  Passes stored_* directly so
    # the helper skips the extra DB read it would otherwise make.
    if _post_claim_checks(
        log_name,
        current_automation_type,
        current_template_name,
        anchor_message,
        stored_automation_type=automation_type,
        stored_template_name=template_name,
    ):
        return

    # 4. Re-validate template shape.
    template = frappe.get_doc("WhatsApp Templates", current_template_name)
    shape_error = _check_template_shape(template, current_template_name,
                                        current_automation_type)
    if shape_error:
        frappe.log_error(
            _(
                "Hour-23 recovery: {0}. Skipping anchor {1}."
            ).format(shape_error, anchor_message),
            "WhatsApp Hour-23 Automation",
        )
        _mark_log_skipped(log_name)
        return

    # 5. Send.  Transient failure here propagates to the caller; the row
    #    stays Pending with the current lease and is retried next run.
    msg_doc = frappe.get_doc(
        {
            "doctype": "WhatsApp Message",
            "type": "Outgoing",
            "use_template": 1,
            "message_type": "Template",
            "content_type": "text",
            "template": current_template_name,
            "to": contact_number,
            "whatsapp_account": whatsapp_account,
            "reference_doctype": "WhatsApp Hour 23 Automation Log",
            "reference_name": log_name,
        }
    )
    msg_doc.insert(ignore_permissions=True)
    # Phase 2 commit: durable before log finalisation (see
    # _reconcile_if_already_sent).
    frappe.db.commit()

    # 6. Mark Sent.
    frappe.db.set_value(
        "WhatsApp Hour 23 Automation Log",
        log_name,
        {
            "outgoing_message": msg_doc.name,
            "sent_at": now_datetime(),
            "send_status": "Sent",
            "claim_expires_at": None,
        },
    )
