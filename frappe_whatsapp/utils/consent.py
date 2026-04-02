"""WhatsApp Business Policy consent management.

Handles opt-out/opt-in keyword detection, profile consent updates,
audit logging, and confirmation message sending.
"""
import frappe
from frappe import _
from frappe.utils import now_datetime, time_diff_in_hours
from typing import Any, cast

from frappe_whatsapp.utils import format_number


def get_compliance_settings() -> Any:
    """Load the singleton WhatsApp Compliance Settings document (cached)."""
    return frappe.get_cached_doc("WhatsApp Compliance Settings")


def get_opt_out_keywords(
        whatsapp_account: str | None = None) -> list[dict[str, Any]]:
    """Return enabled opt-out keywords, optionally filtered by account.

    Each item has: keyword, case_sensitive, match_type, action,
    target_category.
    """
    filters: dict[str, Any] = {"is_enabled": 1}
    if whatsapp_account:
        filters["whatsapp_account"] = ("in", ["", whatsapp_account])

    return frappe.get_all(
        "WhatsApp Opt Out Keyword",
        filters=filters,
        fields=[
            "keyword", "case_sensitive", "match_type",
            "action", "target_category"],
    )


def check_opt_out_keyword(
        message_text: str,
        whatsapp_account: str | None = None,
) -> dict[str, Any] | None:
    """Check whether *message_text* matches any opt-out keyword.

    Returns the first matching keyword row (as dict), or None.
    """
    if not message_text:
        return None

    settings = get_compliance_settings()
    if not settings.enable_opt_out_detection:
        return None

    keywords = get_opt_out_keywords(whatsapp_account)

    for kw in keywords:
        keyword: str = kw["keyword"]
        text = message_text
        if not kw.get("case_sensitive"):
            keyword = keyword.lower()
            text = text.lower()

        text = text.strip()

        match_type = kw.get("match_type", "Exact")
        matched = False
        if match_type == "Exact":
            matched = text == keyword
        elif match_type == "Contains":
            matched = keyword in text
        elif match_type == "Starts With":
            matched = text.startswith(keyword)

        if matched:
            return kw

    return None


def check_opt_in_keyword(message_text: str) -> bool:
    """Check whether *message_text* matches any opt-in keyword."""
    if not message_text:
        return False

    settings = get_compliance_settings()
    if not settings.enable_opt_in_detection:
        return False

    raw = settings.opt_in_keywords or ""
    opt_in_words = [w.strip().lower() for w in raw.split(",") if w.strip()]
    return message_text.strip().lower() in opt_in_words


# ── Consent verification before sending ──────────────────────────────

class ConsentResult:
    """Result of a consent check before sending."""
    __slots__ = ("allowed", "status", "reason")

    def __init__(
            self, allowed: bool, status: str, reason: str = "") -> None:
        self.allowed = allowed
        self.status = status    # "Opted In" | "Opted Out" | "Unknown"
        # | "Bypassed"
        self.reason = reason


def verify_consent_for_send(
        phone_number: str,
        *,
        consent_category: str | None = None,
        is_transactional: bool = False,
        is_consent_request: bool = False,
        service_window_active: bool = False,
) -> ConsentResult:
    """Check whether we are allowed to send a message to *phone_number*.

    Checks (in order):
    1. Whether enforcement is enabled at all.
    2. Whether the profile has do_not_contact set (always blocks).
    3. Whether the profile is opted out.
    4. Optionally, whether category-level consent exists.
    5. Whether this is a consent-request template.
    6. Whether transactional messages bypass consent.
    7. Whether an active 24-hour service window permits a bypass
       (unknown/no-profile only; DNC and explicit opt-out always block).

    Returns a ConsentResult with allowed=True/False and a status string
    suitable for storing in WhatsApp Message.consent_status_at_send.
    """
    settings = get_compliance_settings()

    # Enforcement disabled → always allow
    if settings.consent_check_mode == "Disabled":
        return ConsentResult(True, "Bypassed", "Consent check disabled")

    if not settings.enforce_consent_check:
        return ConsentResult(True, "Bypassed", "Consent enforcement off")

    number = format_number(phone_number)
    if not number:
        return ConsentResult(True, "Unknown", "No phone number")

    profile = frappe.db.get_all(
        "WhatsApp Profiles",
        filters={"number": number},
        fields=["name", "do_not_contact", "is_opted_out", "is_opted_in"],
        limit=1,
    )

    # No profile exists → treat as Unknown
    if not profile:
        # Consent-request templates may be sent to collect opt-in.
        if is_consent_request:
            return ConsentResult(
                True, "Bypassed", "Consent request template")
        # Transactional bypass
        if is_transactional and settings.allow_transactional_without_consent:
            return ConsentResult(True, "Bypassed", "Transactional bypass")
        if settings.consent_check_mode == "Warning Only":
            return ConsentResult(True, "Unknown", "No profile found")
        # Active service window: user initiated the conversation; allow reply
        # even without a recorded opt-in.
        if service_window_active:
            return ConsentResult(
                True, "Bypassed", "Service window bypass (no profile)")
        return ConsentResult(
            False, "Unknown", "No consent profile found for this number")

    from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_profiles.whatsapp_profiles import WhatsAppProfiles  # noqa: E501
    profile = cast(
        WhatsAppProfiles,
        frappe.get_doc("WhatsApp Profiles", profile[0].name))

    # Hard block: do_not_contact always prevents sending
    if profile.do_not_contact:
        return ConsentResult(
            False, "Opted Out", "Contact is marked Do Not Contact")

    # Opted out at profile level
    if profile.is_opted_out:
        return ConsentResult(
            False, "Opted Out", "Contact has opted out")

    # Category-level check (if a category is specified)
    if consent_category and profile.name and not is_consent_request:
        cat_consented = frappe.db.get_value(
            "WhatsApp Profile Consent",
            {"parent": profile.name, "consent_category": consent_category},
            "consented",
        )
        if cat_consented is not None and not cat_consented:
            return ConsentResult(
                False, "Opted Out",
                f"Contact opted out of category: {consent_category}")

    # Explicitly opted in
    if profile.is_opted_in:
        return ConsentResult(True, "Opted In", "")

    # Consent-request templates may be sent to contacts with unknown status
    # (but never to opted-out / DNC contacts).
    if is_consent_request:
        return ConsentResult(
            True, "Bypassed", "Consent request template")

    # Profile exists but consent status is Unknown/Partial
    if is_transactional and settings.allow_transactional_without_consent:
        return ConsentResult(True, "Bypassed", "Transactional bypass")

    if settings.consent_check_mode == "Warning Only":
        return ConsentResult(True, "Unknown", "Consent not confirmed")

    # Active service window: user initiated; reply allowed without opt-in.
    # DNC and explicit opt-out already returned above so this is safe.
    if service_window_active:
        return ConsentResult(
            True, "Bypassed", "Service window bypass")

    # Strict mode: no explicit opt-in → block
    return ConsentResult(
        False, "Unknown", "Contact has not opted in")


# ── 24-hour conversation window ──────────────────────────────────────

def _check_actual_service_window(
        phone_number: str,
        whatsapp_account: str | None = None,
) -> tuple[bool, str]:
    """Query whether there is a real inbound message within the window.

    This helper always hits the database.  It does NOT respect the
    ``enforce_24_hour_window`` toggle — that toggle is purely about
    *enforcement* (whether to block free-form messages outside the window),
    not about whether an actual service window exists for consent purposes.

    Returns (is_within_window, reason).
    """
    number = format_number(phone_number)
    if not number:
        return False, "No phone number"

    settings = get_compliance_settings()
    window_hours = int(settings.window_hours or 24)

    filters: dict[str, Any] = {
        "type": "Incoming",
        "from": number,
    }
    if whatsapp_account:
        filters["whatsapp_account"] = whatsapp_account

    last_incoming = frappe.get_all(
        "WhatsApp Message",
        filters=filters,
        fields=["creation"],
        order_by="creation desc",
        limit=1,
    )

    if not last_incoming:
        return False, "No incoming message found from this contact"

    last_msg_time = last_incoming[0].creation
    hours_since = time_diff_in_hours(now_datetime(), last_msg_time)

    if hours_since <= window_hours:
        return True, ""

    return (
        False,
        f"Last incoming message was {hours_since:.1f}h ago"
        f" (window: {window_hours}h)",
    )


def get_service_window_status(
        phone_number: str,
        whatsapp_account: str | None = None,
) -> tuple[bool, str]:
    """Return whether an active user-initiated service window exists.

    An active service window means the contact sent an inbound message
    within the last ``window_hours`` hours (default 24).  This is used
    as the signal for the consent bypass: when the user initiated the
    conversation we may reply even without a recorded opt-in.

    Unlike ``is_within_conversation_window``, this function **ignores**
    the ``enforce_24_hour_window`` toggle.  That toggle controls whether
    *free-form message sending is blocked* outside the window; it has no
    bearing on whether a real service window exists for consent purposes.
    Disabling enforcement must not fabricate a phantom service window.

    Returns (service_window_active, reason).
    """
    return _check_actual_service_window(phone_number, whatsapp_account)


def is_within_conversation_window(
        phone_number: str,
        whatsapp_account: str | None = None,
) -> tuple[bool, str]:
    """Check the enforcement-aware 24-hour conversation window.

    WhatsApp requires that free-form (non-template) messages can only be
    sent within 24 hours of the last incoming message from the contact.

    When ``enforce_24_hour_window`` is disabled in Compliance Settings,
    this returns ``True`` without a DB query (enforcement is suppressed).
    For the consent-bypass signal, use ``get_service_window_status``
    instead — it always checks for a real inbound message.

    Returns (is_within_window, reason).
    """
    settings = get_compliance_settings()

    if not settings.enforce_24_hour_window:
        return True, "24-hour window enforcement disabled"

    return _check_actual_service_window(phone_number, whatsapp_account)


# ── Profile updates ──────────────────────────────────────────────────

def _get_or_create_profile(
        contact_number: str,
        whatsapp_account: str,
        profile_name: str | None = None) -> str:
    """Return the WhatsApp Profiles *name* for a contact, creating one if
    needed."""
    number = format_number(contact_number)
    profile_name_id = frappe.db.get_value(
        "WhatsApp Profiles", {"number": number}, "name")

    if profile_name_id:
        return str(profile_name_id)

    doc = frappe.get_doc({
        "doctype": "WhatsApp Profiles",
        "number": number,
        "profile_name": profile_name,
        "whatsapp_account": whatsapp_account,
    })
    doc.insert(ignore_permissions=True)
    return str(doc.name)


def process_opt_out(
        *,
        contact_number: str,
        whatsapp_account: str,
        message_doc_name: str | None = None,
        keyword_match: dict[str, Any] | None = None,
        profile_name: str | None = None,
) -> None:
    """Mark a contact as opted-out and create an audit log entry."""
    profile_id = _get_or_create_profile(
        contact_number, whatsapp_account, profile_name)
    from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_profiles.whatsapp_profiles import WhatsAppProfiles  # noqa: E501
    profile = cast(
        WhatsAppProfiles,
        frappe.get_doc("WhatsApp Profiles", profile_id))

    previous_opted_out = bool(profile.is_opted_out)

    action = (keyword_match or {}).get("action", "Full Opt-Out")
    target_category = (keyword_match or {}).get("target_category")

    if action == "Category Opt-Out" and target_category:
        _category_opt_out(profile, target_category, message_doc_name)
    else:
        profile.is_opted_out = 1
        profile.is_opted_in = 0
        profile.opted_out_at = now_datetime()
        profile.opted_out_source = "Keyword"
        profile.opted_out_reason = (
            f"Keyword: {(keyword_match or {}).get('keyword', 'unknown')}")
        profile.consent_status = "Opted Out"
        profile.save(ignore_permissions=True)

        _log_consent(
            profile=profile_id,
            phone_number=format_number(contact_number),
            action_type="Opt-Out",
            previous_status=previous_opted_out,
            new_status=True,
            source="Webhook",
            source_message=message_doc_name,
        )

    # Mark the incoming message as an opt-out request
    if message_doc_name:
        frappe.db.set_value(
            "WhatsApp Message", message_doc_name,
            "is_opt_out_request", 1)


def _category_opt_out(
        profile, target_category: str,
        message_doc_name: str | None) -> None:
    """Opt-out a profile from a specific consent category."""
    for row in (profile.get("category_consents") or []):
        if row.consent_category == target_category:
            row.consented = 0
            row.consented_at = now_datetime()
            break

    # Check if all categories are now opted out
    all_out = all(
        not row.consented
        for row in (profile.get("category_consents") or []))

    if all_out:
        profile.consent_status = "Opted Out"
        profile.is_opted_out = 1
        profile.is_opted_in = 0
    else:
        profile.consent_status = "Partial"

    profile.save(ignore_permissions=True)

    _log_consent(
        profile=str(profile.name),
        phone_number=profile.number,
        action_type="Category Opt-Out",
        consent_category=target_category,
        previous_status=True,
        new_status=False,
        source="Webhook",
        source_message=message_doc_name,
    )


def process_opt_in(
        *,
        contact_number: str,
        whatsapp_account: str,
        message_doc_name: str | None = None,
        profile_name: str | None = None,
) -> None:
    """Mark a contact as opted-in and create an audit log entry."""
    profile_id = _get_or_create_profile(
        contact_number, whatsapp_account, profile_name)
    from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_profiles.whatsapp_profiles import WhatsAppProfiles  # noqa: E501
    profile = cast(
        WhatsAppProfiles,
        frappe.get_doc("WhatsApp Profiles", profile_id))

    previous_opted_in = bool(profile.is_opted_in)

    profile.is_opted_in = 1
    profile.is_opted_out = 0
    profile.opted_in_at = now_datetime()
    profile.opted_in_method = "WhatsApp Reply"
    profile.consent_status = "Opted In"
    # Clear opt-out fields
    profile.opted_out_at = None
    profile.opted_out_reason = None
    profile.opted_out_source = ""
    profile.save(ignore_permissions=True)

    _log_consent(
        profile=profile_id,
        phone_number=format_number(contact_number),
        action_type="Opt-In",
        previous_status=previous_opted_in,
        new_status=True,
        source="Webhook",
        source_message=message_doc_name,
    )

    # Mark the incoming message as an opt-in request
    if message_doc_name:
        frappe.db.set_value(
            "WhatsApp Message", message_doc_name,
            "is_opt_in_request", 1)


# ── Confirmation messages ────────────────────────────────────────────

def send_opt_out_confirmation(
        *, contact_number: str, whatsapp_account_name: str) -> None:
    """Send a confirmation text message after opt-out."""
    settings = get_compliance_settings()
    if not settings.send_opt_out_confirmation:
        return

    if settings.opt_out_confirmation_template:
        _send_template_confirmation(
            to=contact_number,
            template_name=str(settings.opt_out_confirmation_template),
            whatsapp_account_name=whatsapp_account_name,
        )
        return

    message_text = settings.opt_out_confirmation_message
    if not message_text:
        return

    _send_plain_text(
        to=contact_number,
        message=message_text,
        whatsapp_account_name=whatsapp_account_name,
    )


def send_opt_in_confirmation(
        *, contact_number: str, whatsapp_account_name: str) -> None:
    """Send a confirmation text message after opt-in."""
    settings = get_compliance_settings()
    if not settings.send_opt_in_confirmation:
        return

    message_text = settings.opt_in_confirmation_message
    if not message_text:
        return

    _send_plain_text(
        to=contact_number,
        message=message_text,
        whatsapp_account_name=whatsapp_account_name,
    )


def _send_plain_text(
        *, to: str, message: str, whatsapp_account_name: str) -> None:
    """Send a plain text WhatsApp message via the API directly.

    We bypass WhatsApp Message doc creation to avoid triggering consent
    checks on the confirmation itself.
    """
    import json
    from frappe.integrations.utils import make_post_request
    from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_account.whatsapp_account import WhatsAppAccount  # noqa: E501
    from typing import cast

    wa = cast(
        WhatsAppAccount,
        frappe.get_doc("WhatsApp Account", whatsapp_account_name))

    token = wa.get_password("token")
    data = {
        "messaging_product": "whatsapp",
        "to": format_number(to),
        "type": "text",
        "text": {"body": message},
    }
    headers = {
        "authorization": f"Bearer {token}",
        "content-type": "application/json",
    }

    try:
        make_post_request(
            f"{wa.url}/{wa.version}/{wa.phone_id}/messages",
            headers=headers,
            data=json.dumps(data),
        )
    except Exception:
        frappe.log_error(
            frappe.get_traceback(),
            _("Failed to send opt-out/opt-in confirmation to {0}").format(to))


def _send_template_confirmation(
        *, to: str, template_name: str,
        whatsapp_account_name: str) -> None:
    """Send a template confirmation message without consent checks."""
    import json
    from frappe.integrations.utils import make_post_request
    from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_account.whatsapp_account import WhatsAppAccount  # noqa: E501
    from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_templates.whatsapp_templates import WhatsAppTemplates  # noqa: E501
    from typing import cast

    wa = cast(
        WhatsAppAccount,
        frappe.get_doc("WhatsApp Account", whatsapp_account_name))

    template = cast(
        WhatsAppTemplates,
        frappe.get_doc("WhatsApp Templates", template_name))

    status = (getattr(template, "status", "") or "").strip().upper()
    if status != "APPROVED":
        frappe.throw(
            _("Opt-out confirmation template is not approved "
              "(status: {0}).").format(
                getattr(template, "status", "") or "Unknown"
            )
        )

    if template.sample_values or template.field_names:
        frappe.throw(
            _("Opt-out confirmation template must not require parameters."))

    if template.header_type in ("IMAGE", "DOCUMENT"):
        frappe.throw(
            _("Opt-out confirmation template must not require media headers.")
        )

    token = wa.get_password("token")
    data = {
        "messaging_product": "whatsapp",
        "to": format_number(to),
        "type": "template",
        "template": {
            "name": template.actual_name or template.template_name,
            "language": {"code": template.language_code},
            "components": [],
        },
    }

    headers = {
        "authorization": f"Bearer {token}",
        "content-type": "application/json",
    }

    try:
        make_post_request(
            f"{wa.url}/{wa.version}/{wa.phone_id}/messages",
            headers=headers,
            data=json.dumps(data),
        )
    except Exception:
        frappe.log_error(
            frappe.get_traceback(),
            _("Failed to send opt-out confirmation template"
              " to {0}").format(to))


# ── Audit log ────────────────────────────────────────────────────────

def _log_consent(
        *,
        profile: str,
        phone_number: str,
        action_type: str,
        previous_status: bool,
        new_status: bool,
        source: str,
        source_message: str | None = None,
        consent_category: str | None = None,
) -> None:
    """Create a WhatsApp Consent Log entry."""
    frappe.get_doc({
        "doctype": "WhatsApp Consent Log",
        "profile": profile,
        "phone_number": phone_number,
        "action": action_type,
        "consent_category": consent_category,
        "previous_status": int(previous_status),
        "new_status": int(new_status),
        "source": source,
        "source_message": source_message,
        "user": frappe.session.user,
        "timestamp": now_datetime(),
    }).insert(ignore_permissions=True)


def process_category_opt_in(
        *,
        contact_number: str,
        whatsapp_account: str,
        consent_category: str,
        message_doc_name: str | None = None,
        profile_name: str | None = None,
) -> None:
    """Grant category-level consent and create an audit log entry.

    Finds or creates the ``WhatsApp Profile Consent`` row for
    ``consent_category`` and marks it consented.  Updates the overall
    ``consent_status`` to "Partial" when the profile does not yet have
    full master opt-in so downstream code can distinguish the two states.
    """
    profile_id = _get_or_create_profile(
        contact_number, whatsapp_account, profile_name)
    from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_profiles.whatsapp_profiles import WhatsAppProfiles  # noqa: E501
    profile = cast(
        WhatsAppProfiles,
        frappe.get_doc("WhatsApp Profiles", profile_id))

    category_row = None
    for row in (profile.get("category_consents") or []):
        if row.consent_category == consent_category:
            category_row = row
            break

    previous_status = False
    if category_row:
        previous_status = bool(category_row.consented)
        category_row.consented = 1
        category_row.consented_at = now_datetime()
        category_row.consent_method = "WhatsApp Reply"
    else:
        profile.append(
            "category_consents",
            {
                "consent_category": consent_category,
                "consented": 1,
                "consented_at": now_datetime(),
                "consent_method": "WhatsApp Reply",
            },
        )

    if not profile.is_opted_in:
        profile.consent_status = "Partial"

    profile.save(ignore_permissions=True)

    _log_consent(
        profile=profile_id,
        phone_number=format_number(contact_number),
        action_type="Category Opt-In",
        consent_category=consent_category,
        previous_status=previous_status,
        new_status=True,
        source="Webhook",
        source_message=message_doc_name,
    )


def enforce_marketing_template_compliance(template) -> None:
    """Block sending marketing templates without unsubscribe instructions.

    The footer is **always** re-verified against the actual text — the
    ``include_unsubscribe_instructions`` flag is intentionally ignored here
    because it is user-editable and must not serve as a blanket pass-through.

    Verification passes (any hit = compliant footer):
    1. Template-level ``unsubscribe_text`` appears in the footer
       (case-insensitive exact substring).
    2. Shared semantic detection via ``_footer_looks_like_unsubscribe``
       (same logic as sync-time ``_derive_sync_compliance``):
       a. ``settings.default_unsubscribe_text`` substring in footer.
       b. Enabled opt-out keyword rows matching the footer.
       c. Standalone uppercase ``STOP`` token, which is common in
          Meta-approved multilingual footers.
       d. Regex heuristic: verb+STOP ("reply STOP", "replying STOP", ...),
          "opt out" / "opt-out", or "unsubscribe".
          NOTE: title-case prose like "Stop by our office for help" must not
          pass.
    """
    if not template or getattr(template, "category", "") != "MARKETING":
        return

    settings = get_compliance_settings()
    if not settings.include_unsubscribe_in_marketing:
        return

    footer = (getattr(template, "footer", "") or "").strip()

    # Pass 1: template-level unsubscribe_text (explicit operator choice).
    tmpl_unsub = (getattr(template, "unsubscribe_text", "") or "").strip()
    if tmpl_unsub and tmpl_unsub.lower() in footer.lower():
        return

    # Pass 2 (a–c): shared semantic detection — same logic as sync-time.
    # Late import avoids a circular dependency (whatsapp_templates imports
    # from consent, not the other way round).
    if footer:
        from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_templates.whatsapp_templates import (  # noqa: E501
            _footer_looks_like_unsubscribe,
        )
        if _footer_looks_like_unsubscribe(footer, settings):
            return

    unsubscribe_text = tmpl_unsub or (
        settings.default_unsubscribe_text or "").strip()
    if not unsubscribe_text:
        frappe.throw(
            _("Unsubscribe text is required for marketing templates. "
              "Set it on the template or in Compliance Settings.")
        )

    frappe.throw(
        _("Marketing templates must include unsubscribe text in the "
          "footer. Please update the template.")
    )


def enforce_template_send_rules(
        template, *,
        to_number: str | None = None,
        service_window_active: bool = False,
) -> None:
    """Enforce template status + opt-in requirements before sending."""
    if not template:
        frappe.throw(_("Template is required to send a template message."))

    status = (getattr(template, "status", "") or "").strip().upper()
    if status != "APPROVED":
        frappe.throw(
            _("Template is not approved for sending (status: {0}).").format(
                getattr(template, "status", "") or "Unknown"
            )
        )

    if bool(getattr(template, "is_consent_request", 0)):
        return

    requires_opt_in = bool(getattr(template, "requires_opt_in", 0))
    if not requires_opt_in:
        return

    number = format_number(str(to_number or ""))
    if not number:
        frappe.throw(_("Cannot verify opt-in without a recipient number."))

    from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_profiles.whatsapp_profiles import WhatsAppProfiles  # noqa: E501
    profile = frappe.db.get_all(
        "WhatsApp Profiles",
        filters={"number": number},
        fields=["name", "do_not_contact", "is_opted_out", "is_opted_in"],
        limit=1,
    )

    if not profile:
        # Active service window allows sending requires_opt_in templates to
        # contacts who have not yet built a profile (unknown consent).
        if service_window_active:
            return
        frappe.throw(
            _("Recipient has not opted in to receive this template."))

    profile = cast(
        WhatsAppProfiles,
        frappe.get_doc("WhatsApp Profiles", profile[0].name))

    # DNC and explicit opt-out always block, even within the service window.
    if profile.do_not_contact or profile.is_opted_out:
        frappe.throw(
            _("Recipient has opted out. Cannot send this template."))

    if not profile.is_opted_in:
        # Active service window: allow sending to unknown-consent contacts.
        if service_window_active:
            return
        frappe.throw(
            _("Recipient has not explicitly opted in to receive this "
              "template."))
