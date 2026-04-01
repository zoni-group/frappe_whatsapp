"""Inbound language detection for WhatsApp messages.

Calls the local language-detector microservice and updates the
WhatsApp Profile when the detector result clears the acceptance rules.

Configuration (site_config.json / frappe.conf):
  whatsapp_lang_detector_url              str   default "http://localhost:9394"
  whatsapp_lang_detect_min_confidence     float default 0.80
  whatsapp_lang_detect_min_gap            float default 0.20
  whatsapp_lang_detect_fallback_confidence float default 0.60
  whatsapp_lang_detect_fallback_gap       float default 0.35
"""
from typing import cast

import frappe
import requests
from frappe.utils import now_datetime

from frappe_whatsapp.utils import format_number

# ── Tuneable module-level defaults (override via frappe.conf) ─────────

#: Minimum top-1 confidence score to commit to a language update.
MIN_CONFIDENCE: float = 0.80

#: Minimum gap between top-1 and top-2 scores (avoids ambiguous calls).
MIN_GAP: float = 0.20

#: Fallback top-1 confidence floor when the top-1 language clearly dominates.
FALLBACK_CONFIDENCE: float = 0.60

#: Fallback gap used with ``FALLBACK_CONFIDENCE`` for short but decisive text.
FALLBACK_GAP: float = 0.35

#: Minimum alphabetic character count for text to be worth sending.
MIN_ALPHA_CHARS: int = 4

#: HTTP request timeout for the detector service.
DETECTOR_TIMEOUT: float = 3.0

# Consent / control keywords that are not reliable for language detection.
# Case-insensitive exact matches against the stripped message text.
_SKIP_WORDS: frozenset[str] = frozenset({
    "stop", "start", "yes", "no", "subscribe", "unsubscribe",
    "help", "cancel", "ok", "okay",
})


# ── Internal helpers ──────────────────────────────────────────────────

def _get_detector_url() -> str:
    return str(
        frappe.conf.get("whatsapp_lang_detector_url")
        or "http://localhost:9394"
    )


def _get_thresholds() -> tuple[float, float, float, float]:
    min_conf = float(
        frappe.conf.get("whatsapp_lang_detect_min_confidence", MIN_CONFIDENCE)
    )
    min_gap = float(
        frappe.conf.get("whatsapp_lang_detect_min_gap", MIN_GAP)
    )
    fallback_conf = float(
        frappe.conf.get(
            "whatsapp_lang_detect_fallback_confidence",
            FALLBACK_CONFIDENCE,
        )
    )
    fallback_gap = float(
        frappe.conf.get("whatsapp_lang_detect_fallback_gap", FALLBACK_GAP)
    )
    return min_conf, min_gap, fallback_conf, fallback_gap


def _is_worth_detecting(text: str) -> bool:
    """Return True when *text* contains enough real content for detection.

    Rejects:
    - empty / whitespace-only strings
    - consent/control keyword-only messages (STOP, START, YES, NO, …)
    - strings with fewer than MIN_ALPHA_CHARS alphabetic characters
    """
    if not text or not text.strip():
        return False

    stripped = text.strip()

    if stripped.lower() in _SKIP_WORDS:
        return False

    alpha_count = sum(1 for ch in stripped if ch.isalpha())
    if alpha_count < MIN_ALPHA_CHARS:
        return False

    return True


def _call_detector(text: str) -> dict | None:
    """POST to /detect/confidence with top_n=2.

    Returns the parsed JSON dict on success, or None on any failure
    (network error, timeout, non-200 response, invalid JSON).

    Log levels are chosen to balance visibility with noise:
    - ConnectionError (service not running) → DEBUG: expected during
      planned downtime; WARNING here would flood logs on every message.
    - Timeout → WARNING: detector is reachable but slow — worth surfacing.
    - Non-200 or invalid JSON → WARNING: unexpected API misbehaviour.
    """
    url = f"{_get_detector_url()}/detect/confidence"
    try:
        resp = requests.post(
            url,
            json={"text": text, "top_n": 2},
            timeout=DETECTOR_TIMEOUT,
        )
    except requests.exceptions.Timeout:
        frappe.logger("frappe_whatsapp").warning(
            "Language detector timed out; language detection skipped"
        )
        return None
    except requests.exceptions.ConnectionError:
        # Service is not running — expected during planned downtime.
        frappe.logger("frappe_whatsapp").debug(
            "Language detector unreachable; language detection skipped"
        )
        return None
    except Exception as exc:
        frappe.logger("frappe_whatsapp").warning(
            f"Language detector error ({type(exc).__name__}: {exc}); "
            f"detection skipped"
        )
        return None

    if resp.status_code != 200:
        frappe.logger("frappe_whatsapp").warning(
            f"Language detector returned HTTP {resp.status_code}; "
            f"detection skipped"
        )
        return None

    try:
        return resp.json()
    except Exception:
        frappe.logger("frappe_whatsapp").warning(
            "Language detector returned invalid JSON; detection skipped"
        )
        return None


def _parse_accepted_detection(
    response: dict,
    min_confidence: float,
    min_gap: float,
    fallback_confidence: float,
    fallback_gap: float,
) -> tuple[str, str, float] | None:
    """Return (iso639_1_code, language_name, confidence) when the
    detection result clears the acceptance thresholds, otherwise None.

    Gates (applied in order):
    1. `detected` must be non-null (lingua's own internal threshold).
    2. Accept either:
       - top-1 confidence >= min_confidence and gap >= min_gap, or
       - top-1 confidence >= fallback_confidence and gap >= fallback_gap.

    The fallback path is meant for short but decisive phrases where the
    absolute confidence can be modest while the lead over the runner-up is
    still strong enough to be useful.
    """
    detected = response.get("detected")
    if not detected:
        frappe.logger("frappe_whatsapp").debug(
            "Language detector: detected=null; ambiguous input, skipping"
        )
        return None

    values: list = response.get("confidence_values") or []
    if not values:
        return None

    top1_score = float((values[0] or {}).get("confidence", 0.0))
    top2_score = (
        float((values[1] or {}).get("confidence", 0.0)
              ) if len(values) > 1 else 0.0
    )

    gap = top1_score - top2_score
    accepted_by_primary_rule = (
        top1_score >= min_confidence and gap >= min_gap
    )
    accepted_by_fallback_rule = (
        top1_score >= fallback_confidence and gap >= fallback_gap
    )

    if not (accepted_by_primary_rule or accepted_by_fallback_rule):
        frappe.logger("frappe_whatsapp").debug(
            "Language detector: top-1 "
            f"{top1_score:.2f}, gap {gap:.2f} did not clear acceptance "
            f"thresholds (primary: conf>={min_confidence}, gap>={min_gap}; "
            f"fallback: conf>={fallback_confidence}, gap>={fallback_gap}); "
            "skipping"
        )
        return None

    lang = (values[0] or {}).get("language") or {}
    iso_code = str(lang.get("iso639_1") or "").lower()
    lang_name = str(lang.get("name") or "")

    if not iso_code:
        return None

    if not accepted_by_primary_rule:
        frappe.logger("frappe_whatsapp").debug(
            "Language detector: accepted via fallback rule "
            f"(top-1={top1_score:.2f}, gap={gap:.2f})"
        )

    return iso_code, lang_name, top1_score


# ── Public API ────────────────────────────────────────────────────────

def update_profile_language(
    *,
    contact_number: str,
    whatsapp_account: str,
    text: str,
    message_doc_name: str,
    profile_name: str | None = None,
) -> None:
    """Detect the language of *text* and update the WhatsApp Profile.

    Behaviour:
    - If *text* is not worth detecting (keyword, too short, etc.) → no-op.
    - If the detector is down / returns low confidence → existing language
      is preserved unchanged.
    - If the result clears the acceptance rule and there is no existing
      language → set it.
    - If the result clears the acceptance rule and the new language matches
      → refresh metadata.
    - If the result clears the acceptance rule and the new language differs
      → switch and log.

    Never raises; all errors are caught and logged so webhook processing
    continues unaffected.
    """
    try:
        if not _is_worth_detecting(text):
            return

        response = _call_detector(text)
        if response is None:
            return

        min_conf, min_gap, fallback_conf, fallback_gap = _get_thresholds()
        result = _parse_accepted_detection(
            response,
            min_conf,
            min_gap,
            fallback_conf,
            fallback_gap,
        )
        if result is None:
            frappe.logger("frappe_whatsapp").debug(
                f"Language detection: low/ambiguous for {contact_number}; "
                f"existing language preserved"
            )
            return

        iso_code, lang_name, confidence = result

        # Ensure the profile exists; create a minimal row if not.
        number = format_number(contact_number)
        profile_id = frappe.db.get_value(
            "WhatsApp Profiles", {"number": number}, "name"
        )
        from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_profiles.whatsapp_profiles import WhatsAppProfiles  # noqa: E501
        if not profile_id:
            new_doc = cast(
                WhatsAppProfiles,
                frappe.get_doc({
                    "doctype": "WhatsApp Profiles",
                    "number": number,
                    "profile_name": profile_name,
                    "whatsapp_account": whatsapp_account,
                }),
            )
            new_doc.insert(ignore_permissions=True)
            profile_id = str(new_doc.name)

        profile = cast(
            WhatsAppProfiles,
            frappe.get_doc("WhatsApp Profiles", str(profile_id)),
        )
        existing_lang = str(profile.get("detected_language") or "")

        if not existing_lang:
            frappe.logger("frappe_whatsapp").info(
                f"WhatsApp language detected for {number}: "
                f"{iso_code} ({lang_name}) confidence={confidence:.2f}"
            )
        elif existing_lang != iso_code:
            frappe.logger("frappe_whatsapp").info(
                f"WhatsApp language switch for {number}: "
                f"{existing_lang} → {iso_code} ({lang_name}) "
                f"confidence={confidence:.2f}"
            )
        # else: same language — refresh metadata silently

        profile.detected_language = iso_code
        profile.detected_language_name = lang_name
        profile.language_detection_confidence = confidence
        profile.language_detected_at = now_datetime()
        profile.language_source_message = message_doc_name
        profile.save(ignore_permissions=True)

    except Exception:
        frappe.log_error(
            frappe.get_traceback(),
            "WhatsApp language detection failed"
        )
