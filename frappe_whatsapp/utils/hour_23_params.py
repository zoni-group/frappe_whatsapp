"""Body-parameter resolution for hour-23 follow-up automation.

Kept separate from ``hour_23_automation.py`` to maintain a clear
single-responsibility boundary.

Entry points used by ``hour_23_automation.py``:
  ``load_contact_context(contact_number)``
      Loads WhatsApp profile + linked Contact for a phone number.

  ``build_hour_23_body_params(template, context, param_rows)``
      Resolves each mapping row to a string value and returns the
      ordered JSON dict expected by ``WhatsApp Message.body_param``.

  ``count_declared_meta_params(template)``
      Returns the number of ``{{n}}`` placeholder positions declared
      in the current Meta body text, used for drift detection and
      shape validation.  Never reads ``field_names``.

  ``extract_first_name(name)``
      Zero-dependency first-name extractor with salutation stripping.
"""
import json
import re

import frappe
from frappe import _

from frappe_whatsapp.utils import format_number

# Matches Meta body placeholders like {{1}}, {{2}}, …
_PLACEHOLDER_RE = re.compile(r"\{\{(\d+)\}\}")

# Common salutations stripped before extracting the first name token.
_SALUTATIONS: frozenset[str] = frozenset({
    "dr", "madam", "miss", "mr", "mrs", "ms", "mx",
    "prof", "rev", "sir",
})


def extract_first_name(name: str | None) -> str:
    """Return the first non-salutation token from *name*.

    Each token is stripped of trailing ``.,`` before the check.
    Returns ``""`` when *name* is blank or consists only of
    recognised salutations.

    No external dependencies — uses a simple heuristic rather than
    a full name-parsing library.
    """
    if not name:
        return ""
    tokens = name.strip().split()
    for token in tokens:
        clean = token.strip(".,")
        if clean and clean.lower().rstrip(".") not in _SALUTATIONS:
            return clean
    return ""


def _resolve_first_name(profile, contact) -> str:
    """Resolve a contact's first name with a multi-step fallback.

    Fallback order:
      1. ``Contact.first_name``
      2. ``extract_first_name(Contact.full_name)``
      3. ``extract_first_name(WhatsApp Profiles.profile_name)``
      4. ``""`` — caller should apply ``fallback_value`` from the param row
    """
    if contact:
        first = str(getattr(contact, "first_name", "") or "").strip()
        if first:
            return first
        full = str(getattr(contact, "full_name", "") or "").strip()
        parsed = extract_first_name(full)
        if parsed:
            return parsed

    if profile:
        pname = str(
            getattr(profile, "profile_name", "") or ""
        ).strip()
        if pname:
            parsed = extract_first_name(pname)
            if parsed:
                return parsed

    return ""


def _resolve_parameter(row, profile, contact) -> str:
    """Resolve one mapping row to its string value.

    Applies ``fallback_value`` when the primary resolution is blank.
    Returns ``""`` when both primary and fallback are empty.
    """
    source_type = str(getattr(row, "source_type", "") or "")

    if source_type == "First Name":
        value = _resolve_first_name(profile, contact)
    elif source_type == "Literal":
        value = str(getattr(row, "literal_value", "") or "")
    elif source_type == "Profile Field":
        field = str(getattr(row, "source_field", "") or "")
        value = (
            str(getattr(profile, field, "") or "")
            if field and profile else ""
        )
    elif source_type == "Contact Field":
        field = str(getattr(row, "source_field", "") or "")
        value = (
            str(getattr(contact, field, "") or "")
            if field and contact else ""
        )
    else:
        value = ""

    if not value:
        fallback = str(getattr(row, "fallback_value", "") or "")
        if fallback:
            return fallback

    return value


def count_declared_meta_params(template) -> int:
    """Return the number of ``{{n}}`` body-placeholder positions declared
    on *template* according to the Meta-owned template data.

    Source-of-truth priority (first non-empty wins):

    1. ``template.template`` — the body text field, always current after
       a Meta sync.  Placeholder count is derived by counting *unique*
       ``{{n}}`` occurrences via regex.

    2. ``template.sample_values`` — comma-separated example values from
       Meta sync.  Used as a fallback when the body text is absent.

    ``field_names`` is intentionally **never** read here.  It is a
    runtime substitution field, not a Meta-declared placeholder count,
    and is not cleared by the sync pre-reset.
    """
    body = str(getattr(template, "template", "") or "").strip()
    if body:
        return len(set(_PLACEHOLDER_RE.findall(body)))
    sv = str(getattr(template, "sample_values", "") or "").strip()
    if sv:
        return len([x for x in sv.split(",") if x.strip()])
    return 0


# Backward-compatibility alias — external code that imported
# count_template_params continues to work unchanged.
count_template_params = count_declared_meta_params


def build_hour_23_body_params(
    template,
    context: dict,
    param_rows: list,
) -> tuple[str | None, str | None]:
    """Build the ``body_param`` JSON string for a parameterised send.

    Returns ``(json_str, None)`` on success, ``(None, error_msg)``
    when a required parameter cannot be resolved, or ``(None, None)``
    when the template has no body parameters.

    *context* must be the dict returned by ``load_contact_context``
    (keys ``"profile"`` and ``"contact"``; either may be ``None``).

    *param_rows* must be sorted by ``parameter_index`` (guaranteed
    when the caller uses ``_build_param_mapping`` in
    ``hour_23_automation.py``).

    The returned JSON is an ordered object ``{"1": v1, "2": v2, …}``.
    ``WhatsApp Message.send_template()`` reads ``.values()`` from the
    parsed dict, so insertion order must match Meta placeholder order
    exactly.
    """
    expected = count_declared_meta_params(template)
    if expected == 0:
        return None, None

    template_name = getattr(template, "name", "<unknown>")

    if not param_rows:
        return None, _(
            "template '{0}' has {1} body parameter(s) but no "
            "hour-23 parameter mapping is configured"
        ).format(template_name, expected)

    profile = context.get("profile")
    contact = context.get("contact")
    params: dict[str, str] = {}

    for idx in range(1, expected + 1):
        row = next(
            (
                r for r in param_rows
                if int(getattr(r, "parameter_index", 0) or 0) == idx
            ),
            None,
        )
        if row is None:
            return None, _(
                "template '{0}': no mapping row for parameter {1}"
            ).format(template_name, idx)

        value = _resolve_parameter(row, profile, contact)
        if not value:
            return None, _(
                "template '{0}': parameter {1} "
                "(source: {2}) could not be resolved "
                "and has no fallback"
            ).format(
                template_name,
                idx,
                getattr(row, "source_type", "unknown"),
            )
        params[str(idx)] = value

    return json.dumps(params), None


def load_contact_context(contact_number: str) -> dict:
    """Load the WhatsApp profile and linked Contact for *contact_number*.

    Returns ``{"profile": <doc | None>, "contact": <doc | None>}``.
    Callers that need only the context for param resolution should use
    this; callers that need eligibility checking should continue to use
    ``_load_contact_state`` in ``hour_23_automation.py``.
    """
    number = format_number(contact_number)
    profile_rows = frappe.db.get_all(
        "WhatsApp Profiles",
        filters={"number": number},
        fields=["name"],
        limit=1,
    )

    if not profile_rows:
        return {"profile": None, "contact": None}

    profile = frappe.get_doc(
        "WhatsApp Profiles", profile_rows[0].name
    )
    contact_name = str(getattr(profile, "contact", "") or "")
    contact = None
    if contact_name:
        try:
            contact = frappe.get_doc("Contact", contact_name)
        except frappe.exceptions.DoesNotExistError:
            pass  # Stale link — treat as no linked contact.

    return {"profile": profile, "contact": contact}
