def test_hour_23_automation():

    import frappe

    from frappe.utils import now_datetime, add_to_date
    from frappe_whatsapp.utils import format_number
    from frappe_whatsapp.utils.hour_23_automation import (
        _get_candidates, run_hour_23_automation)
    from typing import Any, cast
    from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_profiles.whatsapp_profiles import WhatsAppProfiles  # noqa: E501
    from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_compliance_settings.whatsapp_compliance_settings import WhatsAppComplianceSettings  # noqa: E501

    PROFILE_NAME = "veacqjvfl9"
    LANGUAGE = "es"
    HAS_MARKETING_CONSENT = True   # False => consent request,
    # True => status follow-up
    MINUTES_INTO_FINAL_HOUR = 10    # 0..59; 10 means "23h10m ago"
    # for a 24h window
    ENABLE_HOUR23_IF_DISABLED = True
    RESET_EXISTING_LOG_FOR_ANCHOR = True
    RUN_AUTOMATION_NOW = True      # True will run the automation
    # immediately and may send

    if not 0 <= MINUTES_INTO_FINAL_HOUR < 60:
        raise ValueError("MINUTES_INTO_FINAL_HOUR must be between 0 and 59")

    settings = cast(
        WhatsAppComplianceSettings,
        frappe.get_doc("WhatsApp Compliance Settings"))

    if ENABLE_HOUR23_IF_DISABLED and not int(
            settings.enable_hour_23_follow_up or 0):
        settings.enable_hour_23_follow_up = 1
        settings.save(ignore_permissions=True)

    window_hours = int(settings.window_hours or 24)
    target_creation = add_to_date(
        now_datetime(),
        hours=-(window_hours - 1),
        minutes=-MINUTES_INTO_FINAL_HOUR,
    )

    profile = cast(
        WhatsAppProfiles,
        frappe.get_doc("WhatsApp Profiles", PROFILE_NAME))
    number = format_number(profile.number)

    # Make the profile eligible for hour-23
    profile.do_not_contact = 0
    profile.do_not_contact_reason = None
    profile.is_opted_out = 0
    profile.opted_out_at = None
    profile.opted_out_reason = None
    profile.detected_language = LANGUAGE
    profile.language_detected_at = now_datetime()
    profile.is_opted_in = 1 if HAS_MARKETING_CONSENT else 0
    profile.opted_in_at = now_datetime() if HAS_MARKETING_CONSENT else None
    profile.consent_status = "Opted In" if HAS_MARKETING_CONSENT else "Unknown"

    # If category-specific marketing consent is configured, set/remove
    # that too.
    marketing_cat = (settings.marketing_consent_category or "").strip()
    if marketing_cat:
        kept_rows = []
        for row in (profile.get("category_consents") or []):
            if row.consent_category != marketing_cat:
                kept_rows.append(row.as_dict())

        profile.set("category_consents", kept_rows)

        if HAS_MARKETING_CONSENT:
            profile.append("category_consents", {
                "consent_category": marketing_cat,
                "consented": 1,
                "consented_at": now_datetime(),
                "consent_method": "API",
            })

    # Reuse the latest inbound for this number if it exists; otherwise
    # create one.
    latest = frappe.get_all(
        "WhatsApp Message",
        filters={"type": "Incoming", "from": number},
        fields=["name", "whatsapp_account", "creation"],
        order_by="creation desc",
        limit=1,
    )

    if latest:
        latest_row = cast(Any, latest[0])
        anchor_name = str(latest_row.name)
        whatsapp_account = cast(str | None, latest_row.whatsapp_account)
    else:
        profile_account = cast(str | None, profile.whatsapp_account)
        active_account = cast(str | None, frappe.db.get_value(
            "WhatsApp Account",
            {"status": "Active"},
            "name",
        ))
        whatsapp_account = profile_account or active_account
        if not whatsapp_account:
            raise Exception(
                "No WhatsApp account found. Set profile.whatsapp_account or "
                "create an active account.")

        msg = frappe.get_doc({
            "doctype": "WhatsApp Message",
            "type": "Incoming",
            "from": number,
            "profile_name": profile.profile_name or profile.name,
            "content_type": "text",
            "message": "Hour-23 test anchor",
            "whatsapp_account": whatsapp_account,
        })
        msg.insert(ignore_permissions=True)
        anchor_name = msg.name

    if not profile.whatsapp_account and whatsapp_account:
        profile.whatsapp_account = whatsapp_account
    profile.save(ignore_permissions=True)

    # If you want to re-trigger the same anchor for testing, clear its old
    # hour-23 log row.
    if RESET_EXISTING_LOG_FOR_ANCHOR:
        frappe.db.delete(
            "WhatsApp Hour 23 Automation Log",
            {"anchor_message": anchor_name},
        )

    # Move the latest inbound into the final hour of the window.
    frappe.db.sql(
        """
        UPDATE `tabWhatsApp Message`
        SET creation = %s, modified = %s
        WHERE name = %s
        """,
        (target_creation, target_creation, anchor_name),
    )

    frappe.db.commit()
    frappe.clear_cache()

    candidate = next(
        (row for row in _get_candidates(window_hours) if
         row["anchor_message"] == anchor_name),
        None,
    )

    print({
        "profile": profile.name,
        "number": number,
        "whatsapp_account": whatsapp_account,
        "anchor_message": anchor_name,
        "target_creation": str(target_creation),
        "window_hours": window_hours,
        "expected_path": ("status_follow_up" if HAS_MARKETING_CONSENT
                          else "consent_request"),
        "candidate_ready_now": bool(candidate),
    })

    if RUN_AUTOMATION_NOW:
        run_hour_23_automation()
        log_rows = frappe.get_all(
            "WhatsApp Hour 23 Automation Log",
            {"anchor_message": anchor_name},
            ["name", "send_status", "template", "outgoing_message"],
            limit=1,
        )
        log_row = log_rows[0] if log_rows else None
        print("hour_23_log:", log_row)
