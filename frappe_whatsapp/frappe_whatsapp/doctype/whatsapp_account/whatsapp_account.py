# Copyright (c) 2025, Shridhar Patil and contributors
# For license information, please see license.txt

from datetime import datetime, timezone
from typing import Any, cast

import frappe
from frappe import _
from frappe.model.document import Document

from frappe_whatsapp.utils.meta import get_paginated_data, request_meta_json


REQUIRED_WHATSAPP_SCOPES = {
    "whatsapp_business_management",
    "whatsapp_business_messaging",
}
REQUIRED_ADS_SCOPES = {"ads_read"}


class WhatsAppAccount(Document):
    # begin: auto-generated types
    # This code is auto-generated. Do not modify anything in this block.

    from typing import TYPE_CHECKING

    if TYPE_CHECKING:
        from frappe.types import DF

        account_name: DF.Data | None
        ads_access_token: DF.Password | None
        allow_auto_read_receipt: DF.Check
        app_id: DF.Data | None
        app_secret: DF.Password | None
        business_id: DF.Data | None
        business_portfolio_id: DF.Data | None
        enable_campaign_tracking: DF.Check
        is_default_incoming: DF.Check
        is_default_outgoing: DF.Check
        phone_id: DF.Data | None
        status: DF.Literal["Active", "Inactive"]
        token: DF.Password | None
        url: DF.Data | None
        version: DF.Data | None
        webhook_verify_token: DF.Data | None
        whatsapp_client_app: DF.Link | None
    # end: auto-generated types

    def on_update(self):
        """Check there is only one default of each type."""
        self.there_must_be_only_one_default()

    def there_must_be_only_one_default(self):
        """If current WhatsApp Account is default,
        un-default all other accounts."""
        for field in ("is_default_incoming", "is_default_outgoing"):
            if not self.get(field):
                continue

            for whatsapp_account in frappe.get_all(
                    "WhatsApp Account", filters={field: 1}):
                if whatsapp_account.name == self.name:
                    continue

                whatsapp_account = frappe.get_doc(
                    "WhatsApp Account", whatsapp_account.name)
                whatsapp_account.set(field, 0)
                whatsapp_account.save()


def _bearer_headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _format_expiry(value: Any) -> str | None:
    try:
        timestamp = int(value or 0)
    except (TypeError, ValueError):
        return None
    if timestamp <= 0:
        return None
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()


def validate_account_connection(account: WhatsAppAccount) -> dict[str, Any]:
    """Validate a WhatsApp Account against Meta without exposing secrets."""
    account_name = str(account.name or "")
    required_fields = ("url", "version", "business_id", "phone_id")
    missing = [field for field in required_fields if not account.get(field)]
    token = account.get_password("token")
    if not token:
        missing.append("token")
    if missing:
        frappe.throw(
            _("WhatsApp Account {0} is missing required fields: {1}.").format(
                account_name,
                ", ".join(missing),
            )
        )

    url = str(account.url).rstrip("/")
    version = str(account.version).strip("/")
    headers = _bearer_headers(str(token))

    # This gives the clearest OAuth errors, including Meta code/subcode.
    request_meta_json(
        "GET",
        f"{url}/{version}/me",
        account_name=account_name,
        operation=_("access-token validation"),
        headers=headers,
        params={"fields": "id"},
    )

    phone_numbers = get_paginated_data(
        f"{url}/{version}/{account.business_id}/phone_numbers",
        account_name=account_name,
        operation=_("WABA phone-number validation"),
        headers=headers,
        params={"fields": "id", "limit": 100},
    )
    phone_matches_waba = any(
        str(phone.get("id") or "") == str(account.phone_id)
        for phone in phone_numbers
    )
    if not phone_matches_waba:
        frappe.throw(
            _(
                "WhatsApp Account {0}: phone ID {1} does not belong to "
                "WABA {2}."
            ).format(account_name, account.phone_id, account.business_id)
        )

    result: dict[str, Any] = {
        "valid": True,
        "phone_matches_waba": True,
        "token_type": "UNKNOWN",
        "expires_at": None,
        "required_scopes_present": None,
        "production_ready": False,
        "warnings": [],
    }

    app_id = str(account.app_id or "")
    app_secret = account.get_password("app_secret")
    if not app_id or not app_secret:
        result["warnings"].append(
            _(
                "App ID and App Secret are required to verify token type, "
                "expiry, and scopes."
            )
        )
        return result

    debug_payload = request_meta_json(
        "GET",
        f"{url}/{version}/debug_token",
        account_name=account_name,
        operation=_("access-token inspection"),
        headers=_bearer_headers(f"{app_id}|{app_secret}"),
        params={"input_token": str(token)},
    )
    debug_data = debug_payload.get("data")
    if not isinstance(debug_data, dict) or not debug_data.get("is_valid"):
        frappe.throw(
            _("WhatsApp Account {0}: Meta reports an invalid access token.").format(
                account_name
            )
        )
    assert isinstance(debug_data, dict)

    token_type = str(debug_data.get("type") or "UNKNOWN").upper()
    scopes = {
        str(scope) for scope in (debug_data.get("scopes") or []) if scope
    }
    missing_scopes = sorted(REQUIRED_WHATSAPP_SCOPES - scopes)
    if missing_scopes:
        frappe.throw(
            _("WhatsApp Account {0}: token is missing scopes: {1}.").format(
                account_name,
                ", ".join(missing_scopes),
            )
        )

    expires_at = _format_expiry(debug_data.get("expires_at"))
    result.update(
        {
            "token_type": token_type,
            "expires_at": expires_at,
            "required_scopes_present": True,
            "production_ready": token_type == "SYSTEM_USER" and not expires_at,
        }
    )
    if token_type != "SYSTEM_USER":
        result["warnings"].append(
            _(
                "This is a {0} token. Use a permanent SYSTEM_USER token in "
                "production."
            ).format(token_type)
        )
    elif expires_at:
        result["warnings"].append(
            _("This system-user token expires at {0}.").format(expires_at)
        )
    return result


def validate_campaign_tracking_connection(
    account: WhatsAppAccount,
) -> dict[str, Any]:
    """Validate the separate Marketing API token without exposing it."""
    account_name = str(account.name or "")
    required_fields = ("url", "version", "app_id")
    missing = [field for field in required_fields if not account.get(field)]
    token = account.get_password("ads_access_token")
    app_secret = account.get_password("app_secret")
    if not token:
        missing.append("ads_access_token")
    if not app_secret:
        missing.append("app_secret")
    if missing:
        frappe.throw(
            _("WhatsApp Account {0} is missing campaign tracking fields: {1}.").format(
                account_name,
                ", ".join(missing),
            )
        )

    url = str(account.url).rstrip("/")
    version = str(account.version).strip("/")
    app_id = str(account.app_id)
    debug_payload = request_meta_json(
        "GET",
        f"{url}/{version}/debug_token",
        account_name=account_name,
        operation=_("Ads access-token inspection"),
        headers=_bearer_headers(f"{app_id}|{app_secret}"),
        params={"input_token": str(token)},
    )
    debug_data = debug_payload.get("data")
    if not isinstance(debug_data, dict) or not debug_data.get("is_valid"):
        frappe.throw(
            _("WhatsApp Account {0}: Meta reports an invalid Ads token.").format(
                account_name
            )
        )
    assert isinstance(debug_data, dict)

    token_app_id = str(debug_data.get("app_id") or "")
    if token_app_id and token_app_id != app_id:
        frappe.throw(
            _(
                "WhatsApp Account {0}: the Ads token belongs to a different Meta app."
            ).format(account_name)
        )

    scopes = {
        str(scope) for scope in (debug_data.get("scopes") or []) if scope
    }
    missing_scopes = sorted(REQUIRED_ADS_SCOPES - scopes)
    if missing_scopes:
        frappe.throw(
            _("WhatsApp Account {0}: Ads token is missing scopes: {1}.").format(
                account_name,
                ", ".join(missing_scopes),
            )
        )

    token_type = str(debug_data.get("type") or "UNKNOWN").upper()
    expires_at = _format_expiry(debug_data.get("expires_at"))
    warnings = []
    if token_type != "SYSTEM_USER":
        warnings.append(
            _("This is a {0} token. Use a permanent SYSTEM_USER token in production.").format(
                token_type
            )
        )
    elif expires_at:
        warnings.append(
            _("This system-user token expires at {0}.").format(expires_at)
        )

    return {
        "valid": True,
        "token_type": token_type,
        "expires_at": expires_at,
        "required_scopes_present": True,
        "production_ready": token_type == "SYSTEM_USER" and not expires_at,
        "warnings": warnings,
    }


@frappe.whitelist()
def validate_meta_connection(whatsapp_account: str) -> dict[str, Any]:
    """Permission-checked endpoint for the account form validation button."""
    frappe.only_for("System Manager")
    account = cast(
        WhatsAppAccount,
        frappe.get_doc("WhatsApp Account", whatsapp_account),
    )
    account.check_permission("read")
    return validate_account_connection(account)


@frappe.whitelist()
def validate_campaign_tracking(whatsapp_account: str) -> dict[str, Any]:
    """Permission-checked endpoint for Ads-token validation."""
    frappe.only_for("System Manager")
    account = cast(
        WhatsAppAccount,
        frappe.get_doc("WhatsApp Account", whatsapp_account),
    )
    account.check_permission("read")
    return validate_campaign_tracking_connection(account)
