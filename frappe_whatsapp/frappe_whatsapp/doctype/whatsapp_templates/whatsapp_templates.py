"""Create whatsapp template."""

# Copyright (c) 2022, Shridhar Patil and contributors
# For license information, please see license.txt
import os
import re
import frappe
import magic
from frappe.model.document import Document
from frappe.integrations.utils import make_post_request, make_request
from frappe import _
from frappe_whatsapp.utils import get_whatsapp_account
from frappe_whatsapp.utils.consent import get_compliance_settings, get_opt_out_keywords
from frappe.utils import get_bench_path, get_site_base_path
from typing import Any, Mapping, cast

_ALLOWED_CATEGORY = {
    "", "TRANSACTIONAL", "MARKETING", "OTP", "UTILITY", "AUTHENTICATION"
}
_ALLOWED_HEADER_TYPE = {"", "TEXT", "DOCUMENT", "IMAGE"}

# Categories that do not require opt-in by default
_NON_MARKETING_CATEGORIES = frozenset(
    {"UTILITY", "AUTHENTICATION", "OTP", "TRANSACTIONAL"})

# Compliance fields whose manual edits should clear compliance_auto_managed
_COMPLIANCE_FIELDS = (
    "requires_opt_in",
    "include_unsubscribe_instructions",
    "unsubscribe_text",
    "is_consent_request",
    "required_consent_category",
)

# Regex matching actionable opt-out instructions in a template footer.
#
# Design rationale:
#   • Uppercase "STOP" is treated as a standalone opt-out keyword because
#     Meta-approved multilingual footers commonly keep the keyword in English
#     while translating the surrounding instruction text.
#   • Normal prose like "Stop by our office for help" is still excluded from
#     the standalone STOP path because that uses a case-sensitive token match.
#   • Additional English heuristic matching remains for natural-language
#     footers that do not use the exact uppercase STOP token.
#   • "opt out" / "opt-out" are treated as inherently actionable in footer
#     context; they are almost exclusively used to mean "remove yourself from
#     this list".
#   • "unsubscribe" is treated similarly — virtually always an instruction
#     when it appears in a message footer.
_OPT_OUT_STOP_TOKEN_RE = re.compile(r"\bSTOP\b")
_OPT_OUT_FOOTER_RE = re.compile(
    r"""
    (?:
        # Verb-then-STOP: "reply STOP", "text STOP", "replying STOP",
        # "reply with STOP", "send a message STOP", etc.
        # Capped at 3 filler words so the pattern cannot span a full sentence.
        \b(?:repl(?:y|ying)|text(?:ing)?|send(?:ing)?|type|typing|msg(?:ing)?)\b
        (?:\s+\w+){0,3}?\s+\bSTOP\b
        |
        # "opt out" or "opt-out" as a phrase
        \bopt[\s\-]out\b
        |
        # "unsubscribe" as an action word
        \bunsubscribe\b
    )
    """,
    re.IGNORECASE | re.VERBOSE,
)


class WhatsAppTemplates(Document):
    # begin: auto-generated types
    # This code is auto-generated. Do not modify anything in this block.

    from typing import TYPE_CHECKING

    if TYPE_CHECKING:
        from frappe.types import DF
        from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_button.whatsapp_button import WhatsAppButton

        actual_name: DF.Data | None
        buttons: DF.Table[WhatsAppButton]
        compliance_auto_managed: DF.Check
        category: DF.Literal["", "TRANSACTIONAL", "MARKETING", "OTP", "UTILITY", "AUTHENTICATION"]
        field_names: DF.SmallText | None
        footer: DF.Data | None
        for_doctype: DF.Link | None
        header: DF.Data | None
        header_type: DF.Literal["", "TEXT", "DOCUMENT", "IMAGE"]
        id: DF.Data | None
        include_unsubscribe_instructions: DF.Check
        is_consent_request: DF.Check
        is_transactional: DF.Check
        language: DF.Link
        language_code: DF.Data | None
        required_consent_category: DF.Link | None
        requires_opt_in: DF.Check
        sample: DF.Attach | None
        sample_values: DF.SmallText | None
        status: DF.Data | None
        template: DF.Code
        template_name: DF.Data
        unsubscribe_text: DF.Data | None
        whatsapp_account: DF.Link | None
    # end: auto-generated types
    """Create whatsapp template."""

    def validate(self):
        # printing self for easier debugging in case of errors during
        # validation
        self.set_whatsapp_account()

        before = cast(WhatsAppTemplates, self.get_doc_before_save())
        self._detect_manual_compliance_change(before)

        self._apply_marketing_unsubscribe_rules()
        self._apply_consent_request_rules()

        language_changed = False
        if before:
            language_changed = (str(before.language) != str(self.language))

        if (not self.language_code) or language_changed:
            lang_code = str(
                frappe.db.get_value("Language", self.language) or "en")
            self.language_code = lang_code.replace("-", "_")

        if self.header_type in ["IMAGE", "DOCUMENT"] and self.sample:
            self.get_session_id()
            self.get_media_id()

        if not self.is_new():
            self.update_template()

    def _detect_manual_compliance_change(
        self, before: "WhatsAppTemplates | None"
    ) -> None:
        """Clear compliance_auto_managed if the user changed a compliance field.

        Only acts when the existing record was previously auto-managed
        (compliance_auto_managed == 1).  That way a subsequent Meta sync will
        not silently overwrite the operator's deliberate choice.
        """
        if not before or not before.compliance_auto_managed:
            return
        for field in _COMPLIANCE_FIELDS:
            if str(getattr(self, field) or "") != str(
                    getattr(before, field) or ""):
                self.compliance_auto_managed = 0
                return

    def _apply_marketing_unsubscribe_rules(self) -> None:
        """Auto-inject unsubscribe text for marketing templates
           when enabled."""
        if self.category != "MARKETING":
            return

        settings = get_compliance_settings()
        if not settings.include_unsubscribe_in_marketing:
            return

        unsubscribe_text = (
            (self.unsubscribe_text or "").strip()
            or (settings.default_unsubscribe_text or "").strip()
        )
        if not unsubscribe_text:
            frappe.throw(
                _("Unsubscribe text is required for marketing templates. "
                  "Set Unsubscribe Text on the template or Default "
                  "Unsubscribe Text in Compliance Settings.")
            )

        footer = (self.footer or "").strip()
        if not footer:
            self.footer = unsubscribe_text
            self.include_unsubscribe_instructions = 1
            return

        if unsubscribe_text not in footer:
            if _footer_looks_like_unsubscribe(footer, settings):
                # Footer already contains valid opt-out wording in natural
                # language (e.g. "You can opt out at any time by replying
                # STOP."); accept it without appending the configured text.
                self.include_unsubscribe_instructions = 1
            else:
                # Keep content readable; avoid double separators.
                separator = "\n" if "\n" in footer else " "
                self.footer = f"{footer}{separator}{unsubscribe_text}"
                self.include_unsubscribe_instructions = 1

    def _apply_consent_request_rules(self) -> None:
        """Normalize consent-request templates so they can bootstrap opt-in.

        A consent request must not itself require prior opt-in/category
        consent, otherwise it can never be delivered in strict mode.
        """
        if not self.is_consent_request:
            return

        self.requires_opt_in = 0
        self.required_consent_category = None

    def set_whatsapp_account(self):
        """Set whatsapp account to default if missing"""
        if not self.whatsapp_account:
            default_whatsapp_account = get_whatsapp_account()
            if not default_whatsapp_account:
                frappe.throw(
                    _("Please set a default outgoing WhatsApp Account"
                      " or Select available WhatsApp Account"))
            else:
                self.whatsapp_account = default_whatsapp_account.name

    def get_session_id(self):
        """Upload media and store upload session id."""
        self.get_settings()

        # Guard: sample must exist here
        if not self.sample:
            frappe.throw(_("No sample file attached."))

        file_path = self.get_absolute_path(self.sample)
        if not file_path:
            frappe.throw(_("Could not resolve sample path."))

        if not os.path.exists(str(file_path)):
            frappe.throw(_("Sample file not found at: {0}").format(file_path))

        mime = magic.Magic(mime=True)
        file_type = mime.from_file(str(file_path))

        payload = {
            "file_length": os.path.getsize(str(file_path)),
            "file_type": file_type,
            "messaging_product": "whatsapp",
        }

        r = None
        try:
            r = make_post_request(
                f"{self._url}/{self._version}/{self._app_id}/uploads",
                headers=self._headers,
                json=payload,
            )
        except Exception:
            integration = frappe.flags.integration_request
            if integration is None:
                frappe.throw(_("Upload failed due to server error."))
                return
            res = integration.json().get("error", {})
            frappe.throw(
                msg=res.get(
                    "error_user_msg",
                    res.get("message", _("Upload failed"))),
                title=res.get("error_user_title", "Error"),
            )

        # Runtime + typing safety
        if not r or not isinstance(r, dict):
            frappe.throw(_("Unexpected upload response from Meta."))

        resp = cast(Mapping[str, Any], r)
        session_id = resp.get("id")
        if not session_id:
            frappe.throw(_("Upload session id not returned by Meta."))

        self._session_id = str(session_id)

    def get_media_id(self):
        """Upload the actual binary to the upload session and store media
        handle."""
        self.get_settings()

        # Guards
        if not self.sample:
            frappe.throw(_("No sample file attached."))

        file_name = self.get_absolute_path(self.sample)
        if not file_name:
            frappe.throw(_("Could not resolve sample path."))

        if not os.path.exists(str(file_name)):
            frappe.throw(_("Sample file not found at: {0}").format(file_name))

        if not getattr(self, "_session_id", None):
            frappe.throw(
                _("Missing upload session id. Run get_session_id() first."))

        headers = {"authorization": f"OAuth {self._token}"}

        # Read binary
        with open(str(file_name), "rb") as f:
            file_content = f.read()

        r = None
        try:
            r = make_post_request(
                f"{self._url}/{self._version}/{self._session_id}",
                headers=headers,
                data=file_content,  # bytes
            )
        except Exception:
            integration = frappe.flags.integration_request
            if integration is None:
                frappe.throw(_("Upload failed due to server error."))
                return
            res = integration.json().get("error", {})
            frappe.throw(
                msg=res.get(
                    "error_user_msg",
                    res.get("message", _("Upload failed"))),
                title=res.get("error_user_title", "Error"),
            )

        # Runtime + typing safety
        if not r or not isinstance(r, dict):
            frappe.throw(_("Unexpected upload response from Meta."))

        resp = cast(Mapping[str, Any], r)
        media_id = resp.get("h")  # Meta returns 'h' as the handle
        if not media_id:
            frappe.throw(_("Media handle not returned by Meta (missing 'h')."))

        self._media_id = str(media_id)

    def get_absolute_path(self, file_name) -> str | None:
        file_path = None
        if (file_name.startswith('/files/')):
            file_path = (
                f'{get_bench_path()}/sites/'
                f'{get_site_base_path()[2:]}/public{file_name}')
        if (file_name.startswith('/private/')):
            file_path = (f'{get_bench_path()}/sites/'
                         f'{get_site_base_path()[2:]}{file_name}')
        return file_path

    def after_insert(self):
        if self.template_name:
            self.actual_name = self.template_name.lower().replace(" ", "_")

        self.get_settings()

        data: dict[str, Any] = {
            "name": self.actual_name,
            "language": self.language_code,
            "category": self.category,
            "components": [],
        }

        body: dict[str, Any] = {
            "type": "BODY",
            "text": self.template,
        }

        if self.sample_values:
            placeholder_count = len(
                set(re.findall(r"\{\{\d+\}\}", self.template)))
            splits = (placeholder_count - 1) if placeholder_count > 1 else 0
            values = [v.strip() for v in self.sample_values.split(
                ",", maxsplit=splits) if v.strip()]
            body["example"] = {"body_text": [values]}

        data["components"].append(body)

        if self.header_type:
            data["components"].append(self.get_header())

        if self.footer:
            data["components"].append({"type": "FOOTER", "text": self.footer})

        if self.buttons:
            from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_button.whatsapp_button import (  # noqa: E501
                WhatsAppButton,
            )

            button_block: dict[str, Any] = {"type": "BUTTONS", "buttons": []}

            for btn in self.buttons:
                btn = cast(WhatsAppButton, btn)
                b: dict[str, Any] = {
                    "type": btn.button_type,
                    "text": btn.button_label}

                if btn.button_type == "Visit Website":
                    b["type"] = "URL"
                    b["url"] = str(btn.website_url)
                    if btn.url_type == "Dynamic" and btn.example_url:
                        b["example"] = btn.example_url.split(",")
                elif btn.button_type == "Call Phone":
                    b["type"] = "PHONE_NUMBER"
                    b["phone_number"] = str(btn.phone_number)
                elif btn.button_type == "Quick Reply":
                    b["type"] = "QUICK_REPLY"

                button_block["buttons"].append(b)

            data["components"].append(button_block)

        r = None
        try:
            r = make_post_request(
                (f"{self._url}/{self._version}/"
                 f"{self._business_id}/message_templates"),
                headers=self._headers,
                json=data,
            )
        except Exception:
            integration = frappe.flags.integration_request
            if integration is None:
                frappe.throw(
                    _("Template creation failed due to server error."))
                return
            res = integration.json().get("error", {})
            frappe.throw(
                msg=res.get(
                    "error_user_msg",
                    res.get("message", _("Template creation failed"))),
                title=res.get("error_user_title", "Error"),
            )

        # ✅ Runtime + typing safety for the response
        if not r or not isinstance(r, dict):
            frappe.throw(
                _("Unexpected response from Meta while creating template."))

        resp = cast(Mapping[str, Any], r)

        template_id = resp.get("id")
        status = resp.get("status")

        if not template_id:
            frappe.throw(_("Meta did not return template id (missing 'id')."))

        self.id = str(template_id)
        if status:
            self.status = str(status)

        self.db_update()

    def update_template(self):
        """Update template to Meta."""
        self.get_settings()

        data: dict[str, Any] = {"components": []}

        body: dict[str, Any] = {
            "type": "BODY",
            "text": self.template,
        }

        if self.sample_values:
            placeholder_count = len(
                set(re.findall(r"\{\{\d+\}\}", self.template)))
            splits = (placeholder_count - 1) if placeholder_count > 1 else 0
            values = [
                v.strip() for v in self.sample_values.split(
                    ",", maxsplit=splits) if v.strip()]
            body["example"] = {"body_text": [values]}

        data["components"].append(body)

        if self.header_type:
            data["components"].append(self.get_header())

        if self.footer:
            data["components"].append({"type": "FOOTER", "text": self.footer})

        if self.buttons:
            button_block: dict[str, Any] = {"type": "BUTTONS", "buttons": []}

            for btn in self.buttons:
                b: dict[str, Any] = {
                    "type": btn.button_type,
                    "text": btn.button_label}

                if btn.button_type == "Visit Website":
                    b["type"] = "URL"
                    b["url"] = str(btn.website_url)
                    if btn.url_type == "Dynamic" and btn.example_url:
                        b["example"] = [
                            u.strip() for u in btn.example_url.split(
                                ",") if u.strip()]

                elif btn.button_type == "Call Phone":
                    b["type"] = "PHONE_NUMBER"
                    b["phone_number"] = str(btn.phone_number)

                elif btn.button_type == "Quick Reply":
                    b["type"] = "QUICK_REPLY"

                button_block["buttons"].append(b)

            data["components"].append(button_block)

        try:
            make_post_request(
                f"{self._url}/{self._version}/{self.id}",
                headers=self._headers,
                json=data,
            )
        except Exception:
            integration = frappe.flags.integration_request
            if integration is not None:
                res = integration.json().get("error", {})
                frappe.throw(
                    msg=res.get(
                        "error_user_msg",
                        res.get("message", _("Update failed"))),
                    title=res.get("error_user_title", "Error"),
                )
            raise

    def get_settings(self):
        """Get whatsapp settings."""
        from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_account.whatsapp_account import WhatsAppAccount  # noqa: E501
        settings = cast(
            WhatsAppAccount,
            frappe.get_doc(
                "WhatsApp Account",
                str(self.whatsapp_account)))
        self._token = settings.get_password("token")
        self._url = settings.url
        self._version = settings.version
        self._business_id = settings.business_id
        self._app_id = settings.app_id

        self._headers = {
            "authorization": f"Bearer {self._token}",
            "content-type": "application/json",
        }

    def on_trash(self):
        self.get_settings()
        url = (
            f"{self._url}/{self._version}/{self._business_id}/"
            f"message_templates?name={self.actual_name}"
        )

        try:
            make_request("DELETE", url, headers=self._headers)
            return
        except Exception:
            integration = getattr(frappe.flags, "integration_request", None)
            if integration is None or not hasattr(integration, "json"):
                # Fallback: re-raise or show generic error
                frappe.throw(_("Failed to delete template on Meta."))
                return

            payload = integration.json()
            err = payload.get("error", {}) if isinstance(payload, dict) else {}
            res = cast(Mapping[str, Any], err)

            title = str(res.get("error_user_title") or "Error")
            msg = str(res.get("error_user_msg") or res.get(
                "message") or _("Error"))

            if title == "Message Template Not Found":
                frappe.msgprint("Deleted locally", title, alert=True)
            else:
                frappe.throw(msg=msg, title=title)

    def get_header(self) -> dict[str, Any]:
        """Build Meta template HEADER component."""
        header: dict[str, Any] = {
            "type": "HEADER",
            "format": self.header_type,
        }

        if self.header_type == "TEXT":
            # self.header is Data | None → force to str (or empty string)
            header_text = (self.header or "").strip()
            header["text"] = header_text

            # If you use sample for TEXT header examples,
            # ensure it's a list[str]
            if self.sample:
                samples = [
                    s.strip() for s in self.sample.split(",") if s.strip()]
                if samples:
                    header["example"] = {"header_text": samples}

            return header

        # Non-TEXT header types require a media handle
        media_handle = getattr(self, "_media_id", None)
        if not media_handle:
            frappe.throw(
                _("Missing media handle for header. Attach a sample and"
                  " save to upload media."))

        header["example"] = {"header_handle": [str(media_handle)]}
        return header


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _normalize_meta_language_code(value: Any) -> str:
    """Normalize Meta language codes to the stored template format."""
    return str(value or "").strip().replace("-", "_")


def _resolve_language_link(value: Any) -> str:
    """Resolve a Meta language code to the corresponding Language docname."""
    language = str(value or "").strip()
    if not language:
        return ""

    candidates = [
        language,
        language.replace("_", "-"),
        language.replace("-", "_"),
    ]

    seen: set[str] = set()
    for candidate in candidates:
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)

        if frappe.db.exists("Language", candidate):
            return candidate

    return ""


def _get_integration_error() -> dict[str, Any]:
    integration = getattr(frappe.flags, "integration_request", None)
    if not integration or not hasattr(integration, "json"):
        return {}
    payload = integration.json()
    payload_dict = _as_dict(payload)
    return _as_dict(payload_dict.get("error"))


@frappe.whitelist()
def fetch() -> str:
    """Fetch templates from Meta and upsert into WhatsApp Templates."""
    whatsapp_accounts = frappe.get_all(
        "WhatsApp Account",
        filters={"status": "Active"},
        fields=["name", "url", "version", "business_id"],
    )

    for account in whatsapp_accounts:
        account_map = cast(Mapping[str, Any], account)

        account_name = str(account_map.get("name") or "")
        if not account_name:
            continue

        # get credentials
        token = frappe.get_doc(
            "WhatsApp Account", account_name).get_password("token")
        url = str(account_map.get("url") or "")
        version = str(account_map.get("version") or "")
        business_id = str(account_map.get("business_id") or "")

        headers = {
            "authorization": f"Bearer {token}",
            "content-type": "application/json",
        }

        try:
            raw = make_request(
                "GET",
                f"{url}/{version}/{business_id}/message_templates",
                headers=headers,
            )

            resp = _as_dict(raw)
            templates = _as_list(resp.get("data"))

            for t in templates:
                template = _as_dict(t)

                template_name = str(template.get("name") or "")
                if not template_name:
                    continue

                # load or create
                existing_name = frappe.db.get_value(
                    "WhatsApp Templates",
                    {"actual_name": template_name},
                    "name",
                )
                if existing_name:
                    doc = cast(
                        WhatsAppTemplates,
                        frappe.get_doc(
                            "WhatsApp Templates",
                            str(existing_name),
                        ))
                else:
                    doc = cast(
                        WhatsAppTemplates,
                        frappe.new_doc("WhatsApp Templates"))
                    doc.template_name = template_name
                    doc.actual_name = template_name

                # status/language/id (these are simple Data fields)
                meta_language = str(template.get("language") or "")
                doc.status = str(template.get("status") or "")
                doc.language_code = _normalize_meta_language_code(
                    meta_language)
                doc.language = _resolve_language_link(meta_language)
                doc.id = str(template.get("id") or "")
                doc.whatsapp_account = account_name

                # category is Literal[...] -> validate before assigning
                cat = str(template.get("category") or "")
                if cat not in _ALLOWED_CATEGORY:
                    cat = ""
                doc.category = cast(Any, cat)

                # components
                # Pre-reset component-owned fields.  An existing template
                # may have carried HEADER, FOOTER, BODY parameters, or
                # BUTTONS from an earlier sync.  If Meta has since removed
                # any of those components the fields must be cleared *before*
                # rebuilding from the current payload so stale data does not
                # survive into the next upsert.
                doc.header_type = cast(Any, "")
                doc.header = None
                doc.footer = None
                doc.sample_values = None
                doc.set("buttons", [])

                components = _as_list(template.get("components"))
                for c in components:
                    component = _as_dict(c)
                    ctype = str(component.get("type") or "")

                    if ctype == "HEADER":
                        fmt = str(component.get("format") or "")
                        if fmt not in _ALLOWED_HEADER_TYPE:
                            fmt = ""
                        doc.header_type = cast(Any, fmt)

                        if fmt == "TEXT":
                            doc.header = str(component.get("text") or "")

                    elif ctype == "FOOTER":
                        doc.footer = str(component.get("text") or "")

                    elif ctype == "BODY":
                        doc.template = str(component.get("text") or "")
                        ex = _as_dict(component.get("example"))
                        body_text = _as_list(ex.get("body_text"))
                        # Meta shape: body_text = [[...]]
                        if body_text and isinstance(body_text[0], list):
                            vals = [str(x) for x in body_text[0]]
                            doc.sample_values = ",".join(vals)

                    elif ctype == "BUTTONS":
                        doc.set("buttons", [])
                        frappe.db.delete(
                            "WhatsApp Button",
                            {"parent": doc.name,
                             "parenttype": "WhatsApp Templates"},
                        )

                        type_map = {
                            "URL": "Visit Website",
                            "PHONE_NUMBER": "Call Phone",
                            "QUICK_REPLY": "Quick Reply",
                            "FLOW": "Flow",
                        }

                        buttons = _as_list(component.get("buttons"))
                        for i, b_raw in enumerate(buttons, start=1):
                            button = _as_dict(b_raw)
                            meta_type = str(button.get("type") or "")

                            if meta_type not in type_map:
                                continue

                            btn: dict[str, Any] = {
                                "button_type": type_map[meta_type],
                                "button_label": str(button.get("text") or ""),
                                "sequence": i,
                            }

                            if meta_type == "URL":
                                btn["website_url"] = str(
                                    button.get("url") or "")
                                btn["url_type"] = (
                                    "Dynamic"
                                    if "{{" in btn["website_url"]
                                    else "Static")
                                ex = button.get("example")
                                if isinstance(ex, list):
                                    btn["example_url"] = ",".join(
                                        [str(x) for x in ex])

                            elif meta_type == "PHONE_NUMBER":
                                btn["phone_number"] = str(
                                    button.get("phone_number") or "")

                            elif meta_type == "FLOW":
                                btn["flow"] = str(button.get("flow") or "")

                            doc.append("buttons", btn)

                _derive_sync_compliance(doc, is_new=(existing_name is None))
                upsert_doc_without_hooks(doc, "WhatsApp Button", "buttons")
                _check_hour_23_drift_after_sync(doc)

        except Exception as e:
            err = _get_integration_error()
            title = str(err.get("error_user_title") or "Error")
            msg = str(err.get("error_user_msg") or err.get(
                "message") or str(e))
            frappe.throw(msg=msg, title=title)

    return "Successfully fetched templates from meta"


def _check_hour_23_drift_after_sync(doc: "WhatsAppTemplates") -> None:
    """Log hour-23 configuration drift after a Meta template sync.

    Called **after** ``upsert_doc_without_hooks()`` so that ``doc.name``
    is the final durable document name.  Hour-23 settings link fields
    (``hour_23_language_map`` and ``hour_23_template_parameters``) store
    the ``WhatsApp Templates`` document name, not ``actual_name``, so the
    lookup key passed to ``get_hour_23_drift_messages`` must be ``doc.name``
    (e.g. ``"CONSENT-TMPL-en"``).  ``actual_name`` is preserved only for
    human-readable log context.

    Chosen behaviour: **log, not block**.
    Blocking would reject status/approval updates for all templates in a
    sync batch whenever any single template drifts; that is worse than
    the drift itself.  The existing param-error path in the automation
    (``_mark_log_skipped``) still prevents bad sends — the log gives
    operators early warning to fix the mapping before sends are attempted.
    """
    from frappe_whatsapp.utils.hour_23_params import count_declared_meta_params
    from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_compliance_settings\
        .whatsapp_compliance_settings import get_hour_23_drift_messages

    # doc.name is the authoritative document identifier (e.g. CONSENT-TMPL-en).
    # Settings link fields store this value, not actual_name.
    doc_name = str(doc.name or "").strip()
    if not doc_name:
        return

    declared_params = count_declared_meta_params(doc)
    for msg in get_hour_23_drift_messages(doc_name, declared_params):
        frappe.log_error(msg, "WhatsApp Hour-23 Automation")


def upsert_doc_without_hooks(doc, child_dt, child_field):
    """Insert or update a parent document and its children without hooks."""
    if frappe.db.exists(doc.doctype, doc.name):
        doc.db_update()
        frappe.db.delete(
            child_dt, {"parent": doc.name, "parenttype": doc.doctype})
    else:
        doc.db_insert()
    for d in doc.get(child_field):
        d.parent = doc.name
        d.parenttype = doc.doctype
        d.parentfield = child_field
        d.db_insert()
    frappe.db.commit()


def _footer_looks_like_unsubscribe(
    footer: str,
    settings: Any,
    whatsapp_account: str | None = None,
) -> bool:
    """Return True if *footer* clearly contains opt-out / unsubscribe text.

    Four detection passes (any hit returns True):

    1. Check against ``default_unsubscribe_text`` from Compliance Settings
       (case-insensitive substring match).
    2. Check against enabled WhatsApp Opt Out Keyword rows, scoped to
       *whatsapp_account* (account-specific keywords + global keywords only).
       Keyword matching mirrors ``check_opt_out_keyword()`` in consent.py:
       case normalisation first, then ``match_type``
       (Exact / Contains / Starts With).
    3. Standalone uppercase ``STOP`` token — handles Meta-approved
       multilingual footers such as
       "Responda STOP para cancelar o recebimento de comunicacoes."
    4. Regex heuristic — accepts naturally-worded footers that contain
       recognised opt-out terms (stop, unsubscribe, opt-out / opt out) even
       when the footer does not match the exact configured text.  This handles
       Meta-approved footers like
       "You can opt out at any time by replying STOP."
    """
    if not footer:
        return False

    footer_lower = footer.lower()

    # Pass 1: configured default unsubscribe text
    default_unsub = str(
        getattr(settings, "default_unsubscribe_text", "") or "").strip()
    if default_unsub and default_unsub.lower() in footer_lower:
        return True

    # Pass 2: opt-out keyword rows from DB
    try:
        keywords = get_opt_out_keywords(whatsapp_account)
        for kw in keywords:
            kw_text = str(kw.get("keyword", "")).strip()
            if not kw_text:
                continue

            # Mirror check_opt_out_keyword: normalise case, then strip text
            if kw.get("case_sensitive"):
                text = footer.strip()
            else:
                kw_text = kw_text.lower()
                text = footer_lower.strip()

            match_type = kw.get("match_type", "Exact")
            if match_type == "Exact":
                matched = text == kw_text
            elif match_type == "Contains":
                matched = kw_text in text
            elif match_type == "Starts With":
                matched = text.startswith(kw_text)
            else:
                matched = False

            if matched:
                return True
    except Exception:
        pass

    # Pass 3: standalone uppercase STOP keyword in multilingual instructions
    if _OPT_OUT_STOP_TOKEN_RE.search(footer):
        return True

    # Pass 4: regex heuristic — common opt-out terms in any wording
    if _OPT_OUT_FOOTER_RE.search(footer):
        return True

    return False


def _derive_sync_compliance(doc: "WhatsAppTemplates", is_new: bool) -> None:
    """Populate compliance defaults on a template imported from Meta.

    Called from ``fetch()`` before ``upsert_doc_without_hooks()``.  Never
    modifies ``doc.footer`` — that would push invented content back to Meta.

    Skip / run logic:
    - New template (is_new=True)       → always apply.
    - Existing, compliance_auto_managed=1 → re-derive; all owned fields are
                                           reset first so re-derivation is
                                           fully authoritative.
    - Existing, compliance_auto_managed=0 → skip; preserve existing values
                                           regardless of what they are.
    """
    if not is_new and not doc.compliance_auto_managed:
        return

    # Reset all owned compliance fields before re-deriving so that stale
    # values from a previous sync (e.g. is_consent_request=1 after a prefix
    # is removed from settings) are cleared rather than carried forward.
    doc.is_consent_request = 0
    doc.requires_opt_in = 0
    doc.required_consent_category = None
    doc.include_unsubscribe_instructions = 0
    doc.unsubscribe_text = ""

    settings = get_compliance_settings()
    category = str(doc.category or "")
    template_name = str(doc.actual_name or doc.template_name or "")
    footer = str(doc.footer or "").strip()
    whatsapp_account = str(doc.whatsapp_account or "") or None

    # --- consent-request prefix check ---
    prefixes_raw = str(
        getattr(settings, "consent_request_template_prefixes", "") or "")
    prefixes = [p.strip() for p in prefixes_raw.split(",") if p.strip()]

    if prefixes and any(template_name.startswith(p) for p in prefixes):
        doc.is_consent_request = 1
        doc.requires_opt_in = 0
        doc.required_consent_category = None
    else:
        # Category-based opt-in default
        if category == "MARKETING":
            doc.requires_opt_in = 1
        elif category in _NON_MARKETING_CATEGORIES:
            doc.requires_opt_in = 0
        # Unknown / blank category: leave at reset default (0)

    # --- footer unsubscribe detection (read-only; never writes to doc.footer) ---
    if footer:
        detected = _footer_looks_like_unsubscribe(
            footer, settings, whatsapp_account=whatsapp_account)
        doc.include_unsubscribe_instructions = 1 if detected else 0
        doc.unsubscribe_text = footer if detected else ""

    doc.compliance_auto_managed = 1
