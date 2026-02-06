"""Create whatsapp template."""

# Copyright (c) 2022, Shridhar Patil and contributors
# For license information, please see license.txt
import os
import frappe
import magic
from frappe.model.document import Document
from frappe.integrations.utils import make_post_request, make_request
from frappe import _
from frappe_whatsapp.utils import get_whatsapp_account
from frappe_whatsapp.utils.consent import get_compliance_settings
from frappe.utils import get_bench_path, get_site_base_path
from typing import Any, Mapping, cast

_ALLOWED_CATEGORY = {
    "", "TRANSACTIONAL", "MARKETING", "OTP", "UTILITY", "AUTHENTICATION"
}
_ALLOWED_HEADER_TYPE = {"", "TEXT", "DOCUMENT", "IMAGE"}


class WhatsAppTemplates(Document):
    # begin: auto-generated types
    # This code is auto-generated. Do not modify anything in this block.

    from typing import TYPE_CHECKING

    if TYPE_CHECKING:
        from frappe.types import DF
        from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_button.whatsapp_button import WhatsAppButton

        actual_name: DF.Data | None
        buttons: DF.Table[WhatsAppButton]
        category: DF.Literal["", "TRANSACTIONAL", "MARKETING", "OTP", "UTILITY", "AUTHENTICATION"]
        field_names: DF.SmallText | None
        footer: DF.Data | None
        for_doctype: DF.Link | None
        header: DF.Data | None
        header_type: DF.Literal["", "TEXT", "DOCUMENT", "IMAGE"]
        id: DF.Data | None
        include_unsubscribe_instructions: DF.Check
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
        self.set_whatsapp_account()
        self._apply_marketing_unsubscribe_rules()

        before = cast(
            WhatsAppTemplates,
            self.get_doc_before_save())

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
            # Keep content readable; avoid double separators.
            separator = "\n" if "\n" in footer else " "
            self.footer = f"{footer}{separator}{unsubscribe_text}"
            self.include_unsubscribe_instructions = 1

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
                data=payload,
            )
        except Exception:
            integration = frappe.flags.integration_request
            if not integration:
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
            if not integration:
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
            values = [v.strip() for v in self.sample_values.split(
                ",") if v.strip()]
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
                data=data,  # <-- send dict, not json.dumps
            )
        except Exception:
            integration = frappe.flags.integration_request
            if not integration:
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
            values = [
                v.strip() for v in self.sample_values.split(",") if v.strip()]
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
                data=data,  # ✅ send dict
            )
        except Exception:
            integration = frappe.flags.integration_request
            if integration:
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
            if not integration or not hasattr(integration, "json"):
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
                if frappe.db.exists(
                        "WhatsApp Templates",
                        {"actual_name": template_name}):
                    doc = cast(
                        WhatsAppTemplates,
                        frappe.get_doc(
                            "WhatsApp Templates",
                            str(template_name),
                        ))
                else:
                    doc = cast(
                        WhatsAppTemplates,
                        frappe.new_doc("WhatsApp Templates"))
                    doc.template_name = template_name
                    doc.actual_name = template_name

                # status/language/id (these are simple Data fields)
                doc.status = str(template.get("status") or "")
                doc.language_code = str(template.get("language") or "")
                doc.id = str(template.get("id") or "")
                doc.whatsapp_account = account_name

                # category is Literal[...] -> validate before assigning
                cat = str(template.get("category") or "")
                if cat not in _ALLOWED_CATEGORY:
                    cat = ""
                doc.category = cast(Any, cat)

                # components
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

                upsert_doc_without_hooks(doc, "WhatsApp Button", "buttons")

        except Exception as e:
            err = _get_integration_error()
            title = str(err.get("error_user_title") or "Error")
            msg = str(err.get("error_user_msg") or err.get(
                "message") or str(e))
            frappe.throw(msg=msg, title=title)

    return "Successfully fetched templates from meta"


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
