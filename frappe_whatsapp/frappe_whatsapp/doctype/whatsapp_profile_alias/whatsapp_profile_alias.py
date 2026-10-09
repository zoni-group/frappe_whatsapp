from frappe.model.document import Document


class WhatsAppProfileAlias(Document):
    # begin: auto-generated types
    # This code is auto-generated. Do not modify anything in this block.

    from typing import TYPE_CHECKING

    if TYPE_CHECKING:
        from frappe.types import DF

        alias_key: DF.Data
        alias_type: DF.Literal["phone", "user_id", "parent_user_id"]
        alias_value: DF.Data
        identity_scope: DF.Data
        is_current: DF.Check
        whatsapp_account: DF.Link | None
        whatsapp_profile: DF.Link
    # end: auto-generated types
    def before_validate(self) -> None:
        from frappe_whatsapp.utils.identity import (
            alias_key,
            normalize_alias,
        )

        self.identity_scope = str(self.identity_scope or "").strip()
        self.alias_value = normalize_alias(
            str(self.alias_type or ""), self.alias_value
        )
        if self.identity_scope and self.alias_type and self.alias_value:
            self.alias_key = alias_key(
                self.identity_scope,
                self.alias_type,
                self.alias_value,
            )
