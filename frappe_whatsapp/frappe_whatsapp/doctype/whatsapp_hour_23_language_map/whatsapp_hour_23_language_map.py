# Copyright (c) 2026, Shridhar Patil and contributors
# For license information, please see license.txt

from frappe.model.document import Document


class WhatsAppHour23LanguageMap(Document):
    # begin: auto-generated types
    # This code is auto-generated. Do not modify anything in this block.

    from typing import TYPE_CHECKING

    if TYPE_CHECKING:
        from frappe.types import DF

        consent_template: DF.Link | None
        language_code: DF.Data
        parent: DF.Data
        parentfield: DF.Data
        parenttype: DF.Data
        status_follow_up_template: DF.Link | None
    # end: auto-generated types
    pass
