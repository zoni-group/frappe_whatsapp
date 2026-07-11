from __future__ import annotations

from frappe.model.document import Document


class WhatsAppCallAgent(Document):
    # begin: auto-generated types
    # This code is auto-generated. Do not modify anything in this block.

    from typing import TYPE_CHECKING

    if TYPE_CHECKING:
        from frappe.types import DF

        enabled: DF.Check
        extension: DF.Data
        user: DF.Link
    # end: auto-generated types
    pass
