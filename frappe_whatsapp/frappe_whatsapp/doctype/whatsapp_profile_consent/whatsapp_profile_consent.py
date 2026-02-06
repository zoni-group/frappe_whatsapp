# Copyright (c) 2026, Shridhar Patil and contributors
# For license information, please see license.txt

# import frappe
from frappe.model.document import Document


class WhatsAppProfileConsent(Document):
	# begin: auto-generated types
	# This code is auto-generated. Do not modify anything in this block.

	from typing import TYPE_CHECKING

	if TYPE_CHECKING:
		from frappe.types import DF

		consent_category: DF.Link
		consent_method: DF.Literal["Explicit Form", "API", "Imported", "Web Widget", "WhatsApp Reply", "Verbal"]
		consented: DF.Check
		consented_at: DF.Datetime | None
		parent: DF.Data
		parentfield: DF.Data
		parenttype: DF.Data
	# end: auto-generated types
	pass
