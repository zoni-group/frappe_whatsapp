# Copyright (c) 2026, Shridhar Patil and contributors
# For license information, please see license.txt

# import frappe
from frappe.model.document import Document


class WhatsAppOptOutKeyword(Document):
	# begin: auto-generated types
	# This code is auto-generated. Do not modify anything in this block.

	from typing import TYPE_CHECKING

	if TYPE_CHECKING:
		from frappe.types import DF

		action: DF.Literal["Full Opt-Out", "Category Opt-Out"]
		case_sensitive: DF.Check
		is_enabled: DF.Check
		keyword: DF.Data
		match_type: DF.Literal["Exact", "Contains", "Starts With"]
		target_category: DF.Link | None
		whatsapp_account: DF.Link | None
	# end: auto-generated types
	pass
