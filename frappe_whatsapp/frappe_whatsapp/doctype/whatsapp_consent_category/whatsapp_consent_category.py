# Copyright (c) 2026, Shridhar Patil and contributors
# For license information, please see license.txt

# import frappe
from frappe.model.document import Document


class WhatsAppConsentCategory(Document):
	# begin: auto-generated types
	# This code is auto-generated. Do not modify anything in this block.

	from typing import TYPE_CHECKING

	if TYPE_CHECKING:
		from frappe.types import DF

		category_code: DF.Data
		category_name: DF.Data
		default_opt_in: DF.Check
		description: DF.SmallText | None
		is_enabled: DF.Check
		requires_explicit_consent: DF.Check
	# end: auto-generated types
	pass
