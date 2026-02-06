# Copyright (c) 2026, Shridhar Patil and contributors
# For license information, please see license.txt

# import frappe
from frappe.model.document import Document


class WhatsAppConsentLog(Document):
	# begin: auto-generated types
	# This code is auto-generated. Do not modify anything in this block.

	from typing import TYPE_CHECKING

	if TYPE_CHECKING:
		from frappe.types import DF

		action: DF.Literal["Opt-In", "Opt-Out", "Category Opt-In", "Category Opt-Out", "Consent Updated"]
		consent_category: DF.Link | None
		ip_address: DF.Data | None
		name: DF.Int | None
		new_status: DF.Check
		phone_number: DF.Data
		previous_status: DF.Check
		profile: DF.Link
		source: DF.Literal["Manual", "Webhook", "API", "Bulk Import", "System"]
		source_message: DF.Link | None
		timestamp: DF.Datetime | None
		user: DF.Link | None
	# end: auto-generated types
	pass
