# Copyright (c) 2025, Shridhar Patil and contributors
# For license information, please see license.txt

# import frappe
from frappe.model.document import Document

class WhatsAppButton(Document):
	# begin: auto-generated types
	# This code is auto-generated. Do not modify anything in this block.

	from typing import TYPE_CHECKING

	if TYPE_CHECKING:
		from frappe.types import DF

		button_label: DF.Data
		button_type: DF.Literal["Quick Reply", "Call Phone", "Visit Website", "Flow"]
		example_url: DF.Data | None
		parent: DF.Data
		parentfield: DF.Data
		parenttype: DF.Data
		phone_number: DF.Data | None
		url_type: DF.Literal["Static", "Dynamic"]
		website_url: DF.Data | None
	# end: auto-generated types
	pass
