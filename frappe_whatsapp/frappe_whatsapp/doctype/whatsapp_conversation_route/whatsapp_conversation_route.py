# Copyright (c) 2026, Shridhar Patil and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document


class WhatsAppConversationRoute(Document):
	# begin: auto-generated types
	# This code is auto-generated. Do not modify anything in this block.

	from typing import TYPE_CHECKING

	if TYPE_CHECKING:
		from frappe.types import DF

		contact_number: DF.Data | None
		contact_profile: DF.Link | None
		last_outgoing_at: DF.Datetime | None
		last_outgoing_message: DF.Data | None
		last_source_app: DF.Link | None
		whatsapp_account: DF.Link | None
	# end: auto-generated types
	def autoname(self):
		if self.contact_number:
			self.name = f"{self.contact_number}-{self.whatsapp_account}"
		else:
			self.name = frappe.generate_hash(length=20)
