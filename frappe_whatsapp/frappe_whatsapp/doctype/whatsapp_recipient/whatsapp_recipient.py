import frappe
from frappe.model.document import Document


class WhatsAppRecipient(Document):
	# begin: auto-generated types
	# This code is auto-generated. Do not modify anything in this block.

	from typing import TYPE_CHECKING

	if TYPE_CHECKING:
		from frappe.types import DF

		consent_status: DF.Literal["Unknown", "Opted In", "Opted Out"]
		mobile_number: DF.Data
		parent: DF.Data
		parentfield: DF.Data
		parenttype: DF.Data
		recipient_data: DF.Code | None
		recipient_name: DF.Data | None
		skip_consent_check: DF.Check
	# end: auto-generated types
	pass