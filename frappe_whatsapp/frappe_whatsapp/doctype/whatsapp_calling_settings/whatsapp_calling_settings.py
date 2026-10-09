from __future__ import annotations

from frappe.model.document import Document

from frappe_whatsapp.utils.calling import (
    validate_identity_destination_extension,
)


class WhatsAppCallingSettings(Document):
    # begin: auto-generated types
    # This code is auto-generated. Do not modify anything in this block.

    from typing import TYPE_CHECKING

    if TYPE_CHECKING:
        from frappe.types import DF

        agent_channel_template: DF.Data | None
        ami_host: DF.Data | None
        ami_password: DF.Password | None
        ami_port: DF.Int
        ami_use_tls: DF.Check
        ami_username: DF.Data | None
        call_permission_template: DF.Link | None
        destination_context: DF.Data | None
        destination_number_template: DF.Data | None
        enabled: DF.Check
        identity_destination_extension: DF.Data | None
        originate_timeout: DF.Int
    # end: auto-generated types
    def validate(self) -> None:
        self.identity_destination_extension = validate_identity_destination_extension(
            self.identity_destination_extension,
        )
