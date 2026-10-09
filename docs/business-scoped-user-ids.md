# Business-scoped WhatsApp user IDs

This application supports Meta business-scoped user IDs (BSUIDs) introduced
in the June 29, 2026 webhook format. A contact may have a phone number, a
regular `user_id`, a `parent_user_id`, or any linked combination of them.
Phone numbers are optional and must never be inferred from a BSUID.

## Identity rules

- Identity scope is `portfolio:<business_portfolio_id>` when the WhatsApp
  Account is configured with a Meta Business Portfolio ID. It falls back to
  `account:<WhatsApp Account name>` to prevent unsafe cross-account merges.
- `WhatsApp Profiles` is the stable contact. `WhatsApp Profile Alias` stores
  phone, `user_id`, and `parent_user_id` aliases with a scope-unique hash.
- `WhatsApp Profile Account State` stores consent and do-not-contact state for
  one profile/account pair. Blocking, routes, service windows, calls, and call
  permissions are likewise evaluated with both profile and account.
- A username is mutable display metadata and is never used for deduplication.
- Identifiers are merged only when Meta supplies them in the same event or a
  `REQUEST_CONTACT_INFO` reply explicitly links the phone to the sender.
- `user_id_update` retains previous identifiers as inactive aliases and emits
  `whatsapp.identity_updated`.

## Client webhook v2

Client apps receive additive version-2 envelopes:

```json
{
  "schema_version": 2,
  "event": "whatsapp.incoming",
  "event_id": "stable-id",
  "occurred_at": "2026-10-01 12:00:00",
  "app_id": "zoni-crm",
  "whatsapp_account": "Account A",
  "message": {
    "message_id": "wamid...",
    "from": null,
    "from_user_id": "US.AbCd1234",
    "identity": {
      "profile_id": "...",
      "phone": null,
      "user_id": "US.AbCd1234",
      "parent_user_id": null,
      "username": "example",
      "preferred_recipient": "US.AbCd1234"
    }
  }
}
```

Supported events are `whatsapp.incoming`, `whatsapp.message_status`, and
`whatsapp.identity_updated`. Delivery is at least once and receivers must
deduplicate `event_id` after committing their own transaction.

`whatsapp.message_status` contains the local/provider IDs, current and
previous status, normalized status, recipient identity fields, the structured
raw Meta status `contacts`, and an optional `error`. `whatsapp.identity_updated`
contains `identity`, `previous_user_id`, and `previous_parent_user_id`. All
three use the same durable **WhatsApp Client Webhook Delivery** outbox.
Retryable transport errors, 429, and 5xx responses are retried; other 4xx
responses are terminal. A System Manager can invoke
`frappe_whatsapp.utils.client_delivery.retry_client_event` with the delivery
name for a manual retry. The legacy status log remains only to drain records
created before this release.

Each attempt includes:

- `X-WhatsApp-App-ID`
- `X-WhatsApp-Event-ID`
- `X-WhatsApp-Timestamp`
- `X-WhatsApp-Signature: sha256=<hex>`

The signature is HMAC-SHA256 over the exact bytes
`<timestamp>.<request body>`. Receivers should allow at most five minutes of
clock skew. A retry has a new timestamp/signature but the same event ID.
During the compatibility rollout only, an app may leave signature enforcement
off while no secret is configured. Production BSUID delivery requires a
secret and the required-signature flag; do not use unsigned compatibility
mode for direct CRM delivery.

## Outbound API

Use
`/api/method/frappe_whatsapp.frappe_whatsapp.api.v1.messages.send` with an
authenticated user bound to an enabled WhatsApp Client App and an allow-listed
WhatsApp Account. Required fields are `external_reference`, `content_type`,
and at least one of:

- `to`: phone number
- `recipient`: regular or parent BSUID

When both are supplied, `to` wins. The tuple client app, account, and external
reference is idempotent and serialized across workers before the Meta request.
Authentication/OTP templates require `to`; Meta does not allow BSUID-only
authentication delivery. A successful response has this additive shape:

```json
{
  "ok": true,
  "message_name": "local-message-name",
  "provider_message_id": "wamid...",
  "external_reference": "crm-message-123",
  "status": "Success",
  "recipient_type": "user_id",
  "recipient": "US.AbCd1234",
  "idempotent_replay": false
}
```

Retries with the same tuple return the original result with
`idempotent_replay=true` and do not send another Meta message.

`request_contact_info` on the same API module sends Meta's contact-information
request. A resulting contact message with `origin=contact_request` is the only
contact-card flow that automatically links the shared phone to the sender.
WhatsApp Chat exposes the same operation as **Request contact info** in the
room actions.

The blocking API accepts a phone or regular `user_id`, never a parent ID.
Calling APIs accept `phone_number` or `recipient`; permission queries map these
to Meta's `user_wa_id` and `recipient` parameters respectively.

## Configuration and Meta test checklist

1. Set **Business Portfolio ID** on every WhatsApp Account. Accounts in the
   same portfolio must use exactly the same value.
2. On WhatsApp Client App, set the API user, allowed accounts, inbound/status
   URLs, webhook secret, required-signature flag, and event subscriptions.
3. Put the same app ID and secret in the Zoni site configuration as
   `whatsapp_inbound_app_id` and `whatsapp_inbound_webhook_secret`. Enable
   `whatsapp_require_signed_webhooks` only after a signed sandbox delivery has
   succeeded.
4. In Meta App Dashboard, subscribe and test both `messages` and
   `user_id_update` for every WABA/phone number.
5. Run Meta's four supplied message scenarios: phone plus BSUID, username with
   no phone, username with phone, and parent BSUID. Also test delivered/read
   and failed statuses, nested ID rotation, and a `REQUEST_CONTACT_INFO` reply.
6. Confirm one profile and the expected scoped aliases exist, then verify the
   message, chat room, signed Zoni delivery, and exactly one CRM lead.

Recorded payloads for these checks live in
`frappe_whatsapp/tests/fixtures/bsuid/`.

## Migration and rollback

Before deployment, take database backups and run a read-only inventory for
duplicate phones/BSUIDs by account and portfolio. Then run `bench migrate` for
`frappe_whatsapp`, `whatsapp_chat`, and `zoni_edu`. The identity patch is
idempotent: it backfills phone aliases and account consent state, links
messages, seeds client account allow-lists, marks historical messages as
already forwarded, and splits ambiguous legacy profiles when their accounts
belong to different fallback scopes. Run the patch twice in staging and
compare profile, alias, message, route, consent, block, call, and room counts.

Do not roll back by deleting aliases or profile links. Application rollback is
performed by restoring the previous code and client URLs; retain the additive
columns/tables so old code can ignore them and new code can be redeployed
without identity loss. Restore a database backup only if the schema/data
migration itself must be undone. Migration writes do not publish client
events.

## CRM team handoff

The decision-complete CRM schema, endpoint, migration, conflict-quarantine,
outbound, calling, and direct-cutover contract is in
[Zoni CRM Service: business-scoped WhatsApp user IDs](./zoni-crm-bsuid.md).

Do not deploy BSUID-only delivery until `zoni_crm_service` has all of these
backward-compatible changes:

1. Widen `PROVIDER_USER_ID` to `VARCHAR(191)` and allow WABAZ intake without a
   phone.
2. Add WABAZ to provider-ID-capable channels.
3. Add a provider identity-alias table unique on
   `(crm_provider_channel_id, identity_type, identity_value)`. Backfill phone
   aliases and use this table as the deduplication authority.
4. Accept `providerIdentityAliases`, `providerUserHandle`, and
   `providerEventId` on lead creation. Link aliases only when one explicit
   event supplies their association. Quarantine a linking event when its
   aliases already resolve to multiple CRM leads; do not auto-merge leads.
5. Implement `apis/leads/providerUser/identityUpdate`, retaining previous and
   current regular/parent aliases on the same lead.
6. Select phone `to` when available and otherwise BSUID `recipient` for
   outbound messages/calls. Remove phone-only UI/validation assumptions.
7. Deduplicate inbound/status/identity events before returning success.

No CRM source code is changed by the frappe_whatsapp implementation.

## PBX team handoff

Phone calls retain the current numeric AMI destination. A BSUID-only call uses
the configured **BSUID Destination Extension** and includes:

- `WHATSAPP_CALL_ID=<local call document>`
- `WHATSAPP_RECIPIENT_KIND=user_id|parent_user_id`
- `WHATSAPP_RECIPIENT_B64=<unpadded base64url UTF-8 BSUID>`

The dialplan/bridge must decode the value and originate the WhatsApp leg using
Meta's `recipient`. The reviewed FreePBX fragment and maintenance-window
procedure are maintained in the companion `asterisk-voice-agent-bridge`
repository at `docs/whatsapp-bsuid-calling.md`.

The deployed contract is `whatsapp-bsuid@from-internal`. It accepts only
unpadded base64url metadata, validates the decoded value against the same
regular/parent BSUID formats used by this application, and dials
`PJSIP/<recipient>@Meta-WhatsApp`. Configure **Destination Context** as
`from-internal` and **BSUID Destination Extension** as `whatsapp-bsuid`.
The existing numeric destination template remains unchanged.

`PBX Queued` proves only that AMI accepted the originate request. Treat BSUID
calling as unavailable until PBX channel evidence and an observer confirm that
an approved BSUID-only test user's WhatsApp handset rang end to end.

## Rollout and direct CRM cutover

1. Back up databases and run a read-only collision inventory by account,
   portfolio, phone, and BSUID.
2. Deploy CRM alias/schema support and the PBX contract first.
3. Deploy the Zoni receiver with signature verification optional, then migrate
   frappe_whatsapp and whatsapp_chat.
4. Populate Business Portfolio IDs, client API users/account allow-lists, and
   the same webhook secret in frappe_whatsapp and Zoni site configuration.
5. Set `whatsapp_require_signed_webhooks`, subscribe `user_id_update`, and test
   phone-plus-BSUID, BSUID-only, parent-ID, rotation, status, blocking,
   contact-request, and calling scenarios in the sandbox.

For direct integration, CRM implements these same signed endpoints. Repoint
the WhatsApp Client App inbound/status URLs from Zoni to CRM; Meta remains
subscribed only to frappe_whatsapp. Roll back by restoring the Zoni URLs.
Stable event IDs make the switch safe for at-least-once delivery. After seven
clean days, disable the Zoni WhatsApp routes, retain them for one release, then
remove only that bridge.
