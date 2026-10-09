# Zoni CRM Service: business-scoped WhatsApp user IDs

This document is the implementation handoff for the `zoni_crm_service` team.
It describes the CRM work required to accept WhatsApp contacts that have a
phone number, a business-scoped user ID (BSUID), a parent BSUID, or any
explicitly linked combination of those identifiers.

The initial production path remains:

```text
Meta -> frappe_whatsapp -> zoni_edu -> zoni_crm_service
```

After the initial path is stable, CRM can receive the same signed version-2
events directly from `frappe_whatsapp`. Meta must remain subscribed only to
`frappe_whatsapp`; CRM must not add a second Meta webhook.

This is a CRM-owned implementation. No `zoni_crm_service` source is changed by
the Frappe BSUID work.

## Reviewed compatibility baseline

This contract was reviewed against these revisions:

| Repository | Branch | Revision |
| --- | --- | --- |
| `frappe_whatsapp` | `feature/business-scoped-user-ids` | `212a75b79f34` |
| `zoni_edu` | `feature/business-scoped-user-ids` | `88386c0468e3` |
| `whatsapp_chat` | `feature/business-scoped-user-ids` | `50cf336ab4cc` |
| `asterisk-voice-agent-bridge` | `feature/business-scoped-user-ids` | `aebe9aad9f24` |
| `zoni_crm_service` | `main` | `56baa262dfae` |

Revalidate request and response examples if any of these revisions changes.

## Ownership and trust boundaries

- `frappe_whatsapp` owns the Meta webhook, identity resolution, Meta access
  tokens, outbound WhatsApp API calls, and the AMI originate request.
- `zoni_edu` verifies Frappe webhook signatures, durably deduplicates events,
  translates version-2 events into the current CRM API, and returns success to
  Frappe only after CRM accepts the event.
- CRM owns leads, CRM-scoped aliases, messages, event deduplication, conflict
  reconciliation, outbound intent, and the agent UI.
- CRM calls Frappe for WhatsApp messages and calls. It must never call Meta,
  Asterisk, or FreePBX directly and must not store Meta, AMI, or SIP secrets.
- `whatsapp_chat` is not a CRM dependency. Its phone-optional display and
  profile/account behavior are a reference for the corresponding CRM UX.
- The Go voice-agent bridge is not in the outbound WhatsApp call path. Its
  repository owns the reviewed FreePBX BSUID dialplan and operator runbook.

## Identity rules

CRM must use the following rules consistently in intake, search, messaging,
calling, status handling, and display:

1. The stable CRM record is the lead, not a phone number or username.
2. Identity is scoped by `CRM_PROVIDER_CHANNEL_ID`. Never match an alias from
   one channel to a lead in another channel.
3. Supported alias types are `phone`, `user_id`, and `parent_user_id`.
4. Normalize phones to one E.164 form before lookup or storage.
5. Treat BSUIDs as case-sensitive ASCII values. A regular BSUID matches
   `^[A-Z]{2}\.[A-Za-z0-9]{1,128}$`; a parent BSUID matches
   `^[A-Z]{2}\.ENT\.[A-Za-z0-9]{1,128}$`.
6. `username` is mutable display metadata and is never a deduplication key.
7. An inbound `providerUserId` is selected in this order: regular `user_id`,
   parent `parent_user_id`, then phone.
8. For outbound operations, use a phone when available; otherwise use the
   active regular BSUID, then the active parent BSUID.
9. Retain rotated BSUIDs as inactive aliases. Historical events must continue
   to resolve through them.
10. Link aliases only when one explicit Meta event supplies them together, an
    identity-update event links the previous and current values, or a
    `REQUEST_CONTACT_INFO` response explicitly links the voluntarily shared
    phone to the sender.

Do not infer a phone from a BSUID, merge by username, or merge across provider
channels even when values happen to match.

## Required CRM data model

Apply equivalent changes to both the CRM base schema and its production delta
migration.

The current CRM implementation points that require coordinated changes are:

- `web/src/migration/tables.sql` and `web/src/migration/delta.sql` for the
  additive schema and backfill;
- `web/src/entities/channel.ts`, `web/src/repository/users.ts`, and
  `web/src/services/leadCreateUsingContactUs.ts` for phone-optional identity
  intake and alias-based deduplication;
- `web/src/provider/zoniEdu.ts`, `web/src/services/messageCreate.ts`, and
  `web/src/services/whatsappCall.ts` for outbound messaging and calling; and
- the lead profile/calling components for display and validation changes.

Keep identity resolution in one service/repository layer shared by the
phase-one API routes and phase-two direct webhooks.

### Lead compatibility fields

- Widen `crm_leads.PROVIDER_USER_ID` from `VARCHAR(50)` to `VARCHAR(191)`.
- Keep `PROVIDER_USER_HANDLE` as display metadata.
- The database already permits a null phone, but the TypeScript `ICrmLead`
  model and validation paths must also make `phone` optional/null-safe.
- Add `WABAZ` to the provider-ID-capable channel list so a BSUID-only contact
  does not require an email or phone.
- Stop deduplicating WABAZ contacts by `(provider, providerUserId)`. The alias
  table below becomes authoritative because provider IDs can rotate.

### Provider identity aliases

Add a table equivalent to `crm_provider_identity_aliases` with:

| Column | Requirement |
| --- | --- |
| Primary key | Auto-increment CRM alias ID |
| `CRM_LEAD_ID` | Required owner lead |
| `CRM_PROVIDER_CHANNEL_ID` | Required identity scope |
| `IDENTITY_TYPE` | `phone`, `user_id`, or `parent_user_id` |
| `IDENTITY_VALUE` | Normalized value, up to 191 ASCII characters |
| `IS_ACTIVE` | Current alias marker; previous rotated IDs remain inactive |
| Audit fields | Existing CRM `CROPER`, `CRTIME`, `LASTOPER`, and `LASTTIME` conventions |

Use a binary/case-sensitive collation for `IDENTITY_VALUE` and enforce:

```text
UNIQUE (CRM_PROVIDER_CHANNEL_ID, IDENTITY_TYPE, IDENTITY_VALUE)
INDEX  (CRM_LEAD_ID, CRM_PROVIDER_CHANNEL_ID, IS_ACTIVE)
```

The unique key applies to inactive aliases too. An old BSUID must never be
reassigned silently to another lead.

### Provider event ledger and conflict quarantine

Add a durable event table equivalent to `crm_provider_events` with:

- provider channel ID and provider event ID;
- event type and SHA-256 hash of the canonical received payload;
- processing state such as `PROCESSING`, `PROCESSED`, `CONFLICT`, or `FAILED`;
- raw JSON payload;
- associated lead/message IDs when known;
- involved lead IDs and conflict reason when quarantined;
- attempt, error, creation, processing, and update timestamps.

Enforce a unique key on `(CRM_PROVIDER_CHANNEL_ID, PROVIDER_EVENT_ID)`. The
ledger is required even though Zoni already deduplicates events: it protects
CRM-side retries and is reused by the future direct webhook path.

A duplicate event ID with the same payload hash returns its stored result. The
same event ID with a different hash is an integration incident and must not be
processed as a new event.

### Message audit fields

Add fields equivalent to:

- `PROVIDER_RECIPIENT_TYPE`: `phone`, `user_id`, or `parent_user_id`;
- `PROVIDER_RECIPIENT_VALUE`: the exact selected outbound identity;
- `PROVIDER_EXTERNAL_REFERENCE`: the stable reference sent to Frappe.

Keep `crm_messages.PROVIDER_PHONE` phone-only for compatibility. Do not put a
BSUID into that column. Enforce uniqueness of the external reference within a
provider channel when it is present.

CRM must reserve and persist an outgoing message/reference before making the
remote Frappe request. If an HTTP response is lost, retry the same logical send
with the same external reference rather than creating a new reference.

## Phase-one Zoni-to-CRM API contract

Zoni authenticates these requests with its existing Bearer token. CRM routes
continue to use the existing response wrapper:

```json
{
  "success": true,
  "data": {}
}
```

Zoni treats `success: false` as rejection even if the HTTP response itself is
2xx. It then returns 502 to Frappe, leaving the Frappe outbox event retryable.
Return `success: true` only after the CRM transaction has committed or the
event has been durably quarantined.

### Incoming message

```http
POST /apis/contactUs/lead/create
Authorization: Bearer <API_SECRET_FOR_APPS>
Content-Type: application/json
```

Example BSUID-only request produced by `zoni_edu`:

```json
{
  "firstName": "example_user",
  "middleName": "",
  "lastName": "",
  "message": "Hello",
  "crmProviderChannelUid": "11111111-2222-3333-4444-555555555555",
  "providerMessageId": "wamid.example",
  "providerUserId": "US.AbCd1234",
  "providerUserHandle": "example_user",
  "providerIdentityAliases": [
    {"type": "user_id", "value": "US.AbCd1234"},
    {"type": "parent_user_id", "value": "US.ENT.Parent1234"}
  ],
  "providerEventId": "2fbd6f6ebfbf3b2b7f5be2da105a321f",
  "ip": "",
  "providerUserJSON": {
    "event": "whatsapp.incoming",
    "message": {},
    "payload": {}
  },
  "providerMessageJson": {}
}
```

When known, Zoni also supplies `phone` in E.164 form and includes a phone
entry in `providerIdentityAliases`. `phone` must remain optional. The route
must accept and validate:

- `providerEventId`;
- `providerIdentityAliases[]` objects containing `type` and `value`;
- `providerUserHandle`;
- `providerUserId` up to 191 characters.

The success body must retain the identifiers already consumed by Zoni:

```json
{
  "success": true,
  "data": {
    "crmLeadId": 123,
    "crmLeadUid": "lead-uid",
    "crmUserId": 456,
    "crmMessageId": 789,
    "eventStatus": "processed"
  }
}
```

### Identity rotation

Implement this currently missing endpoint:

```http
POST /apis/leads/providerUser/identityUpdate
Authorization: Bearer <API_SECRET_FOR_APPS>
Content-Type: application/json
```

Zoni sends:

```json
{
  "crmProviderChannelUid": "11111111-2222-3333-4444-555555555555",
  "providerEventId": "86d6fd68fbb80a50f5aa95f980d45daf",
  "previousUserId": "US.Old1234",
  "currentUserId": "US.New1234",
  "previousParentUserId": "US.ENT.OldParent",
  "currentParentUserId": "US.ENT.NewParent",
  "username": "new_handle",
  "identity": {
    "profile_id": "whatsapp-profile-name",
    "phone": null,
    "user_id": "US.New1234",
    "parent_user_id": "US.ENT.NewParent",
    "username": "new_handle",
    "preferred_recipient": "US.New1234"
  }
}
```

Resolve the lead through any previous or current alias in the same provider
channel. Insert the new aliases, mark superseded IDs inactive, retain all old
aliases, update the lead's compatibility `PROVIDER_USER_ID` and handle, and
record the event atomically. A missing previous value is valid.

### Message status

```http
POST /apis/leads/message/statusUpdate
Authorization: Bearer <API_SECRET_FOR_APPS>
Content-Type: application/json
```

Example request:

```json
{
  "crmProviderChannelUid": "11111111-2222-3333-4444-555555555555",
  "providerMessageId": "wamid.example",
  "status": "delivered",
  "time": "2026-10-01T12:00:00Z",
  "oper": "SYSTEM",
  "statusJson": {
    "event": "whatsapp.message_status",
    "eventId": "7ef59a82fb6580918eb8f03d51b12851",
    "messageName": "local-frappe-message",
    "sourceApp": "crm-client-app",
    "externalReference": "crm-message-reference",
    "previousStatus": "sent",
    "currentStatus": "delivered",
    "normalizedStatus": "delivered"
  }
}
```

Use `statusJson.eventId` as the phase-one event ID. Match the message by
provider channel plus `providerMessageId`, falling back to the stored external
reference when necessary. Phone is not a valid status correlation key.
Repeated or out-of-order statuses must not create messages or regress a later
terminal status.

## Transactional identity intake

Process an incoming or identity-update event in one database transaction:

1. Resolve `crmProviderChannelUid` to one provider channel.
2. Normalize and validate every supplied alias. Reject duplicate alias types
   with conflicting values in one request.
3. Claim `(channel, providerEventId)` in the provider-event ledger and store
   the payload hash and raw payload.
4. Lock the matching alias rows in a deterministic order.
5. When no alias resolves, create one lead and attach all explicitly linked
   aliases.
6. When every alias resolves to one lead, reuse that lead and attach any new
   aliases.
7. When aliases resolve to multiple leads, do not create or merge a lead and
   do not reassign aliases. Mark the event `CONFLICT`, store the involved lead
   IDs and raw evidence, and return a committed success response containing
   `eventStatus: "conflict_quarantined"`.
8. Create or update the CRM message only after identity resolution succeeds.
9. Mark the event `PROCESSED` and commit before returning `success: true`.

CRM does not currently have a safe general lead-merge service, so automatic
lead merging is outside this release. Provide an administrator-only conflict
view or report plus a reconciliation action that:

- selects a canonical lead;
- explicitly reassigns or closes the conflicting CRM data under CRM policy;
- attaches the aliases to the canonical lead;
- resets the stored event for replay; and
- replays the original payload exactly once without changing its event ID.

Quarantine is an accepted durable state, not data loss. The raw event remains
available until reconciliation and replay succeed.

## Outbound messages

### Existing direct-create integration

`POST /api/resource/WhatsApp Message` also returns known recipient identities
in its `data` document. For new outgoing messages sent using `to`, Frappe
fills `recipient_user_id` and `recipient_parent_user_id` from the resolved
contact profile when Meta's immediate send response does not supply them.
When `recipient` was omitted, it contains the regular BSUID, falling back to
the parent BSUID. An explicitly supplied `recipient` is preserved. The field
is spelled `recipient`, not `receipient`.

The phone still takes precedence for sending when `to` and `recipient` are
both present. These response fields are persisted on the message before the
creation response is returned. Later saves do not refresh this snapshot from
the profile; status webhooks can still update the recipient identity fields
with Meta-reported values. If neither the scoped profile nor Meta knows a
BSUID, the identity fields remain empty. There is no backfill of older
messages and no BSUID inferred from a phone alone.

### Recommended idempotent send API

Replace the legacy `POST /api/resource/WhatsApp Message` integration with:

```http
POST /api/method/frappe_whatsapp.frappe_whatsapp.api.v1.messages.send
Authorization: token <frappe-api-key>:<frappe-api-secret>
Content-Type: application/json
```

The authenticated Frappe user must be bound to exactly one enabled **WhatsApp
Client App**, and the selected WhatsApp Account must be in that app's allowed
accounts.

Example BSUID-only send:

```json
{
  "external_reference": "crm-message-550e8400-e29b-41d4-a716-446655440000",
  "content_type": "text",
  "recipient": "US.AbCd1234",
  "whatsapp_account": "18299477544",
  "source_app": "crm-client-app",
  "message": "Hello"
}
```

Supported destination fields are:

| Field | Meaning |
| --- | --- |
| `to` | Phone destination |
| `recipient` | Regular or parent BSUID |

At least one is required. If both are supplied, Frappe sends `to`, matching
Meta's precedence rule. CRM should normally send only the selected field.

Other supported request fields are `content_type`, `attachment_url`,
`template`, `body_param`, and `buttons`. Authentication/OTP templates require
`to`; reject a BSUID-only authentication send in CRM before calling Frappe.

A successful Frappe method response wraps this result in `message`:

```json
{
  "message": {
    "ok": true,
    "message_name": "local-frappe-message",
    "provider_message_id": "wamid.example",
    "external_reference": "crm-message-550e8400-e29b-41d4-a716-446655440000",
    "status": "Success",
    "recipient_type": "user_id",
    "recipient": "US.AbCd1234",
    "idempotent_replay": false
  }
}
```

Idempotency is scoped by client app, WhatsApp Account, and external reference.
Persist the external reference before the first request and reuse it after a
timeout or retry. Store the returned Frappe message name, provider message ID,
selected recipient type/value, status, and replay flag.

## Outbound calling

Use the existing calling endpoints:

```text
GET  /api/method/frappe_whatsapp.frappe_whatsapp.api.calling.get_call_state
POST /api/method/frappe_whatsapp.frappe_whatsapp.api.calling.request_call_permission
POST /api/method/frappe_whatsapp.frappe_whatsapp.api.calling.start_outbound_call
```

Every request requires one identity:

- `phone_number` for a phone; or
- `recipient` for a regular or parent BSUID.

When both are supplied, `phone_number` wins. CRM must resolve
`agent_extension` on the server from the authenticated agent; never trust a
browser-supplied extension. Continue to send the channel's WhatsApp Account,
the CRM client app as `source_app`, and the lead UID as
`external_reference`.

Permission and call-start mutations also require an `idempotency_key`. Store
the key with the logical action and reuse it after an uncertain response. A
new intentional permission request or call attempt gets a new key.

For BSUID calls, Frappe safely encodes the identity into AMI variables and
uses `whatsapp-bsuid@from-internal`. CRM must not construct AMI variables,
dialplan extensions, SIP destinations, or dial prefixes. The deployment and
staged-ring procedure is in the `asterisk-voice-agent-bridge` repository at
`docs/whatsapp-bsuid-calling.md`.

`pbx_queued` proves only that AMI accepted the originate. BSUID calling is not
accepted until an approved regular BSUID-only test user rings and Asterisk
evidence correlates that attempt with the Frappe WhatsApp Call ID. Run one
numeric WhatsApp call afterward to prove the existing route is unchanged.

See [the detailed CRM calling guide](./zoni-crm-whatsapp-calling.md) for the
authentication, permission state machine, and production configuration.

## CRM user experience

Remove phone-only assumptions from WABAZ lead validation, chat, and calling.
Use this display fallback:

```text
lead name -> provider username -> phone -> masked BSUID
```

Mask a BSUID so the UI does not expose the full opaque identifier in list
views or notifications. The full value may be available in an authorized
technical detail view for support.

- Permit WABAZ leads and conversations without a phone.
- Enable WhatsApp messaging and calling when a usable BSUID exists.
- Disable phone-specific actions when no phone alias exists.
- Label actions as WhatsApp messaging/calling rather than phone messaging or
  phone calling.
- Do not show a BSUID under a label such as **WhatsApp #**.
- Treat username changes as display updates, not new contacts.

## Configuration and secrets

Never commit any value in the following table.

| Phase | System | Setting | Purpose |
| --- | --- | --- | --- |
| 1 | CRM | `API_SECRET_FOR_APPS` | Existing Bearer token accepted from Zoni |
| 1 | Zoni site config | `zoni_crm_url` | CRM base URL |
| 1 | Zoni site config | `zoni_crm_token` | Must match the CRM app secret |
| 1 | Frappe Client App | App ID and webhook secret | Signs Frappe-to-Zoni version-2 events |
| 1 | Zoni site config | `whatsapp_inbound_app_id` | Must match the Frappe Client App ID |
| 1 | Zoni site config | `whatsapp_inbound_webhook_secret` | Must match the Frappe webhook secret |
| 1 | Zoni site config | `whatsapp_require_signed_webhooks` | Enable after one signed sandbox delivery succeeds |
| 2 | CRM secret manager | `WHATSAPP_INBOUND_APP_ID` | Expected direct Frappe app ID |
| 2 | CRM secret manager | `WHATSAPP_INBOUND_WEBHOOK_SECRET` | Direct webhook HMAC secret |
| 2 | CRM configuration | WhatsApp Account to provider-channel mapping | Maps event scope to `CRM_PROVIDER_CHANNEL_ID` |

Use distinct secrets for the Zoni-to-CRM Bearer token and the Frappe webhook
HMAC. Rotate them independently. Redact authorization and signature values
from logs.

## Phase-two direct Frappe-to-CRM delivery

After phase one is stable, implement:

```text
POST /apis/webhooks/whatsapp/inbound
POST /apis/webhooks/whatsapp/status
```

The inbound endpoint accepts `whatsapp.incoming` and
`whatsapp.identity_updated`. The status endpoint accepts
`whatsapp.message_status`. Both consume the same version-2 envelopes already
received by Zoni and feed the phase-one identity/event services rather than
creating a second implementation.

Treat these envelopes as additive: validate the required fields, preserve the
raw body, and ignore unknown fields rather than rejecting a compatible future
sender.

Representative incoming envelope:

```json
{
  "schema_version": 2,
  "event": "whatsapp.incoming",
  "event_id": "2fbd6f6ebfbf3b2b7f5be2da105a321f",
  "occurred_at": "2026-10-01 12:00:00",
  "app_id": "crm-client-app",
  "whatsapp_account": "18299477544",
  "message": {
    "name": "local-frappe-message",
    "message_id": "wamid.example",
    "from": null,
    "from_user_id": "US.AbCd1234",
    "from_parent_user_id": "US.ENT.Parent1234",
    "username": "example_user",
    "contact_profile": "whatsapp-profile-name",
    "identity": {
      "profile_id": "whatsapp-profile-name",
      "phone": null,
      "user_id": "US.AbCd1234",
      "parent_user_id": "US.ENT.Parent1234",
      "username": "example_user",
      "preferred_recipient": "US.AbCd1234"
    },
    "whatsapp_account": "18299477544",
    "content_type": "text",
    "message": "Hello",
    "timestamp": "2026-10-01 12:00:00",
    "contact_payload": null,
    "contact_origin": null
  }
}
```

Representative identity-update envelope:

```json
{
  "schema_version": 2,
  "event": "whatsapp.identity_updated",
  "event_id": "86d6fd68fbb80a50f5aa95f980d45daf",
  "occurred_at": "2026-10-01 12:05:00",
  "app_id": "crm-client-app",
  "whatsapp_account": "18299477544",
  "identity": {
    "profile_id": "whatsapp-profile-name",
    "phone": null,
    "user_id": "US.New1234",
    "parent_user_id": "US.ENT.NewParent",
    "username": "new_handle",
    "preferred_recipient": "US.New1234"
  },
  "previous_user_id": "US.Old1234",
  "previous_parent_user_id": "US.ENT.OldParent"
}
```

Representative status envelope:

```json
{
  "schema_version": 2,
  "event": "whatsapp.message_status",
  "event_id": "7ef59a82fb6580918eb8f03d51b12851",
  "occurred_at": "2026-10-01 12:06:00",
  "app_id": "crm-client-app",
  "whatsapp_account": "18299477544",
  "message": {
    "name": "local-frappe-message",
    "message_id": "wamid.example",
    "external_reference": "crm-message-reference",
    "source_app": "crm-client-app",
    "to": "",
    "recipient": "US.AbCd1234",
    "recipient_user_id": "US.AbCd1234",
    "recipient_parent_user_id": "",
    "contact_profile": "whatsapp-profile-name",
    "identity": {
      "profile_id": "whatsapp-profile-name",
      "phone": null,
      "user_id": "US.AbCd1234",
      "parent_user_id": null,
      "username": "example_user",
      "preferred_recipient": "US.AbCd1234"
    },
    "previous_status": "sent",
    "current_status": "delivered",
    "normalized_status": "delivered"
  }
}
```

### Signature verification

Read the raw request bytes before JSON parsing and require:

```text
X-WhatsApp-App-ID
X-WhatsApp-Event-ID
X-WhatsApp-Timestamp
X-WhatsApp-Signature: sha256=<lowercase hex digest>
```

Reject the request unless:

1. the app ID matches the configured value;
2. the header event ID exactly matches the body `event_id`;
3. the timestamp is an integer no more than 300 seconds from CRM time; and
4. the signature matches HMAC-SHA256 over the exact bytes
   `<timestamp>.<raw request body>` using a constant-time comparison.

Do not parse and reserialize the JSON before verifying it. A retry has a new
timestamp and signature but the same stable event ID and payload.

### Direct response semantics

| Result | HTTP behavior |
| --- | --- |
| Committed event | 2xx |
| Same event ID and payload hash | 2xx with `deduplicated: true` |
| Durably quarantined conflict | 2xx with `conflict_quarantined: true` |
| Invalid event schema or unknown account mapping | 400 |
| Invalid app ID/signature | 401 or 403 |
| Same event ID with a different payload hash | 409 and alert |
| Temporary backpressure | 429 |
| Temporary database/server failure | 5xx |

Frappe retries transport errors, 429, and 5xx responses. Other 4xx responses
are terminal and require correction plus a manual retry from the Frappe
delivery record.

### Cutover and rollback

1. Validate all recorded BSUID fixtures against the direct endpoints in the
   sandbox while production still points to Zoni.
2. Confirm CRM event counts, lead resolution, messages, statuses, and
   quarantined conflicts match the Zoni path.
3. Update both the inbound and status URLs on the Frappe Client App to the CRM
   endpoints as one maintenance-window change.
4. Keep the same app ID, event subscriptions, allowed accounts, and webhook
   secret; require signatures.
5. Monitor event latency, duplicates, conflicts, terminal failures, and
   retryable failures.
6. Roll back by restoring both URLs to Zoni. Stable event IDs prevent a replay
   from creating duplicate CRM messages.
7. After seven clean days, disable the Zoni WhatsApp forwarding routes. Keep
   them for one release as rollback code before removing only that bridge.

Meta's webhook destination does not change during this cutover.

## Migration procedure

1. Back up the CRM database.
2. Run a read-only collision report grouped by provider channel, normalized
   phone, regular BSUID, and parent BSUID.
3. Resolve malformed values and document collisions before adding the unique
   alias constraint.
4. Deploy the additive tables and columns first.
5. Backfill WABAZ phone aliases and any valid existing provider IDs in bounded
   batches. Do not emit provider events during backfill.
6. Deploy alias-based intake, event deduplication, identity updates, and
   phone-optional UI behavior.
7. Run the migration/backfill a second time and verify that counts and alias
   ownership do not change.
8. Enable BSUID-only sandbox delivery before production delivery.

Do not roll back by deleting aliases or event records. An application rollback
should leave the additive CRM schema in place so old code can ignore it and a
fixed release can reuse the identity history.

## Acceptance checklist

### Identity and intake

- [ ] Phone plus regular BSUID creates one lead with both aliases.
- [ ] BSUID-only username creates a lead and message without a phone.
- [ ] Regular and parent BSUIDs attach to one lead.
- [ ] Username changes update display metadata without creating a lead.
- [ ] BSUID rotation retains old/new aliases and message continuity.
- [ ] A contact-request reply links the voluntarily shared phone.
- [ ] No alias resolves across provider channels.
- [ ] Concurrent duplicate events create exactly one lead and message.
- [ ] Multi-lead alias collisions quarantine without creating another lead.
- [ ] Reconciliation replays the quarantined event exactly once.

Use the recorded fixtures under `frappe_whatsapp/tests/fixtures/bsuid/` for
phone-plus-BSUID, BSUID-only, parent-ID, rotation, statuses, and
contact-request scenarios.

### Messaging and status

- [ ] Phone-only, BSUID-only, and both-destination sends work.
- [ ] Phone wins when both `to` and `recipient` are supplied.
- [ ] Authentication templates reject BSUID-only delivery before Meta.
- [ ] A timeout replay uses the same external reference and does not resend.
- [ ] Provider and local Frappe message IDs are persisted.
- [ ] Statuses match without a phone and do not regress out of order.
- [ ] Duplicate and concurrent status events are idempotent.

### Calling and UI

- [ ] State, permission, and start calls work with phone and BSUID identities.
- [ ] Permission and call idempotency keys survive uncertain retries.
- [ ] The approved staged regular-BSUID call rings through the PBX route.
- [ ] A numeric WhatsApp call proves the existing route is unchanged.
- [ ] Phone-optional leads render, message, and call without misleading labels.

### Direct webhook phase

- [ ] Valid signatures succeed using the exact raw body.
- [ ] Wrong app IDs, signatures, and expired timestamps fail.
- [ ] Event ID/header mismatches and changed-payload collisions fail closed.
- [ ] Duplicate events return the previous committed outcome.
- [ ] 429/5xx retry behavior and terminal 4xx handling are verified.
- [ ] URL cutover and rollback do not create duplicate CRM records.

BSUID production delivery is ready only after phase-one CRM acceptance passes.
Direct CRM delivery is a separate later acceptance gate.
