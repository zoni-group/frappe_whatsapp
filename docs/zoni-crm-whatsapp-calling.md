# Zoni CRM Service: WhatsApp outbound calling integration

This guide is the handoff contract for the `zoni_crm_service` maintainers. The
CRM backend calls the production Zoni Frappe API to:

1. check whether a WhatsApp user has granted call permission;
2. send a localized call-permission request when Meta allows it; and
3. ask the PBX to originate a call between a CRM agent's extension and the
   WhatsApp user.

The CRM does not call Meta, Asterisk, or FreePBX directly. It must not receive
or store the Meta access token, AMI credentials, SIP configuration, or PBX dial
prefix. Those details remain in Frappe.

## Production contract

| Setting | Production value |
| --- | --- |
| API base URL | `https://www.zoni.edu` |
| WhatsApp account used for calling | `18299477544` |
| CRM source app | `zoni_crm_epTzT5AjLF6LdIBht7b1RydPAsH3LiIk` |
| Example WhatsApp user | `+12012345678` |
| Example PBX extension | `847` |
| Required Frappe role | `WhatsApp Calling API` |

Use `https://www.zoni.edu` directly. `https://zoni.edu` currently redirects to
the `www` host, and HTTP clients may remove the `Authorization` header during a
cross-host redirect.

The calling WhatsApp account is `18299477544`. Do not substitute the CRM
client app's normal outbound default account (`12129475989`) for these calling
requests; the approved call-permission templates belong to account
`18299477544`.

### Approved call-permission templates

The three production records are approved language variants of the same Meta
template family, `call_permission`:

| API `language_code` | Template record selected | Meta language | Behavior |
| --- | --- | --- | --- |
| omitted, `en`, or `en_US` | `call_permission-en_US` | `en_US` | English; configured default |
| `es` | `call_permission-es-18299477544` | `es` | Spanish |
| `pt` or `pt_BR` | `call_permission-pt_BR-18299477544` | `pt_BR` | Brazilian Portuguese |

Language codes are case-insensitive on input and accept `-` or `_` separators.
The response says which language was actually sent in `language_code`. An
unsupported but well-formed language falls back to `en_US` and returns
`language_fallback: true`. When the field is omitted, the API reports
`requested_language_code: null` and `language_fallback: true` because it used
the configured default. A malformed value is rejected.

## Authentication and trust boundary

Create or use a dedicated Frappe integration user with the **WhatsApp Calling
API** role, then provide its API key and API secret to the CRM service through
the service's secret manager. Every request uses:

```http
Authorization: token <api_key>:<api_secret>
Accept: application/json
Content-Type: application/json
```

These calls belong in the CRM backend, never in browser code. The backend must
resolve `agent_extension` from the authenticated CRM user and an authoritative
user-to-extension mapping. Never trust an extension supplied by the browser.

The API accepts extensions containing only 1 to 10 ASCII digits. It rejects
spaces, newlines, SIP/PJSIP channels, dialplan contexts, and AMI syntax.

## Endpoints

```text
GET  /api/method/frappe_whatsapp.frappe_whatsapp.api.calling.get_call_state
POST /api/method/frappe_whatsapp.frappe_whatsapp.api.calling.request_call_permission
POST /api/method/frappe_whatsapp.frappe_whatsapp.api.calling.start_outbound_call
```

All responses from successful Frappe method dispatch are wrapped in a
top-level `message` object. Application logic should therefore read
`response.message.status`, not `response.status`.

### Common fields

| Field | Required | Meaning |
| --- | --- | --- |
| `phone_number` | Yes | International number; punctuation is removed and 8-15 digits are required |
| `whatsapp_account` | Yes | Exact active WhatsApp Account name; use `18299477544` |
| `agent_extension` | Yes | Extension resolved by the CRM backend |
| `source_app` | Yes | Use the production CRM source app shown above |
| `external_reference` | No | CRM lead UID or another printable correlation ID, up to 140 characters |
| `idempotency_key` | Both POSTs | Caller-generated unique key, preferably a UUID |
| `language_code` | Permission POST only | Optional permission-template language |

## Complete curl example

Set the credentials in the shell used for this example. Do not commit them or
write them to application logs.

```bash
export ZONI_API_KEY='<integration-user-api-key>'
export ZONI_API_SECRET='<integration-user-api-secret>'
export ZONI_AUTH="token ${ZONI_API_KEY}:${ZONI_API_SECRET}"
```

Replace `<crm-lead-uid>` in the examples with the real CRM lead UID, or omit
`external_reference` when there is no CRM record to correlate. The
`source_app` value is a routing identifier, not an authentication credential;
authentication still requires the integration user's API key and secret.

### 1. Check current permission

This request checks Meta's current permission state for `+12012345678`. It does
not send a template and does not originate a call.

```bash
curl --silent --show-error --get \
  'https://www.zoni.edu/api/method/frappe_whatsapp.frappe_whatsapp.api.calling.get_call_state' \
  --header "Authorization: ${ZONI_AUTH}" \
  --header 'Accept: application/json' \
  --data-urlencode 'phone_number=+12012345678' \
  --data-urlencode 'whatsapp_account=18299477544' \
  --data-urlencode 'agent_extension=847' \
  --data-urlencode 'source_app=zoni_crm_epTzT5AjLF6LdIBht7b1RydPAsH3LiIk' \
  --data-urlencode 'external_reference=<crm-lead-uid>'
```

The decision fields are inside `message`:

```json
{
  "message": {
    "status": "permission_required",
    "can_request_permission": true,
    "can_start_call": false,
    "permission_status": "No Permission"
  }
}
```

Do not infer the next action only from `permission_status`. Use the canonical
`status` plus the `can_request_permission` and `can_start_call` booleans, which
also account for Meta's current action limits.

### 2. Request permission

Only do this when `message.can_request_permission` is `true`. Generate a new
UUID for this distinct permission request. The example requests Spanish; use
`en_US` or `pt_BR` to select either of the other approved variants.

```bash
PERMISSION_KEY="$(uuidgen | tr '[:upper:]' '[:lower:]')"

curl --silent --show-error --request POST \
  'https://www.zoni.edu/api/method/frappe_whatsapp.frappe_whatsapp.api.calling.request_call_permission' \
  --header "Authorization: ${ZONI_AUTH}" \
  --header 'Accept: application/json' \
  --header 'Content-Type: application/json' \
  --data-raw "{
    \"phone_number\": \"+12012345678\",
    \"whatsapp_account\": \"18299477544\",
    \"agent_extension\": \"847\",
    \"source_app\": \"zoni_crm_epTzT5AjLF6LdIBht7b1RydPAsH3LiIk\",
    \"external_reference\": \"<crm-lead-uid>\",
    \"language_code\": \"es\",
    \"idempotency_key\": \"${PERMISSION_KEY}\"
  }"
```

A newly sent request normally returns:

```json
{
  "message": {
    "ok": true,
    "status": "permission_pending",
    "can_request_permission": false,
    "can_start_call": false,
    "pending_call_id": "<frappe-whatsapp-call-id>",
    "requested_language_code": "es",
    "language_code": "es",
    "language_fallback": false,
    "idempotency_key": "<permission-uuid>"
  }
}
```

The user must approve the request in WhatsApp. While the status is
`permission_pending`, repeat the GET request from step 1 on a reasonable poll
interval. Do not send another template. This integration intentionally has no
permission-status webhook to `zoni_crm_service`.

If an HTTP attempt times out and the client does not know whether it succeeded,
retry that exact permission POST with the same `PERMISSION_KEY`. The API will
return the stored result with `idempotent_replay: true` when appropriate.

### 3. Start the call from extension 847

Only do this after the state endpoint returns both `status: "ready"` and
`can_start_call: true`. Use a new UUID, different from the permission request's
UUID.

```bash
CALL_KEY="$(uuidgen | tr '[:upper:]' '[:lower:]')"

curl --silent --show-error --request POST \
  'https://www.zoni.edu/api/method/frappe_whatsapp.frappe_whatsapp.api.calling.start_outbound_call' \
  --header "Authorization: ${ZONI_AUTH}" \
  --header 'Accept: application/json' \
  --header 'Content-Type: application/json' \
  --data-raw "{
    \"phone_number\": \"+12012345678\",
    \"whatsapp_account\": \"18299477544\",
    \"agent_extension\": \"847\",
    \"source_app\": \"zoni_crm_epTzT5AjLF6LdIBht7b1RydPAsH3LiIk\",
    \"external_reference\": \"<crm-lead-uid>\",
    \"idempotency_key\": \"${CALL_KEY}\"
  }"
```

The Frappe service rechecks permission with Meta immediately before asking
Asterisk to originate the call. A successful queue response looks like:

```json
{
  "message": {
    "ok": true,
    "status": "pbx_queued",
    "message": "Calling your PBX extension now.",
    "retryable": false,
    "can_request_permission": false,
    "can_start_call": false,
    "call_id": "<frappe-whatsapp-call-id>",
    "agent_extension": "847",
    "whatsapp_account": "18299477544",
    "idempotency_key": "<call-uuid>"
  }
}
```

In the current production routing, Frappe asks Asterisk to call
`Local/847@from-internal` and route the WhatsApp destination through the
server-side calling dialplan. The agent answers extension 847, and the PBX/SIP
integration establishes the WhatsApp leg to `+12012345678`. The CRM must send
the plain WhatsApp number, not the internal dial prefix.

`pbx_queued` means AMI accepted the asynchronous Originate request. It does not
prove that extension 847 exists, rang, answered, or that the WhatsApp user
answered. The current CRM API does not expose subsequent Asterisk or WhatsApp
call-progress events.

## Status handling

| `message.status` | CRM behavior |
| --- | --- |
| `permission_required` | Show/request permission only if `can_request_permission` is true |
| `permission_pending` | Poll state; do not send another permission request |
| `ready` | Enable Call only if `can_start_call` is true |
| `unavailable` | Show `message`; retry later only if `retryable` is true |
| `pbx_queued` | Show queued/dialing, not connected |
| `failed` | Show `failure_reason`; a deliberate new attempt needs a new UUID |

An idempotency key is bound to the action, phone number, WhatsApp account,
extension, and (for permission requests) selected template. Reusing it with
different bound values is an idempotency conflict.

Frappe validation/authentication errors may use a non-2xx response with
`exc_type` and `exception`/`exc` instead of the normal calling result. Treat
those as request or integration failures, not as a permission status.

## CRM implementation checklist

- Keep the Frappe API key and secret server-side.
- Give the integration user the **WhatsApp Calling API** role.
- Resolve extension 847 (or any other extension) from the logged-in CRM user;
  never accept it from the browser.
- Always send calling account `18299477544` and the exact CRM `source_app`.
- Preserve `external_reference` as the CRM lead UID for audit correlation.
- Make the three-step workflow response-driven; do not assume permission after
  sending the template.
- Use a new UUID per distinct mutation and retain it for network retries.
- Do not display `pbx_queued` as answered or connected.

## Documentation references

- [Local `frappe_whatsapp` calling contract](./whatsapp-calling.md)
- [Frappe REST API: token authentication, remote methods, and `message` response wrapper](https://docs.frappe.io/framework/user/en/api/rest)
- [Meta: WhatsApp Cloud API Calling overview](https://developers.facebook.com/documentation/business-messaging/whatsapp/calling)
- [Meta: obtain user call permissions](https://developers.facebook.com/documentation/business-messaging/whatsapp/calling/user-call-permissions)
- [Meta: business-initiated calls](https://developers.facebook.com/documentation/business-messaging/whatsapp/calling/business-initiated-calls)
- [Meta: SIP configuration for WhatsApp Business Calling](https://developers.facebook.com/documentation/business-messaging/whatsapp/calling/sip)
