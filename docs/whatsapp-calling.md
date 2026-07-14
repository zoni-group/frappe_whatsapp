# WhatsApp Outbound Calling

`frappe_whatsapp` can request WhatsApp call permission, poll Meta for the
permission state, and ask Asterisk/FreePBX to originate an outbound call.

There are two deliberately separate agent modes:

- Frappe Desk and `whatsapp_chat` supply `agent_user`. The service resolves its
  extension from an enabled **WhatsApp Call Agent** record.
- Server-to-server CRM calls supply `agent_extension`. No Frappe User or
  **WhatsApp Call Agent** record is required, and `agent_user` remains empty on
  the **WhatsApp Call** audit record.

An operation cannot use both modes.

## CRM API authentication and trust boundary

Assign the **WhatsApp Calling API** role to the Frappe integration user used by
the CRM backend. **System Manager** is also accepted. All three methods reject
Guest access.

The CRM backend must derive `agent_extension` from its authenticated user
session and authoritative user-to-extension mapping. It must never forward an
extension selected or supplied by browser input. The extension is restricted
to 1–10 ASCII digits; whitespace, line breaks, AMI delimiters, SIP/PJSIP
channels, contexts, and other characters are rejected before calling Meta or
AMI.

Meta tokens, AMI credentials, channel templates, dial contexts, and destination
templates stay in ERPNext. They are not API inputs or response fields.

Use token authentication for the integration user:

```http
Authorization: token <api_key>:<api_secret>
Content-Type: application/json
```

Frappe wraps every returned object in a top-level `message` property.

## Common request fields

| Field | Required | Description |
| --- | --- | --- |
| `phone_number` | Yes | Destination in international format. It is normalized and must contain 8–15 digits. |
| `whatsapp_account` | Yes | Exact active **WhatsApp Account** name. CRM must pass its channel ID; no fallback account is inferred. |
| `agent_extension` | Yes | CRM server-resolved PBX extension, 1–10 ASCII digits. |
| `source_app` | Yes | Exact enabled **WhatsApp Client App** name for the CRM. |
| `external_reference` | No | CRM lead UID or another printable external identifier, up to 140 characters. |
| `idempotency_key` | Mutations only | Caller-generated unique key, normally a UUID. It must be 8–140 allowed characters. |

The API does not accept `agent_email` or `agent_user`.

## Read call state

```http
GET /api/method/frappe_whatsapp.frappe_whatsapp.api.calling.get_call_state
```

Supply the common fields as query parameters. A typical response is:

```json
{
  "message": {
    "ok": true,
    "status": "permission_required",
    "message": "Call permission is required before dialing.",
    "retryable": false,
    "can_request_permission": true,
    "can_start_call": false,
    "permission": {
      "status": "No Permission",
      "expires_at": null,
      "last_checked_at": "2026-07-14 12:00:00"
    },
    "permission_status": "No Permission",
    "permission_expires_at": null,
    "permission_last_checked_at": "2026-07-14 12:00:00",
    "pending_call_id": null,
    "call_id": null,
    "agent_extension": "847",
    "whatsapp_account": "crm-channel-id",
    "source_app": "crm-client-app",
    "external_reference": "lead-uid",
    "idempotency_key": null
  }
}
```

## Request call permission

```http
POST /api/method/frappe_whatsapp.frappe_whatsapp.api.calling.request_call_permission
```

```json
{
  "phone_number": "+15551234567",
  "whatsapp_account": "crm-channel-id",
  "agent_extension": "847",
  "source_app": "crm-client-app",
  "external_reference": "lead-uid",
  "idempotency_key": "9a028213-4b64-4963-9d66-f5c8fe824e59"
}
```

The permission-template **WhatsApp Message** retains `source_app` and
`external_reference`, allowing normal CRM routing and message-status
correlation. The account/phone lock prevents duplicate permission templates.

## Start an outbound call

```http
POST /api/method/frappe_whatsapp.frappe_whatsapp.api.calling.start_outbound_call
```

The request body uses the same fields as the permission request, with a new
idempotency UUID. The service rechecks permission with Meta immediately before
AMI origination and requires Meta's `start_call` action to be allowed.

A successful originate response uses status `pbx_queued`:

```json
{
  "message": {
    "ok": true,
    "status": "pbx_queued",
    "message": "Calling your PBX extension now.",
    "retryable": false,
    "can_request_permission": false,
    "can_start_call": false,
    "pending_call_id": null,
    "call_id": "8h1d2k3m4n",
    "agent_extension": "847",
    "whatsapp_account": "crm-channel-id",
    "source_app": "crm-client-app",
    "external_reference": "lead-uid",
    "idempotency_key": "112dd981-994a-40ac-96bc-8c64c1a87bec"
  }
}
```

`pbx_queued` means AMI accepted the Originate action. It does not prove that the
extension exists, rang, answered, or connected. An AMI rejection or connection
failure returns `failed` with `failure_reason`; the failed **WhatsApp Call**
audit record is preserved.

## Status and retry behavior

| Status | Consumer action |
| --- | --- |
| `permission_required` | Request permission only when `can_request_permission` is true. |
| `permission_pending` | Poll `get_call_state`; do not send another template. |
| `ready` | Enable the Call action when `can_start_call` is true. |
| `unavailable` | Show the message. Retry later only when `retryable` is true. |
| `pbx_queued` | Originate was accepted by AMI; show queued/dialing state, not connected. |
| `failed` | Show `failure_reason`. A deliberate retry must use a new idempotency key. |

An exact mutation retry with the same action, phone, account, and extension
returns the existing result and sets `idempotent_replay`. Reusing that key with
different bound values is an idempotency conflict. Polling remains the only CRM
permission update mechanism; this feature adds no CRM webhook.

Recommended sequence:

1. Load `get_call_state`.
2. If allowed, request permission with a newly generated UUID.
3. Poll state while `permission_pending`.
4. When `ready` and `can_start_call` is true, start the call with another new
   UUID.

## FreePBX/Asterisk setup

- Enable WhatsApp Business Calling for the Meta app and WhatsApp phone number.
- Configure WhatsApp SIP calling in FreePBX/Asterisk following Meta's SIP guide.
- Create or verify an outbound route that can dial WhatsApp users from an
  internal extension.
- Create a restricted AMI user with Originate permission.
- Manually test the route from an agent extension before enabling calling.

## Frappe setup

- Open **WhatsApp Calling Settings** and enable calling.
- Select an approved WhatsApp template marked **Is Call Permission Request**.
- Enter AMI host, port, username, password, and TLS setting.
- Configure **Agent Channel Template**, **Destination Context**, and
  **Destination Number Template**.
- Keep **WhatsApp Call Agent** records for Desk/`whatsapp_chat` users only.

`{number}` is the WhatsApp number without a leading `+`; `{e164}` includes the
leading `+`; `{extension}` is the validated PBX extension.
