<div align="right">
	<a href="https://frappecloud.com/marketplace/apps/frappe_whatsapp" target="_blank">
		<picture>
			<source media="(prefers-color-scheme: dark)" srcset="https://frappe.io/files/try-on-fc-white.png">
			<img src="https://frappe.io/files/try-on-fc-black.png" alt="Try on Frappe Cloud" height="28" />
		</picture>
	</a>
</div>

# Frappe WhatsApp

Business-scoped user ID support, client webhook v2, rollout guidance, and the
CRM/PBX handoff are documented in
[docs/business-scoped-user-ids.md](docs/business-scoped-user-ids.md).
The CRM implementation contract is maintained separately in
[docs/zoni-crm-bsuid.md](docs/zoni-crm-bsuid.md).

[Documentation](https://shridarpatil.github.io/frappe_whatsapp/)

WhatsApp integration for Frappe/ERPNext. Use Meta's WhatsApp Cloud API directly without any third-party integration.

[![WhatsApp Video](https://img.youtube.com/vi/nq5Kcc5e1oc/0.jpg)](https://www.youtube.com/watch?v=nq5Kcc5e1oc)

[![YouTube](http://i.ytimg.com/vi/TncXQ0UW5UM/hqdefault.jpg)](https://www.youtube.com/watch?v=TncXQ0UW5UM)

![whatsapp](https://user-images.githubusercontent.com/11792643/203741234-29edeb1b-e2f9-4072-98c4-d73a84b48743.gif)

> **Note:** If you're not using live credentials, follow [step no 2](https://developers.facebook.com/docs/whatsapp/cloud-api/get-started) to add the number on Meta to which you are sending messages.

## Features

- **Multi-Account Support** - Manage multiple WhatsApp Business accounts
- **Two-way Messaging** - Send and receive messages with full conversation tracking
- **Template Management** - Create and sync WhatsApp templates with Meta
- **WhatsApp Flows** - Build interactive forms using WhatsApp's native Flow Builder
- **Interactive Messages** - Send button and list messages for quick user responses
- **WhatsApp Notifications** - Automated notifications triggered by DocType events
- **Bulk Messaging** - Send campaigns to recipient lists with variable substitution
- **Webhook Support** - Real-time message delivery and status updates
- **Media Support** - Send and receive images, documents, videos, and audio files
- **Contact Blocking** - Block spam contacts locally and through Meta's Block Users API
- **Sync from Meta** - Import and sync templates and flows from your Meta Business Account
- **ERPNext Integration** - Native integration with Frappe/ERPNext DocTypes

## Installation

### Step 1: Get the app
```bash
bench get-app https://github.com/shridarpatil/frappe_whatsapp
```

### Step 2: Install on your site
```bash
bench --site [sitename] install-app frappe_whatsapp
```

## Quick Setup

### 1. Get WhatsApp Credentials
Visit [Meta Developer Portal](https://developers.facebook.com/docs/whatsapp/cloud-api/get-started) to set up your WhatsApp Business API.

### 2. Configure WhatsApp Account
Go to **WhatsApp Account** in Frappe and enter your credentials:
- Account Name
- Access Token
- Phone Number ID
- Business Account ID
- App ID
- Webhook Verify Token

![WhatsApp Settings](https://user-images.githubusercontent.com/11792643/198827382-90283b36-f8ab-430e-a909-1b600d6f5da4.png)

### 3. Create Templates
Create WhatsApp templates that are approved by Meta:

![Create Template](https://user-images.githubusercontent.com/11792643/198827355-ebf9c113-f39a-4d37-98f7-38f719fb2d1f.png)

## Core Features

### WhatsApp Notifications

Automatically send WhatsApp messages based on DocType events.

**Supported Triggers:**
- DocType Events: Before/After Insert, Validate, Save, Submit, Cancel, Delete
- Scheduler Events: Hourly, Daily, Weekly, Monthly
- Date-based: Days Before/After a specified date field

![WhatsApp Notification](https://user-images.githubusercontent.com/11792643/198827295-f6d756a3-6289-40b3-99ea-0394efb61041.png)

**Features:**
- Map template parameters to DocType fields
- Add conditions using Python expressions
- Attach document print PDFs or custom files
- Set DocType field values after sending (e.g., mark as notified)
- Support for interactive buttons with dynamic URLs

### Bulk WhatsApp Messages

Send WhatsApp messages to multiple recipients at once.

**Features:**
- Import recipients from any DocType (Customer, Contact, etc.)
- Create recipient lists for reuse
- Variable substitution from recipient data
- Background processing with progress tracking
- Retry failed messages
- Select specific WhatsApp account for sending

**Recipient Types:**
1. **Individual Recipients** - Add recipients directly with their phone numbers
2. **Recipient List** - Use a pre-configured WhatsApp Recipient List

**Variable Types:**
1. **Common** - Same values for all recipients
2. **Unique** - Different values per recipient (from recipient data)

### Direct Messaging

Send messages without templates (within 24-hour window):

![Direct Message](https://user-images.githubusercontent.com/11792643/211518862-de2d3fbc-69c8-48e1-b000-8eebf20b75ab.png)

### Voice Notes

Client apps can create outgoing WhatsApp voice notes directly through the
`WhatsApp Message` DocType. For iPhone-compatible playback, attach a local
Frappe `File` containing Ogg/Opus audio, set `content_type = "audio"`, set
`is_voice_note = 1`, and leave `message` empty unless you intentionally want a
caption.

See [docs/voice-notes.md](docs/voice-notes.md) for the exact format
requirements, a Python example, and a detailed NextJS/Frappe REST API example.

### Interactive Messages

Send interactive button and list messages for quick user responses.

**Button Messages:**
- Up to 3 quick reply buttons
- Users tap to respond instantly
- Great for confirmations, options, and CTAs

**List Messages:**
- Up to 10 items organized in sections
- Users select from a menu
- Ideal for product catalogs, service menus, FAQs

### WhatsApp Flows

Build interactive forms using WhatsApp's native Flow Builder. Flows provide a rich UI experience for collecting structured data from users.

**Features:**
- Visual form builder with multiple screens
- Support for text inputs, dropdowns, date pickers, and more
- Client-only flows (no endpoint required)
- Publish and manage flows directly from Frappe
- Send flow messages and receive responses via webhook

**Supported Field Types:**
- TextInput, TextArea
- Dropdown (with static options)
- DatePicker
- RadioButtonsGroup, CheckboxGroup
- OptIn
- Text display components (TextHeading, TextBody, TextSubheading, TextCaption)

**Flow Actions:**
- Create on WhatsApp - Push new flows to Meta
- Upload Flow JSON - Update flow definition
- Publish - Make flow available to all users
- Deprecate - Mark flow as deprecated
- Send Test - Test draft flows with registered test numbers
- Sync from Meta - Import existing flows back to Frappe

### Sync from Meta

Keep your Frappe data synchronized with your Meta Business Account.

**Templates:**
- Go to **WhatsApp Templates** list view
- Click **Sync from Meta** button
- All templates from active WhatsApp accounts are imported/updated

**Flows:**
- Go to **WhatsApp Flow** list view
- Click **Sync from Meta** button
- Select the WhatsApp Account to sync from
- New flows are imported, existing flows are updated with latest status and JSON

### Custom Data Templates

Send templates using custom data instead of DocType fields:

```python
doc.set("_data_list", [
    {"phone": "+1234567890", "name": "John", "order_id": "ORD-001"},
    {"phone": "+0987654321", "name": "Jane", "order_id": "ORD-002"}
])
```

![Custom Data](https://github.com/user-attachments/assets/7496b081-df2b-41dc-bdcb-ed7e5f464698)

## Webhook Setup

### Configure Webhook on Meta
1. Go to your Meta Developer App
2. Set Webhook URL: `<your-domain>/api/method/frappe_whatsapp.utils.webhook.webhook`
3. Add Verify Token (same as in WhatsApp Account settings)
4. Subscribe to webhook fields:
   - `messages` - to receive incoming messages
   - `message_template_status_update` - for template status updates

### Incoming Messages
Messages received via webhook are automatically created as WhatsApp Message documents:

![Incoming Message](https://user-images.githubusercontent.com/11792643/211519625-a528abe2-ba24-46a4-bcbc-170f6b4e27fb.png)

### Click-to-WhatsApp Campaign Attribution

Inbound messages opened from Facebook or Instagram Click-to-WhatsApp ads
include Meta's `referral` object. The integration stores that object on the
incoming WhatsApp Message and forwards it to the configured client app.

To enrich the referral with ad-set and campaign IDs:

1. Open the receiving **WhatsApp Account**.
2. Enable **Campaign Tracking**.
3. Add a separate permanent Meta system-user token with `ads_read` and access
   to the relevant ad assets.
4. Use **Validate Campaign Tracking** before enabling production traffic.

#### Generate the Meta Ads system-user token

Meta sometimes changes the names and location of Business Suite controls. The
equivalent **Business settings**, **System users**, **Assign assets**, and
**Generate token** controls should be used if the labels below differ.

Before generating the token, confirm all of the following:

- You have full control of the Meta Business Portfolio that owns the app and ad
  accounts.
- The Meta app is added to that Business Portfolio and has the Marketing API
  product/use case. Its App ID must match the **App ID** on the receiving
  WhatsApp Account; validation rejects a token issued to another app.
- Every ad account containing a Click-to-WhatsApp ad for this number is owned
  by, or shared with, the Business Portfolio. The ad account ID is shown as
  `act=<AD_ACCOUNT_ID>` in the Ads Manager URL.

Then create and authorize a least-privilege system user:

1. Open **Meta Business Suite > Settings > Business settings > Users > System
   users** and choose **Add**.
2. Create a dedicated system user, for example `Frappe WhatsApp Attribution`.
   Use the employee/regular system-user role; the person performing these steps
   must still be a Business Portfolio administrator. An admin system-user role
   is not required for read-only attribution.
3. Select the new system user, choose **Assign assets**, and assign:
   - **Apps:** the same Meta app configured on the WhatsApp Account, with
     **Manage app** or **Full control**.
   - **Ad accounts:** every account containing the relevant ads, with
     **View performance** access. This is the least-privilege ad-account role
     for read access; `ads_management` is not used by this integration.
4. With the system user still selected, choose **Generate new token** (or
   **Generate token**) and select that same Meta app.
5. Set the expiration to **Never** and grant only `ads_read`. If **Never** is
   unavailable, do not treat the token as permanent: use the longest permitted
   expiration and establish a rotation reminder before enabling tracking.
6. Generate the token and copy it immediately. Meta only displays it once. Do
   not paste it into source control, chat, screenshots, browser URLs, or logs.
7. In Frappe, open **WhatsApp Account**, paste it into **Ads Access Token**,
   enable **Campaign Tracking**, save, and click **Validate Campaign
   Tracking**. A **Production ready** result confirms that the token is valid,
   belongs to the configured app, represents a system user, includes
   `ads_read`, and has no expiry. Review any warnings when validation succeeds
   without reporting production readiness.

Use Meta's [Access Token Debugger](https://developers.facebook.com/tools/debug/accesstoken/)
when an independent token check is needed. It should report a system-user token,
the expected App ID, `ads_read`, and an expiry of `Never`. To confirm access to
the actual ad asset, run this read-only request in Meta's Graph API Explorer or
an API client, using the Ads token as a Bearer token:

```http
GET /<GRAPH_API_VERSION>/<AD_ID>?fields=id,name,account_id,adset{id,name},campaign{id,name}
Authorization: Bearer <ADS_ACCESS_TOKEN>
```

The response should contain the ad, ad-set, and campaign objects. If token
validation succeeds but this request fails, recheck the system user's ad-account
assignment and confirm that `<AD_ID>` belongs to one of the assigned accounts.
Regenerate the token after adding `ads_read`; permissions are captured when the
token is issued.

For an app reading ad accounts owned by the same Business Portfolio, Meta says
Standard Access and `ads_read` are sufficient. Reading client or partner ad
accounts additionally requires explicit asset sharing and may require Advanced
Access for `ads_read`. See Meta's [Marketing API prerequisites and token
guidance](https://www.postman.com/meta/facebook-marketing-api/documentation/0zr4mes/facebook-marketing-api-mapi)
and [Marketing API onboarding checklist](https://www.postman.com/meta/facebook-marketing-api/documentation/9jo4f5y/mapi-onboarding).

A token marked **Never** can still stop working if the system user, app, token,
or ad-account assignment is revoked, or if Meta applies a security restriction.
Keep the token in the encrypted **Ads Access Token** field and revoke and replace
it immediately if it may have been exposed.

The Ads token is separate from the WhatsApp messaging token and is never
included in webhook payloads or error logs. Referral messages are resolved
before their first client-app delivery. If Meta cannot resolve an ad, the
message is still delivered once with its original `source_id` and `ctwa_clid`.
Client-app deliveries add `message.referral` without changing existing fields:

```json
{
  "source_type": "ad",
  "source_id": "<AD_ID>",
  "ctwa_clid": "<CLICK_ID>",
  "ad": {"id": "<AD_ID>", "name": "<AD_NAME>", "account_id": "<ACCOUNT_ID>"},
  "adset": {"id": "<ADSET_ID>", "name": "<ADSET_NAME>"},
  "campaign": {"id": "<CAMPAIGN_ID>", "name": "<CAMPAIGN_NAME>"},
  "resolution_status": "resolved"
}
```

Organic messages use `"referral": null`.

Retained webhook logs can be previewed and backfilled without replaying client
webhooks:

```bash
bench --site <site> execute \
  frappe_whatsapp.utils.campaign_attribution.backfill_campaign_attribution \
  --kwargs "{'dry_run': True}"

bench --site <site> execute \
  frappe_whatsapp.utils.campaign_attribution.backfill_campaign_attribution \
  --kwargs "{'dry_run': False}"
```

Always review the preview counts before applying the backfill. The command is
idempotent and preserves each WhatsApp Message's `modified` timestamp.

See the [manual webhook reliability deployment guide](docs/webhook-reliability-deployment.md)
for deployment checks, attribution auditing, and rollback steps.

### Contact Blocking and Spam Protection

You can block unwanted WhatsApp contacts from **WhatsApp Profiles** or from an
incoming **WhatsApp Message**.

**How it works:**
- The app creates or updates a **WhatsApp Blocked Contact** record first.
- The app then attempts to block the contact through Meta's Block Users API.
- If Meta rejects the request, the local block remains active.
- Locally blocked contacts are ignored before message creation, media download,
  and third-party forwarding.

Use **WhatsApp Profiles** as the main blocking workspace:
1. Open the contact's **WhatsApp Profiles** record.
2. Review the **Blocking** section for local and Meta sync status.
3. Click **Block Contact** or **Unblock Contact**.
4. If the profile has no WhatsApp Account, select the account in the dialog.

When reviewing spam directly from a conversation, open the incoming
**WhatsApp Message** and click **Block Sender**. This path resolves the sender
number and WhatsApp Account from the message automatically.

**Meta limitation:** Meta only allows blocking WhatsApp users who messaged your
business in the last 24 hours, and business accounts cannot block other
WhatsApp Business accounts. The local block still protects your Frappe site even
when Meta sync fails.

**Third-party API example:**

Authenticated API users need permission to manage **WhatsApp Blocked Contact**
records. Connected apps do not need direct access to Meta tokens.

```http
POST /api/method/frappe_whatsapp.frappe_whatsapp.api.blocking.block_contact
```

```json
{
  "message_name": "WAMSG-0001",
  "reason": "Spam attachment",
  "sync_meta": 1
}
```

For profile-based integrations:

```http
POST /api/method/frappe_whatsapp.frappe_whatsapp.api.blocking.block_profile_contact
```

```json
{
  "profile_name": "WAPROF-0001",
  "whatsapp_account": "Main WhatsApp",
  "reason": "Repeated spam",
  "sync_meta": 1
}
```

`profile_name` is the **WhatsApp Profiles** document name, not necessarily the
phone number shown on the profile.

## Multi-Account Support

Manage multiple WhatsApp Business accounts for different use cases:

- Set default accounts for incoming and outgoing messages
- Select specific accounts when sending bulk messages
- Route notifications through designated accounts
- Auto-read receipts per account

## Recommended Apps

Enhance your WhatsApp experience with these companion apps:

### WhatsApp Chat
A chat app for Frappe Desk to manage WhatsApp conversations.

| | |
|---|---|
| **Features** | Real-time messaging, media support, contact management, read receipts |
| **Install** | `bench get-app https://github.com/shridarpatil/whatsapp_chat` |
| **Marketplace** | <a href="https://frappecloud.com/marketplace/apps/whatsapp_chat"><img src="https://frappe.io/files/try-on-fc-black.png" alt="Try on Frappe Cloud" height="28" /></a> |
| **Use Cases** | Customer conversations, sales follow-ups, support tickets |

### WhatsApp Chatbot
Build automated chatbots with flows, keyword replies, and AI-powered responses.

| | |
|---|---|
| **Features** | Multi-step flows, keyword matching, AI fallback (OpenAI/Anthropic/Google), session management |
| **Install** | `bench get-app https://github.com/shridarpatil/frappe_whatsapp_chatbot` |
| **Marketplace** | <a href="https://frappecloud.com/marketplace/apps/frappe_whatsapp_chatbot"><img src="https://frappe.io/files/try-on-fc-black.png" alt="Try on Frappe Cloud" height="28" /></a> |
| **Use Cases** | Customer support, order status, appointment booking, lead capture |

## Documentation

For detailed documentation, visit [https://shridarpatil.github.io/frappe_whatsapp/](https://shridarpatil.github.io/frappe_whatsapp/)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT
