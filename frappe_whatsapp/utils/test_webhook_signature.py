"""Tests for Meta webhook signature validation (X-Hub-Signature-256).

Covers three layers:
  1. _verify_webhook_signature() — HMAC-SHA256 unit tests.
  2. _get_active_app_secrets() — DB-layer unit tests (patches the module-level
     _get_decrypted_password alias so no request context is needed).
  3. _handle_post_body() gating — integration tests confirming that
     valid/invalid/missing signatures produce 200-with-enqueue or
     403-without-enqueue.  Uses _handle_post_body() directly to avoid the
     frappe.request LocalProxy which is only bound inside a real HTTP request.
"""

import hashlib
import hmac
from unittest.mock import patch

import frappe
from frappe.tests.utils import FrappeTestCase

from frappe_whatsapp.utils.webhook import (
    _get_active_app_secrets,
    _handle_post_body,
    _verify_webhook_signature,
)

# ---------------------------------------------------------------------------
# Shared test constants
# ---------------------------------------------------------------------------

_TEST_SECRET = "super-secret-app-key"
_TEST_BODY = b'{"object":"whatsapp_business_account","entry":[]}'


def _make_sig(body: bytes, secret: str) -> str:
    """Return the X-Hub-Signature-256 header value for the given body/secret."""
    hex_sig = hmac.new(
        secret.encode("utf-8"), body, hashlib.sha256
    ).hexdigest()
    return f"sha256={hex_sig}"


# ===========================================================================
# 1. _verify_webhook_signature() — HMAC unit tests
# ===========================================================================

class TestVerifyWebhookSignature(FrappeTestCase):

    def _call(self, body: bytes, sig: str, secrets=None) -> bool:
        if secrets is None:
            secrets = [_TEST_SECRET]
        with patch(
            "frappe_whatsapp.utils.webhook._get_active_app_secrets",
            return_value=secrets,
        ):
            return _verify_webhook_signature(body, sig)

    def test_valid_signature_accepted(self):
        sig = _make_sig(_TEST_BODY, _TEST_SECRET)
        self.assertTrue(self._call(_TEST_BODY, sig))

    def test_tampered_body_rejected(self):
        """Signature computed over original body must fail for a different body."""
        sig = _make_sig(_TEST_BODY, _TEST_SECRET)
        self.assertFalse(self._call(b'{"tampered": true}', sig))

    def test_wrong_secret_rejected(self):
        sig = _make_sig(_TEST_BODY, "wrong-secret")
        self.assertFalse(self._call(_TEST_BODY, sig))

    def test_missing_signature_header_rejected(self):
        self.assertFalse(self._call(_TEST_BODY, ""))

    def test_signature_without_sha256_prefix_rejected(self):
        bare_hex = hmac.new(
            _TEST_SECRET.encode(), _TEST_BODY, hashlib.sha256
        ).hexdigest()
        self.assertFalse(self._call(_TEST_BODY, f"md5={bare_hex}"))

    def test_sha256_prefix_with_empty_hex_rejected(self):
        self.assertFalse(self._call(_TEST_BODY, "sha256="))

    def test_no_app_secrets_configured_rejected(self):
        """When no accounts have app_secret set, all requests must be blocked
        and an error must be logged so operators know what to configure."""
        with patch("frappe_whatsapp.utils.webhook.frappe.log_error") as mock_log:
            result = self._call(_TEST_BODY, _make_sig(_TEST_BODY, _TEST_SECRET),
                                secrets=[])
        self.assertFalse(result)
        mock_log.assert_called_once()
        title = mock_log.call_args.args[1]
        self.assertIn("app secret", title.lower())

    def test_validates_against_all_configured_secrets(self):
        """Any one of multiple configured secrets must satisfy the check
        (supports deployments with several Meta apps)."""
        secret_a = "secret-for-app-a"
        secret_b = "secret-for-app-b"
        sig = _make_sig(_TEST_BODY, secret_b)
        self.assertTrue(self._call(_TEST_BODY, sig, secrets=[secret_a, secret_b]))

    def test_correct_length_but_wrong_hex_rejected(self):
        """Constant-time comparison must reject a same-length but wrong digest
        (guards against timing-attack shortcuts in naive == comparisons)."""
        fake_sig = "sha256=" + "0" * 64
        self.assertFalse(self._call(_TEST_BODY, fake_sig))


# ===========================================================================
# 2. _get_active_app_secrets() — DB-layer unit tests
# ===========================================================================

class TestGetActiveAppSecrets(FrappeTestCase):
    """Patch _get_decrypted_password at the module level where it is bound
    so tests work without a request context or real encrypted field."""

    _PATCH_PWD = "frappe_whatsapp.utils.webhook._get_decrypted_password"

    @patch(_PATCH_PWD, return_value="my-secret")
    @patch("frappe_whatsapp.utils.webhook.frappe.get_all",
           return_value=[frappe._dict({"name": "Test Account"})])
    def test_returns_secret_from_active_account(self, _mock_get_all, _mock_pwd):
        secrets = _get_active_app_secrets()
        self.assertEqual(secrets, ["my-secret"])

    @patch(_PATCH_PWD, return_value=None)
    @patch("frappe_whatsapp.utils.webhook.frappe.get_all",
           return_value=[frappe._dict({"name": "Test Account"})])
    def test_account_with_no_secret_skipped(self, _mock_get_all, _mock_pwd):
        secrets = _get_active_app_secrets()
        self.assertEqual(secrets, [])

    @patch("frappe_whatsapp.utils.webhook.frappe.get_all", return_value=[])
    def test_no_active_accounts_returns_empty(self, _mock_get_all):
        secrets = _get_active_app_secrets()
        self.assertEqual(secrets, [])

    @patch(_PATCH_PWD, return_value="shared-secret")
    @patch("frappe_whatsapp.utils.webhook.frappe.get_all",
           return_value=[frappe._dict({"name": "A"}), frappe._dict({"name": "B"})])
    def test_duplicate_secrets_deduplicated(self, _mock_get_all, _mock_pwd):
        """Two accounts sharing the same Meta App (same app_secret) must
        produce only one entry — HMAC should be computed once."""
        secrets = _get_active_app_secrets()
        self.assertEqual(len(secrets), 1)
        self.assertEqual(secrets[0], "shared-secret")

    @patch(_PATCH_PWD, side_effect=Exception("field missing"))
    @patch("frappe_whatsapp.utils.webhook.frappe.get_all",
           return_value=[frappe._dict({"name": "Broken Account"})])
    def test_exception_from_decryption_skipped_silently(
        self, _mock_get_all, _mock_pwd
    ):
        """A decryption error (e.g. field not yet migrated) must not crash
        the function — the account is silently skipped."""
        secrets = _get_active_app_secrets()
        self.assertEqual(secrets, [])


# ===========================================================================
# 3. _handle_post_body() — signature gating integration tests
# ===========================================================================

class TestHandlePostBodyGating(FrappeTestCase):
    """Test _handle_post_body() directly to avoid frappe.request LocalProxy.

    post() is a thin wrapper that reads raw_body and sig_header from the
    live request then delegates here; the interesting logic lives here.
    """

    def _call(
        self,
        *,
        sig_valid: bool,
        raw_body: bytes = _TEST_BODY,
    ):
        """Call _handle_post_body() with a controlled validation outcome."""
        with (
            patch(
                "frappe_whatsapp.utils.webhook._verify_webhook_signature",
                return_value=sig_valid,
            ),
            patch("frappe_whatsapp.utils.webhook.frappe.enqueue") as mock_enqueue,
            patch("frappe_whatsapp.utils.webhook.frappe.get_doc"),
        ):
            response = _handle_post_body(raw_body, "sha256=controlled-by-mock")

        return response, mock_enqueue

    # --- rejection paths ---

    def test_invalid_signature_returns_403_without_enqueue(self):
        response, mock_enqueue = self._call(sig_valid=False)
        self.assertEqual(response.status_code, 403)
        mock_enqueue.assert_not_called()

    def test_missing_signature_returns_403_without_enqueue(self):
        """An empty sig_header causes _verify_webhook_signature to return
        False; the gating behaviour is the same as an invalid signature."""
        with (
            patch(
                "frappe_whatsapp.utils.webhook._get_active_app_secrets",
                return_value=[_TEST_SECRET],
            ),
            patch("frappe_whatsapp.utils.webhook.frappe.enqueue") as mock_enqueue,
            patch("frappe_whatsapp.utils.webhook.frappe.get_doc"),
        ):
            response = _handle_post_body(_TEST_BODY, "")   # no header

        self.assertEqual(response.status_code, 403)
        mock_enqueue.assert_not_called()

    # --- acceptance paths ---

    def test_valid_signature_returns_200_and_enqueues(self):
        response, mock_enqueue = self._call(sig_valid=True)
        self.assertEqual(response.status_code, 200)
        mock_enqueue.assert_called_once()
        target = mock_enqueue.call_args.args[0]
        self.assertEqual(
            target,
            "frappe_whatsapp.utils.webhook.process_webhook_payload",
        )

    def test_valid_signature_passes_parsed_json_to_enqueue(self):
        """The worker must receive the parsed payload dict."""
        payload = b'{"object":"whatsapp_business_account","entry":[]}'
        _, mock_enqueue = self._call(sig_valid=True, raw_body=payload)
        call_kwargs = mock_enqueue.call_args.kwargs
        self.assertIsInstance(call_kwargs["data"], dict)
        self.assertEqual(call_kwargs["data"]["object"],
                         "whatsapp_business_account")

    # --- end-to-end with real HMAC (no mocking of _verify_webhook_signature) ---

    def test_real_hmac_valid_signature_accepted(self):
        """Full HMAC path: compute a real signature, verify it passes."""
        sig = _make_sig(_TEST_BODY, _TEST_SECRET)
        with (
            patch(
                "frappe_whatsapp.utils.webhook._get_active_app_secrets",
                return_value=[_TEST_SECRET],
            ),
            patch("frappe_whatsapp.utils.webhook.frappe.enqueue") as mock_enqueue,
            patch("frappe_whatsapp.utils.webhook.frappe.get_doc"),
        ):
            response = _handle_post_body(_TEST_BODY, sig)

        self.assertEqual(response.status_code, 200)
        mock_enqueue.assert_called_once()

    def test_real_hmac_invalid_signature_rejected(self):
        """Full HMAC path: a wrong signature is rejected."""
        fake_sig = "sha256=" + "b" * 64
        with (
            patch(
                "frappe_whatsapp.utils.webhook._get_active_app_secrets",
                return_value=[_TEST_SECRET],
            ),
            patch("frappe_whatsapp.utils.webhook.frappe.enqueue") as mock_enqueue,
            patch("frappe_whatsapp.utils.webhook.frappe.get_doc"),
        ):
            response = _handle_post_body(_TEST_BODY, fake_sig)

        self.assertEqual(response.status_code, 403)
        mock_enqueue.assert_not_called()
