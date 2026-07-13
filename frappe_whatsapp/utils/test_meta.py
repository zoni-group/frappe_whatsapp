import json
from unittest.mock import patch

import frappe
from frappe.tests.utils import FrappeTestCase
from requests import Response

from frappe_whatsapp.utils.meta import get_paginated_data, request_meta_json


def _response(status: int, payload: dict) -> Response:
    response = Response()
    response.status_code = status
    response._content = json.dumps(payload).encode()
    response.headers["content-type"] = "application/json"
    return response


class TestMetaRequests(FrappeTestCase):
    @patch("frappe_whatsapp.utils.meta.requests.request")
    def test_oauth_error_includes_account_code_and_subcode(self, mock_request):
        mock_request.return_value = _response(
            401,
            {
                "error": {
                    "message": "Error validating access token: Session has expired.",
                    "type": "OAuthException",
                    "code": 190,
                    "error_subcode": 463,
                }
            },
        )

        with self.assertRaises(frappe.ValidationError) as raised:
            request_meta_json(
                "GET",
                "https://graph.facebook.com/v24.0/me",
                account_name="expired-account",
                operation="access-token validation",
                headers={"Authorization": "Bearer secret"},
            )

        message = str(raised.exception)
        self.assertIn("expired-account", message)
        self.assertIn("code 190", message)
        self.assertIn("subcode 463", message)
        self.assertNotIn("secret", message)

    @patch("frappe_whatsapp.utils.meta.requests.request")
    def test_post_error_preserves_safe_meta_details(self, mock_request):
        mock_request.return_value = _response(
            400,
            {
                "error": {
                    "message": "Invalid parameter",
                    "code": 100,
                    "error_subcode": 2494073,
                    "error_user_msg": "The template payload is invalid.",
                    "error_data": {
                        "details": "components must not be empty",
                    },
                }
            },
        )

        with self.assertRaises(frappe.ValidationError) as raised:
            request_meta_json(
                "POST",
                "https://graph.facebook.com/v24.0/phone/messages",
                account_name="calling-account",
                operation="message send",
                headers={"Authorization": "Bearer top-secret-token"},
                json_body={"messaging_product": "whatsapp"},
            )

        message = str(raised.exception)
        self.assertIn("calling-account", message)
        self.assertIn("Invalid parameter", message)
        self.assertIn("The template payload is invalid.", message)
        self.assertIn("components must not be empty", message)
        self.assertIn("code 100", message)
        self.assertIn("subcode 2494073", message)
        self.assertNotIn("top-secret-token", message)
        self.assertEqual(
            mock_request.call_args.kwargs["json"],
            {"messaging_product": "whatsapp"},
        )

    @patch("frappe_whatsapp.utils.meta.requests.request")
    def test_paginated_collection_follows_all_pages(self, mock_request):
        mock_request.side_effect = [
            _response(
                200,
                {
                    "data": [{"id": "one"}],
                    "paging": {
                        "next": "https://graph.facebook.com/v24.0/waba/items?after=one"
                    },
                },
            ),
            _response(200, {"data": [{"id": "two"}]}),
        ]

        data = get_paginated_data(
            "https://graph.facebook.com/v24.0/waba/items",
            account_name="account",
            operation="item sync",
            headers={"Authorization": "Bearer secret"},
            params={"limit": 1},
        )

        self.assertEqual([item["id"] for item in data], ["one", "two"])
        self.assertEqual(mock_request.call_count, 2)
        self.assertIsNone(mock_request.call_args_list[1].kwargs["params"])
