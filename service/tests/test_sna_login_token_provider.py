import json
import os
import tempfile
from unittest import TestCase

from apollo.egress.agent.service.login_token_provider import (
    ATTR_NAME_AUTH_METHOD,
    ATTR_NAME_KEY_ID,
    ATTR_NAME_TOKEN_FILE_PATH,
    AUTH_METHOD_TOKEN_FILE,
)
from apollo.egress.agent.utils.utils import X_MCD_ID, X_MCD_TOKEN

from agent.sna.sna_login_token_provider import SNALoginTokenProvider


class SNALoginTokenProviderTests(TestCase):
    def setUp(self):
        self._tmpdir = tempfile.mkdtemp()
        self._token_path = os.path.join(self._tmpdir, "secret_string")

    def tearDown(self):
        if os.path.exists(self._token_path):
            os.remove(self._token_path)
        os.rmdir(self._tmpdir)

    def _write_token(self, contents: str) -> None:
        with open(self._token_path, "w") as f:
            f.write(contents)

    def test_returns_token_from_valid_file(self):
        self._write_token(json.dumps({"mcd_id": "id-123", "mcd_token": "secret"}))
        token = SNALoginTokenProvider(self._token_path).get_token()
        self.assertEqual(
            {X_MCD_ID: "id-123", X_MCD_TOKEN: "secret"},
            token,
        )

    def test_missing_file_raises(self):
        # self._token_path was never created.
        with self.assertRaises(ValueError) as ctx:
            SNALoginTokenProvider(self._token_path).get_token()
        self.assertIn(self._token_path, str(ctx.exception))

    def test_invalid_json_raises(self):
        self._write_token("{this is not json")
        with self.assertRaises(ValueError) as ctx:
            SNALoginTokenProvider(self._token_path).get_token()
        # The underlying JSONDecodeError is chained as the cause.
        self.assertIsInstance(ctx.exception.__cause__, json.JSONDecodeError)

    def test_missing_keys_raises(self):
        self._write_token(json.dumps({"mcd_id": "id-123"}))  # no mcd_token
        with self.assertRaises(ValueError) as ctx:
            SNALoginTokenProvider(self._token_path).get_token()
        self.assertIn("mcd_token", str(ctx.exception))

    def test_reports_credential_id_and_never_the_token(self):
        self._write_token(
            json.dumps({"mcd_id": "id-123", "mcd_token": "a-token-value"})
        )
        provider = SNALoginTokenProvider(self._token_path)

        credential_info = provider.get_credential_info()

        self.assertEqual(
            {
                ATTR_NAME_KEY_ID: "id-123",
                ATTR_NAME_AUTH_METHOD: AUTH_METHOD_TOKEN_FILE,
                ATTR_NAME_TOKEN_FILE_PATH: self._token_path,
            },
            credential_info,
        )
        self.assertNotIn("a-token-value", json.dumps(credential_info))

    def test_credential_id_is_none_when_file_is_missing(self):
        # Reporting is the one path that must not raise: it is what diagnoses
        # the missing secret that makes get_token() fail.
        provider = SNALoginTokenProvider(self._token_path)

        self.assertIsNone(provider.get_credential_id())
        self.assertEqual(
            self._token_path,
            provider.get_credential_info()[ATTR_NAME_TOKEN_FILE_PATH],
        )

    def test_credential_id_is_none_when_file_is_invalid(self):
        self._write_token("{this is not json")
        provider = SNALoginTokenProvider(self._token_path)

        self.assertIsNone(provider.get_credential_id())

    def test_non_object_json_raises_value_error(self):
        # sna_service catches ValueError around get_token() to abort startup
        # cleanly; JSON that parses but isn't an object must not escape as a
        # TypeError/AttributeError traceback.
        for payload in ("null", "[]", '"a-string"', "123"):
            with self.subTest(payload=payload):
                self._write_token(payload)
                with self.assertRaises(ValueError) as ctx:
                    SNALoginTokenProvider(self._token_path).get_token()
                self.assertIn(self._token_path, str(ctx.exception))

    def test_credential_id_is_none_for_non_object_json(self):
        self._write_token("null")
        provider = SNALoginTokenProvider(self._token_path)

        self.assertIsNone(provider.get_credential_id())

    def test_credential_id_is_none_when_file_is_unreadable(self):
        self._write_token(json.dumps({"mcd_id": "id-123", "mcd_token": "a-token"}))
        os.chmod(self._token_path, 0)
        provider = SNALoginTokenProvider(self._token_path)
        try:
            self.assertIsNone(provider.get_credential_id())
        finally:
            # Restored here rather than via addCleanup: tearDown removes the
            # file before cleanups run.
            os.chmod(self._token_path, 0o600)
