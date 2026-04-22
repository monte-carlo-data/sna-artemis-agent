import json
import os
import tempfile
from unittest import TestCase

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
