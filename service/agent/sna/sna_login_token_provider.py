import json
import os
from json import JSONDecodeError
from typing import Dict

from apollo.egress.agent.service.login_token_provider import LoginTokenProvider
from apollo.egress.agent.utils.utils import X_MCD_ID, X_MCD_TOKEN

_SECRET_STRING_PATH = "/usr/local/creds/secret_string"
_MCD_ID_ATTR = "mcd_id"
_MCD_TOKEN_ATTR = "mcd_token"


class SNALoginTokenProvider(LoginTokenProvider):
    """Token provider for the Snowflake Native Application.

    Reads the JSON secret written by the ``APP_PUBLIC.UPDATE_TOKEN`` stored
    procedure and returns the ``mcd_id`` / ``mcd_token`` pair used to
    authenticate against the Monte Carlo backend.

    Does **not** extend :class:`FileLoginTokenProvider`: that class returns
    a ``no-token-id`` / ``no-token-secret`` sentinel when the file is
    missing or malformed, which is dangerous in SNA specifically — because
    ``mcd_id`` carries the tenant routing, a silent sentinel makes the
    agent fall back to the US1 default URL, so an EU install whose secret
    wasn't readable at startup would cheerfully talk to the wrong backend.
    Failing loudly (via :class:`ValueError`) lets SPCS restart the
    container once the secret is provisioned.
    """

    def __init__(self, file_path: str = _SECRET_STRING_PATH):
        self._file_path = file_path

    def get_token(self) -> Dict[str, str]:
        if not os.path.exists(self._file_path):
            raise ValueError(
                f"Monte Carlo token file not found at {self._file_path}. "
                "The token must be set via APP_PUBLIC.UPDATE_TOKEN before "
                "the service can run."
            )
        with open(self._file_path, "r") as f:
            key_str = f.read()
        try:
            key_json = json.loads(key_str)
        except JSONDecodeError as e:
            raise ValueError(
                f"Failed to parse Monte Carlo token JSON at " f"{self._file_path}: {e}"
            ) from e
        if _MCD_ID_ATTR not in key_json or _MCD_TOKEN_ATTR not in key_json:
            raise ValueError(
                f"Monte Carlo token at {self._file_path} is missing required "
                f"keys ({_MCD_ID_ATTR!r}, {_MCD_TOKEN_ATTR!r}); "
                f"found: {sorted(key_json.keys())}"
            )
        return {
            X_MCD_ID: key_json[_MCD_ID_ATTR],
            X_MCD_TOKEN: key_json[_MCD_TOKEN_ATTR],
        }
