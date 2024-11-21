import json
import logging
import os
import socket
import sys
from json import JSONDecodeError
from typing import Dict, Optional, Any

from urllib3.connection import HTTPConnection

from agent.utils.settings import VERSION, BUILD_NUMBER

BACKEND_SERVICE_URL = os.getenv(
    "BACKEND_SERVICE_URL",
    "http://mcd-orchestrator-test-nlb-9b478a23917fbdf9.elb.us-east-1.amazonaws.com",
)
LOCAL = os.getenv("ENV", "snowflake") == "local"
DEBUG = os.getenv("DEBUG", "false").lower() == "true"

USE_CONNECTION_POOL = os.getenv("USE_CONNECTION_POOL", "true").lower() == "true"
# We have the following threads opening Snowflake connections:
# - a single thread running queries
# - a single thread pushing results
# - a single thread executing other operations, like storage, that uses a connection too
# So, we maintain 3 open connections, we also set max_overflow to -1 to allow for "extra"
# connections to be created if needed (they will be immediately closed after being used).
CONNECTION_POOL_SIZE = int(os.getenv("CONNECTION_POOL_SIZE", "3"))

_X_MCD_ID = "x-mcd-id"
_X_MCD_TOKEN = "x-mcd-token"
_MCD_ID_ATTR = "mcd_id"
_MCD_TOKEN_ATTR = "mcd_token"
_LOCAL_TOKEN_ID = os.getenv("LOCAL_TOKEN_ID", "local-token-id")
_LOCAL_TOKEN_SECRET = os.getenv("LOCAL_TOKEN_SECRET", "local-token-secret")
_NO_TOKEN_ID = "no-token-id"
_NO_TOKEN_SECRET = "no-token-secret"

_SECRET_STRING_PATH = "/usr/local/creds/secret_string"
_SNOWFLAKE_TOKEN_PATH = "/snowflake/session/token"

_HEALTH_ENV_VARS = [
    "PYTHON_VERSION",
    "SERVER_SOFTWARE",
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_HOST",
    "SNOWFLAKE_SERVICE_NAME",
]

logger = logging.getLogger(__name__)


def init_logging():
    logging.basicConfig(
        stream=sys.stdout,
        level=logging.DEBUG if DEBUG else logging.INFO,
        format="[%(asctime)s] %(levelname)s:%(name)s: %(message)s",
    )


def get_mc_login_token() -> Dict[str, str]:
    if LOCAL:
        return {
            _X_MCD_ID: _LOCAL_TOKEN_ID,
            _X_MCD_TOKEN: _LOCAL_TOKEN_SECRET,
        }
    if os.path.exists(_SECRET_STRING_PATH):
        with open(_SECRET_STRING_PATH, "r") as f:
            key_str = f.read()
        try:
            key_json = json.loads(key_str)
            if _MCD_ID_ATTR in key_json and _MCD_TOKEN_ATTR in key_json:
                return {
                    _X_MCD_ID: key_json[_MCD_ID_ATTR],
                    _X_MCD_TOKEN: key_json[_MCD_TOKEN_ATTR],
                }
            else:
                logger.warning(f"Invalid secret string, keys: {key_json.keys()}")
        except JSONDecodeError as ex:
            logger.error(f"Failed to parse Key JSON: {ex}")
    else:
        logger.warning("No token file found")

    return {
        _X_MCD_ID: _NO_TOKEN_ID,
        _X_MCD_TOKEN: _NO_TOKEN_SECRET,
    }


def get_sf_login_token():
    with open(_SNOWFLAKE_TOKEN_PATH, "r") as f:
        return f.read()


def enable_tcp_keep_alive():
    HTTPConnection.default_socket_options = HTTPConnection.default_socket_options + [  # type: ignore
        (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
    ]
    logger.info("TCP Keep-alive enabled")


def health_information(trace_id: Optional[str] = None) -> Dict[str, Any]:
    health_info = {
        "platform": "SNA",
        "version": VERSION,
        "build": BUILD_NUMBER,
        "env": _env_dictionary(),
    }
    if trace_id:
        health_info["trace_id"] = trace_id
    return health_info


def _env_dictionary() -> Dict:
    env: Dict[str, Optional[str]] = {
        "PYTHON_SYS_VERSION": sys.version,
        "CPU_COUNT": str(os.cpu_count()),
    }
    env.update(
        {
            env_var: os.getenv(env_var)
            for env_var in _HEALTH_ENV_VARS
            if os.getenv(env_var)
        }
    )
    return env
