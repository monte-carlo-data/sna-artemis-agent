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
AGENT_ID = os.getenv("AGENT_ID", "snowflake")
LOCAL = os.getenv("ENV", "snowflake") == "local"
DEBUG = os.getenv("DEBUG", "false").lower() == "true"

_HEALTH_ENV_VARS = [
    "PYTHON_VERSION",
    "SERVER_SOFTWARE",
]

logger = logging.getLogger(__name__)


def init_logging():
    logging.basicConfig(
        stream=sys.stdout, level=logging.DEBUG if DEBUG else logging.INFO
    )


def get_mc_login_token() -> Dict[str, str]:
    if LOCAL:
        return {
            "x-mcd-id": "local-token-id",
            "x-mcd-token": "local-token-secret",
        }
    if os.path.exists("/usr/local/creds/secret_string"):
        with open("/usr/local/creds/secret_string", "r") as f:
            key_str = f.read()
        try:
            key_json = json.loads(key_str)
            if "mcd_id" in key_json and "mcd_token" in key_json:
                return {
                    "x-mcd-id": key_json["mcd_id"],
                    "x-mcd-token": key_json["mcd_token"],
                }
        except JSONDecodeError as ex:
            logger.error(f"Failed to parse Key JSON: {ex}")
    else:
        logger.warning("No token file found")

    return {
        "x-mcd-id": "no-token-id",
        "x-mcd-token": "no-token-secret",
    }


def get_sf_login_token():
    with open("/snowflake/session/token", "r") as f:
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
