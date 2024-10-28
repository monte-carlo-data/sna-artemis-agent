import json
import logging
import os
import socket
import sys
from json import JSONDecodeError
from typing import Dict

from urllib3.connection import HTTPConnection

BACKEND_SERVICE_URL = os.getenv(
    "BACKEND_SERVICE_URL",
    "http://mcd-orchestrator-test-nlb-9b478a23917fbdf9.elb.us-east-1.amazonaws.com"
)
AGENT_ID = os.getenv("AGENT_ID", "snowflake")
LOCAL = os.getenv("ENV", "snowflake") == "local"


def get_logger(logger_name):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(
        logging.Formatter(
            '%(name)s [%(asctime)s] [%(levelname)s] %(message)s'))
    logger.addHandler(handler)
    return logger


logger = get_logger(__name__)


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
    HTTPConnection.default_socket_options = (
            HTTPConnection.default_socket_options
            + [
                (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
            ]
    )
    logger.info("TCP Keep-alive enabled")
