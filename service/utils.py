import logging
import os
import sys

# BACKEND_SERVICE_URL = os.getenv('BACKEND_SERVICE_URL', 'https://f011-34-195-153-240.ngrok-free.app')
BACKEND_SERVICE_URL = os.getenv(
    "BACKEND_SERVICE_URL",
    "http://mcd-orchestrator-test-nlb-85de00564645f8e2.elb.us-east-1.amazonaws.com"
)
AGENT_ID = os.getenv("AGENT_ID", "snowflake")
LOCAL = os.getenv("ENV", "snowflake") == "local"


def get_mc_login_token():
    if LOCAL:
        return "local-token"
    if os.path.exists("/usr/local/creds/secret_string"):
        with open("/usr/local/creds/secret_string", "r") as f:
            return f.read()
    else:
        return "no-token"


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
