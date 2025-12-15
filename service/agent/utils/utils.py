import logging
import os

BACKEND_SERVICE_URL = os.getenv(
    "BACKEND_SERVICE_URL",
    "https://artemis.getmontecarlo.com:443",
)
_SNOWFLAKE_TOKEN_PATH = "/snowflake/session/token"

logger = logging.getLogger(__name__)


def get_sf_login_token():
    with open(_SNOWFLAKE_TOKEN_PATH, "r") as f:
        return f.read()


def get_application_name():
    # in Snowpark, the application name matches the current database name
    # for local execution, we use MCD_AGENT
    return os.getenv("SNOWFLAKE_DATABASE", "MCD_AGENT")


def get_query_for_logs(query: str) -> str:
    return query[:500].replace("\n", " ")  # limit to 500 chars and remove new lines
