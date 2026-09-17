import logging
import os

BACKEND_SERVICE_URL = os.getenv(
    "BACKEND_SERVICE_URL",
    "https://artemis.getmontecarlo.com:443",
)
_SNOWFLAKE_TOKEN_PATH = "/snowflake/session/token"

logger = logging.getLogger(__name__)

_ARROW_IMPORT_WARNING = "Failed to import ArrowResult"


def silence_arrow_import_warning():
    """Drop the connector's import-time warning about the Arrow result reader.

    `snowflake.connector.cursor` logs a WARNING while being imported when it
    cannot load its compiled Arrow iterator, which is always the case here: the
    alpine runtime image ships no libstdc++. We never read Arrow result sets, so
    the record is noise on every start. Call this before the first import that
    pulls the connector in.
    """
    logging.getLogger("snowflake.connector.cursor").addFilter(
        lambda record: _ARROW_IMPORT_WARNING not in record.getMessage()
    )


def get_sf_login_token():
    with open(_SNOWFLAKE_TOKEN_PATH, "r") as f:
        return f.read()


def get_application_name():
    # in Snowpark, the application name matches the current database name
    # for local execution, we use MCD_AGENT
    return os.getenv("SNOWFLAKE_DATABASE", "MCD_AGENT")


def get_query_for_logs(query: str) -> str:
    return query[:500].replace("\n", " ")  # limit to 500 chars and remove new lines
