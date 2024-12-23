import os
from snowflake.connector import connect as snowflake_connect

from agent.utils.utils import get_sf_login_token


def create_connection(warehouse_name: str):
    if os.getenv("SNOWFLAKE_HOST"):  # running in a Snowpark container
        return snowflake_connect(
            host=os.getenv("SNOWFLAKE_HOST"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            warehouse=warehouse_name,
            token=get_sf_login_token(),
            authenticator="oauth",
            paramstyle="qmark",
        )
    else:  # running locally
        return snowflake_connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=warehouse_name,
            paramstyle="qmark",
            user=os.getenv("SNOWFLAKE_USER"),
            private_key_file=os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE"),
            role=os.getenv("SNOWFLAKE_ROLE"),
        )
