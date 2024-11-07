import logging
import os
from sqlite3 import ProgrammingError
from typing import Dict, Any, Optional

from snowflake.connector import connect as snowflake_connect, DatabaseError
from snowflake.connector.cursor import SnowflakeCursor

from agent.sna.sf_queries import (
    QUERY_EXECUTE_QUERY_WITH_HELPER,
    QUERY_SET_STATEMENT_TIMEOUT,
    QUERY_EXECUTE_QUERY_WITH_HELPER_SYNC,
)
from agent.sna.sf_query import SnowflakeQuery
from agent.utils.serde import (
    ATTRIBUTE_NAME_RESULT,
    ATTRIBUTE_NAME_ERROR,
    ATTRIBUTE_NAME_ERROR_ATTRS,
    ATTRIBUTE_NAME_ERROR_TYPE,
)
from agent.utils.utils import get_sf_login_token, LOCAL

logger = logging.getLogger(__name__)

WAREHOUSE_NAME = "MCD_AGENT_WH"

_SYNC_QUERIES = LOCAL
_SNOWFLAKE_SYNC_QUERIES = False

ERROR_INSUFFICIENT_PRIVILEGES = 3001
ERROR_SHARED_DATABASE_NO_LONGER_AVAILABLE = 3030
ERROR_OBJECT_DOES_NOT_EXIST = 2043
ERROR_QUERY_CANCELLED = 604
ERROR_STATEMENT_TIMED_OUT = 630
_PROGRAMMING_ERRORS = [
    ERROR_INSUFFICIENT_PRIVILEGES,
    ERROR_SHARED_DATABASE_NO_LONGER_AVAILABLE,
    ERROR_OBJECT_DOES_NOT_EXIST,
    ERROR_QUERY_CANCELLED,
    ERROR_STATEMENT_TIMED_OUT,
]


class SnowflakeClient:
    """
    Takes care of executing queries in Snowflake, the queries are wrapped in a procedure that
    uses SF functions to notify the agent when the query is completed or failed.
    The query is executed using the MCD_AGENT_EXECUTE_QUERY procedure, which is configured to execute
    as owner and allow us to take advantage of FUTURE grants (which is not
    available to applications).
    """

    def __init__(self):
        pass

    @classmethod
    def result_for_query(cls, query_id: str) -> Dict[str, Any]:
        with (conn := cls._connect()):
            with conn.cursor() as cur:
                conn.get_query_status_throw_if_error(query_id)
                cur.get_results_from_sfqid(query_id)
                return cls._result_for_cursor(cur)

    @classmethod
    def result_for_query_failed(
        cls, operation_id: str, code: int, msg: str, state: str
    ):
        msg = cls._get_error_message(msg)
        logger.info(
            f"QUERY FAILED: op_id={operation_id}, code={code}, msg={msg}, state={state}"
        )
        error_type = (
            "ProgrammingError" if code in _PROGRAMMING_ERRORS else "DatabaseError"
        )
        return {
            ATTRIBUTE_NAME_ERROR: msg,
            ATTRIBUTE_NAME_ERROR_ATTRS: {"errno": code, "sqlstate": state},
            ATTRIBUTE_NAME_ERROR_TYPE: error_type,
        }

    @staticmethod
    def _result_for_cursor(cursor: SnowflakeCursor) -> Dict[str, Any]:
        return {
            ATTRIBUTE_NAME_RESULT: {
                "all_results": cursor.fetchall(),
                "description": cursor.description,
                "rowcount": cursor.rowcount,
            },
            # ATTRIBUTE_NAME_TRACE_ID: trace_id,
        }

    @classmethod
    def _connect(cls):
        if os.getenv("SNOWFLAKE_HOST"):  # running in a Snowpark container
            return snowflake_connect(
                host=os.getenv("SNOWFLAKE_HOST"),
                account=os.getenv("SNOWFLAKE_ACCOUNT"),
                warehouse=WAREHOUSE_NAME,
                token=get_sf_login_token(),
                authenticator="oauth",
                paramstyle="qmark",
            )
        else:  # running locally
            return snowflake_connect(
                account=os.getenv("SNOWFLAKE_ACCOUNT"),
                warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                paramstyle="qmark",
                user=os.getenv("SNOWFLAKE_USER"),
                private_key_file=os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE"),
                role=os.getenv("SNOWFLAKE_ROLE"),
            )

    @classmethod
    def run_query(cls, query: SnowflakeQuery) -> Optional[Dict[str, Any]]:
        timeout = query.timeout or 850
        operation_id = query.operation_id
        sql_query = query.query
        with (conn := cls._connect()):
            with conn.cursor() as cur:
                if _SYNC_QUERIES:
                    cur.execute(sql_query)
                    logger.info(
                        f"Sync query executed: {operation_id} {sql_query}, id: {cur.sfqid}"
                    )
                    return cls._result_for_cursor(cur)
                elif _SNOWFLAKE_SYNC_QUERIES:
                    cur.execute(QUERY_SET_STATEMENT_TIMEOUT.format(timeout=timeout))
                    cur.execute(QUERY_EXECUTE_QUERY_WITH_HELPER_SYNC, [sql_query])
                    logger.info(f"Sync query executed: {operation_id} {sql_query}")
                    return cls._result_for_cursor(cur)
                else:
                    execute_query = QUERY_EXECUTE_QUERY_WITH_HELPER.format(
                        timeout=timeout
                    )
                    cur.execute_async(execute_query, [operation_id, sql_query])
                    logger.info(
                        f"Async query executed: {operation_id} {sql_query}, id: {cur.sfqid}"
                    )
                    return None

    @staticmethod
    def _get_error_message(msg: str) -> str:
        # remove the prefix:
        # "Uncaught exception of type 'STATEMENT_ERROR' on line 2 at position 25 : "
        if ":" in msg:
            return msg[(msg.index(":") + 1) :].strip()
        return msg

    @staticmethod
    def result_for_exception(ex: Exception) -> Dict:
        result: Dict[str, Any] = {
            ATTRIBUTE_NAME_ERROR: str(ex),
        }
        if isinstance(ex, DatabaseError):
            result[ATTRIBUTE_NAME_ERROR_ATTRS] = {
                "errno": ex.errno,
                "sqlstate": ex.sqlstate,
            }
            if isinstance(ex, ProgrammingError):
                result[ATTRIBUTE_NAME_ERROR_TYPE] = "ProgrammingError"
            elif isinstance(ex, DatabaseError):
                result[ATTRIBUTE_NAME_ERROR_TYPE] = "DatabaseError"

        return result
