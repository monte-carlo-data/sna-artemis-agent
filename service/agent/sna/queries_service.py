import logging
from contextlib import closing
from typing import Dict, Any, Optional, Tuple, List

from snowflake.connector import (
    DatabaseError,
    ProgrammingError,
    SnowflakeConnection,
)
from snowflake.connector.cursor import SnowflakeCursor
from snowflake.sqlalchemy.snowdialect import SnowflakeDialect
from sqlalchemy import QueuePool

from agent.sna.config.config_manager import ConfigurationManager
from agent.sna.config.config_keys import (
    CONFIG_CONNECTION_POOL_SIZE,
    CONFIG_USE_CONNECTION_POOL,
    CONFIG_USE_SYNC_QUERIES,
)
from agent.sna.sf_connection import create_connection
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
from agent.utils.utils import LOCAL

logger = logging.getLogger(__name__)

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

# We have the following threads opening Snowflake connections:
# - a single thread running queries
# - a single thread pushing results
# - a single thread executing other operations, like storage, that uses a connection too
# So, we maintain 3 open connections, we also set max_overflow to -1 to allow for "extra"
# connections to be created if needed (they will be immediately closed after being used).
_DEFAULT_CONNECTION_POOL_SIZE = 3


class QueriesService:
    """
    Takes care of executing queries in Snowflake, the queries are wrapped in a procedure that
    uses SF functions to notify the agent when the query is completed or failed.
    The query is executed using the MCD_AGENT_EXECUTE_QUERY procedure, which is configured to execute
    as owner and allow us to take advantage of FUTURE grants (which is not
    available to applications).
    """

    def __init__(self, config_manager: ConfigurationManager):
        self._config_manager = config_manager
        self._direct_sync_queries = LOCAL
        self._helper_sync_queries = config_manager.get_bool_value(
            CONFIG_USE_SYNC_QUERIES, False
        )

        self._connection_pool = (
            self._create_connection_pool(
                pool_size=self._config_manager.get_int_value(
                    CONFIG_CONNECTION_POOL_SIZE, _DEFAULT_CONNECTION_POOL_SIZE
                ),
            )
            if self._config_manager.get_bool_value(CONFIG_USE_CONNECTION_POOL, True)
            else None
        )

    def result_for_query(self, query_id: str) -> Dict[str, Any]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                conn.get_query_status_throw_if_error(query_id)
                cur.get_results_from_sfqid(query_id)
                return self._result_for_cursor(cur)

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

    def run_query_and_fetch_all(
        self,
        query: str,
        *args,  # type: ignore
    ) -> Tuple[List[Tuple], List[Tuple]]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, *args)
                return cur.fetchall(), cur.description  # type: ignore

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

    def _connect(self) -> SnowflakeConnection:
        if self._connection_pool:
            # connections returned by SQLAlchemy's pool doesn't support context manager protocol
            # so we wrap them with "closing" to support it
            return closing(_connection_pool.connect())  # type: ignore
        else:
            return create_connection()

    def run_query(self, query: SnowflakeQuery) -> Optional[Dict[str, Any]]:
        timeout = query.timeout or 850
        operation_id = query.operation_id
        sql_query = query.query
        with self._connect() as conn:
            with conn.cursor() as cur:
                if self._direct_sync_queries:
                    cur.execute(sql_query)
                    logger.info(
                        f"Sync query executed: {operation_id} {sql_query}, id: {cur.sfqid}"
                    )
                    return self._result_for_cursor(cur)
                elif self._helper_sync_queries:
                    cur.execute(QUERY_SET_STATEMENT_TIMEOUT.format(timeout=timeout))
                    cur.execute(QUERY_EXECUTE_QUERY_WITH_HELPER_SYNC, [sql_query])
                    logger.info(f"Sync query executed: {operation_id} {sql_query}")
                    return self._result_for_cursor(cur)
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

    @staticmethod
    def _create_connection_pool(pool_size: int) -> Optional[QueuePool]:
        return QueuePool(
            create_connection,  # type: ignore
            dialect=SnowflakeDialect(),
            pool_size=pool_size,
            max_overflow=-1,
            recycle=30 * 60,  # don't use connections older than 30 minutes
            reset_on_return="rollback",
            echo=True,
            logging_name="pool",
            pre_ping=True,
            # test the connection before using it, it uses "SELECT 1" for Snowflake
        )
