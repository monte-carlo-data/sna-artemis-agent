import json
import signal
import socket
import sys
import uuid
from threading import Thread, Condition
from typing import List, Tuple, Dict, Optional
from urllib.parse import urljoin
from events_client import EventsClient
from serde import ATTRIBUTE_NAME_ERROR, ATTRIBUTE_NAME_ERROR_ATTRS, ATTRIBUTE_NAME_ERROR_TYPE, \
    ATTRIBUTE_NAME_RESULT, AgentSerializer, ATTRIBUTE_NAME_TRACE_ID
from utils import BACKEND_SERVICE_URL, AGENT_ID, LOCAL, get_mc_login_token, get_logger

import requests
from flask import Flask
from flask import request
from flask import make_response
import os
import snowflake.connector
from snowflake.connector import DatabaseError, ProgrammingError
from urllib3.connection import HTTPConnection

SERVICE_HOST = os.getenv('SERVER_HOST', '0.0.0.0')
SERVICE_PORT = os.getenv('SERVER_PORT', 8080)

WAREHOUSE_NAME = "APP_DEV_WH" if LOCAL else "MC_APP_WH"
_SYNC_QUERIES = LOCAL
_SNOWFLAKE_SYNC_QUERIES = False

_queries_condition = Condition()
_pending_queries: List[Tuple[str, str, Optional[int]]] = []
_queries_executor_running = True

_results_condition = Condition()
_pending_results: List[Tuple[str, str]] = []
_publisher_running = True

_events_clients: List[EventsClient] = []

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

logger = get_logger(__name__)

app = Flask(__name__)


def handler(signum, frame):
    print("Signal handler called with signal", signum)
    with _queries_condition:
        global _queries_executor_running
        _queries_executor_running = False
        _queries_condition.notify()
    with _results_condition:
        global _publisher_running
        _publisher_running = False
        _results_condition.notify()
    if _events_clients:
        _events_clients[0].stop()
    print("Signal handler completed")
    sys.exit(0)


signal.signal(signal.SIGINT, handler)


@app.get("/healthcheck")
def readiness_probe():
    return "I'm ready!"


@app.post("/schedule_query")
def schedule_query():
    """
    Main handler for input data sent by Snowflake.
    """
    message = request.json
    logger.debug(f'Received request: {message}')

    if message is None or not message['data']:
        logger.info('Received empty message')
        return {}

    input_rows = message['data']
    if input_rows:
        query = input_rows[0][1]
        operation_id = str(uuid.uuid4())
        logger.info(f"Scheduling operation: {operation_id}, query: {query}")
        _schedule_query(operation_id, query)
        results = ["Query scheduled"]
        output_rows = [[0, str(results)]]
    else:
        output_rows = []

    response = make_response({"data": output_rows})
    response.headers['Content-type'] = 'application/json'
    logger.debug(f'Sending response: {response.json}')
    return response


@app.post("/push_metrics")
def fetch_metrics():
    # response = requests.get("http://127.0.0.1:9001/metrics")
    response = requests.get("http://discover.monitor.mc_app_compute_pool.snowflakecomputing.internal:9001/metrics")
    lines = response.text.splitlines()
    _push_results_to_backend("metrics", {
        "metrics": lines,
    })

    output_rows = [[0, "metrics pushed"]]
    response = make_response({"data": output_rows})
    response.headers['Content-type'] = 'application/json'
    return response


@app.post("/query_completed")
def query_completed():
    message = request.json
    logger.debug(f'Received query completed: {message}')

    if message is None or not message['data']:
        logger.info('Received empty message')
        return {}

    input_rows = message['data']
    if input_rows:
        op_id = input_rows[0][1]
        query_id = input_rows[0][2]
        logger.info(f"QUERY COMPLETED: op_id={op_id}, query_id={query_id}")
        _schedule_push_results_for_query(op_id, query_id)

    output_rows = [[0, "ok"]]
    response = make_response({"data": output_rows})
    response.headers['Content-type'] = 'application/json'
    return response


def _get_error_message(msg: str) -> str:
    # remove the prefix:
    # "Uncaught exception of type 'STATEMENT_ERROR' on line 2 at position 25 : "
    if ":" in msg:
        return msg[(msg.index(":") + 1):].strip()
    return msg


@app.post("/query_failed")
def query_failed():
    message = request.json
    logger.debug(f'Received query failed: {message}')

    if message is None or not message['data']:
        logger.info('Received empty message')
        return {}

    input_rows = message['data']
    if input_rows:
        operation_id = input_rows[0][1]
        code = input_rows[0][2]
        msg = input_rows[0][3]
        state = input_rows[0][4]
        msg = _get_error_message(msg)
        logger.info(f"QUERY FAILED: op_id={operation_id}, code={code}, msg={msg}, state={state}")
        error_type = "ProgrammingError" if code in _PROGRAMMING_ERRORS else "DatabaseError"
        _push_results_to_backend(operation_id, {
            ATTRIBUTE_NAME_ERROR: msg,
            ATTRIBUTE_NAME_ERROR_ATTRS: {
                "errno": code,
                "sqlstate": state
            },
            ATTRIBUTE_NAME_ERROR_TYPE: error_type,
        })

    output_rows = [[0, "ok"]]
    response = make_response({"data": output_rows})
    response.headers['Content-type'] = 'application/json'
    return response


def _schedule_query(operation_id: str, query: str, timeout: Optional[int] = None):
    with _queries_condition:
        _pending_queries.append((operation_id, query, timeout))
        _queries_condition.notify()


def _run_query(operation_id: str, query: str, timeout: Optional[int] = None):
    try:
        timeout = timeout or 850
        with (conn := _connect()):
            with conn.cursor() as cur:
                if _SYNC_QUERIES:
                    cur.execute(query)
                    logger.info(f"Sync query executed: {operation_id} {query}, id: {cur.sfqid}")
                    _push_results_to_backend(operation_id, _result_for_query(cur))
                elif _SNOWFLAKE_SYNC_QUERIES:
                    cur.execute(f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS={timeout}")
                    cur.execute("CALL MC_APP_HELPER.MC_APP.MC_APP_EXECUTE_QUERY(?)", [query])
                    logger.info(f"Sync query executed: {operation_id} {query}")
                    _push_results_to_backend(operation_id, _result_for_query(cur))
                else:
                    execute_query = f"""
                    WITH RUN_QUERY AS PROCEDURE(op_id VARCHAR, query STRING)
                        RETURNS VARCHAR
                        LANGUAGE SQL
                        AS
                        $$
                        BEGIN
                            BEGIN
                                ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS={timeout};
                                CALL MC_APP_HELPER.MC_APP.MC_APP_EXECUTE_QUERY(:query);
                                SELECT * FROM TABLE(RESULT_SCAN(:SQLID));
                                SELECT mc_app.core.query_completed(:op_id, :SQLID);
                            EXCEPTION
                                WHEN OTHER THEN BEGIN
                                    SELECT mc_app.core.query_failed(:op_id, :sqlcode, :sqlerrm, :sqlstate);
                                END;
                            END;
                        END;
                        $$
                    CALL RUN_QUERY(?, ?);
                    """
                    cur.execute_async(execute_query, [operation_id, query])
                    # execute_query = """
                    # EXECUTE IMMEDIATE $$
                    # BEGIN
                    #     BEGIN
                    #         ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS=$var_timeout;
                    #         CALL MC_APP_HELPER.MC_APP.MC_APP_EXECUTE_QUERY($var_query);
                    #         EXECUTE IMMEDIATE 'SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))';
                    #         LET query_id := LAST_QUERY_ID();
                    #         SELECT mc_app.core.query_completed($var_op_id, :query_id);
                    #     EXCEPTION
                    #         WHEN OTHER THEN BEGIN
                    #             SELECT mc_app.core.query_failed($var_op_id, :sqlcode, :sqlerrm, :sqlstate);
                    #         END;
                    #     END;
                    # END;
                    # $$
                    # ;
                    # """
                    # cur.execute("SET var_op_id = ?", (operation_id, ))
                    # cur.execute("SET var_query = ?", (query, ))
                    # cur.execute("SET var_timeout = ?", (timeout, ))
                    # cur.execute_async(execute_query)
                    logger.info(f"Async query executed: {operation_id} {query}, id: {cur.sfqid}")
    except Exception as ex:
        logger.error(f"Query failed: {query}, error: {ex}")
        _push_results_to_backend(operation_id, _result_for_exception(ex))


def _connect():
    if os.getenv('SNOWFLAKE_HOST'):
        return snowflake.connector.connect(
            host=os.getenv('SNOWFLAKE_HOST'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=WAREHOUSE_NAME,
            token=_get_sf_login_token(),
            authenticator='oauth',
            paramstyle='qmark',
        )
    else:
        # local environment
        # return snowflake.connector.connect(
        #     user="MONTE_CARLO_TEST_DEV",
        #     password="xaBP2VZaoFuaZFe_ynHNbtXnP",
        #     account="hda34492.us-east-1",
        #     warehouse="MONTE_CARLO",
        # )
        rsa_key_file = os.getenv("RSA_KEY_FILE", os.getenv("HOME") + "/.ssh/snowflake_rsa_key.p8")
        return snowflake.connector.connect(
            account="RNB23277",
            warehouse=WAREHOUSE_NAME,
            paramstyle="qmark",
            user="MC_APP_LOCAL",
            private_key_file=rsa_key_file,
            role="MONTE_CARLO_APP_ROLE",
        )


def _schedule_push_results_for_query(operation_id: str, query_id: str):
    with _results_condition:
        _pending_results.append((operation_id, query_id))
        _results_condition.notify()


def _push_results_for_query(operation_id: str, query_id: str):
    try:
        with (conn := _connect()):
            with conn.cursor() as cur:
                conn.get_query_status_throw_if_error(query_id)
                cur.get_results_from_sfqid(query_id)
                result = _result_for_query(cur)
            _push_results_to_backend(operation_id, result)
    except Exception as ex:
        logger.error(f"Failed to push results for query: {query_id}, error: {ex}")


def _get_sf_login_token():
    with open("/snowflake/session/token", "r") as f:
        return f.read()


def _result_for_query(cursor):
    return {
        ATTRIBUTE_NAME_RESULT: {
            "all_results": cursor.fetchall(),
            "description": cursor.description,
            "rowcount": cursor.rowcount,
        },
        # ATTRIBUTE_NAME_TRACE_ID: trace_id,
    }


def _result_for_exception(ex: Exception) -> Dict:
    result = {
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


def _push_results_to_backend(operation_id: str, result: Dict):
    logger.info(f'Sending query results to backend')
    try:
        results_url = urljoin(BACKEND_SERVICE_URL, "/api/v1/agent_operation_result")
        result_str = json.dumps({
            "operation_id": operation_id,
            "agent_id": AGENT_ID,
            "result": result,
        }, cls=AgentSerializer)
        logger.info(f"Sending result to backend: {result_str[:500]}")
        response = requests.post(
            results_url,
            data=result_str,
            headers={
                "Content-Type": "application/json",
                'x-mcd-token': get_mc_login_token(),
            },
        )
        logger.info(f'Sent query results to backend, response: {response.status_code}')
    except Exception as ex:
        logger.error(f"Failed to push results to backend: {ex}")


def _run_queries():
    logger.info("Query runner started")
    while _queries_executor_running:
        with _queries_condition:
            while not _pending_queries and _queries_executor_running:
                _queries_condition.wait()
            if not _queries_executor_running:
                break
            to_execute_queries = _pending_queries.copy()
            _pending_queries.clear()
        for operation_id, next_query, timeout in to_execute_queries:
            logger.info(f"Running operation: {operation_id}, query: {next_query}")
            _run_query(operation_id, next_query, timeout)


def _download_operation(operation_id: str) -> Dict:
    url = urljoin(BACKEND_SERVICE_URL, f"/api/v1/agent/operations/{operation_id}/request")
    response = requests.get(
        url,
        headers={
            'x-mcd-token': get_mc_login_token(),
        },
    )
    return response.json()


def _get_query_from_event(event: Dict) -> Tuple[Optional[str], Optional[int]]:
    if legacy_query := event.get("query"):
        return legacy_query, None
    operation = event.get("operation", {})
    if operation.get("__mcd_size_exceeded__", False):
        logger.info("Downloading operation from orchestrator")
        operation = _download_operation(event.get("operation_id"))
    commands = operation.get("commands", [])
    timeout: Optional[int] = None
    resolved_query: Optional[str] = None
    for command in commands:
        if command.get("target") == "_cursor" and command.get("method") == "execute":
            query = command.get("args", [None])[0]
            if not query:
                continue
            if query.startswith("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS="):
                timeout = int(query.split("=")[1])
                continue
            resolved_query = query
            if not timeout:
                timeout = command.get("kwargs", {}).get("timeout")
            break
    return resolved_query, timeout


# def _run_queries_receiver():
#     while True:
#         try:
#             logger.info("Connecting SSE Client ...")
#             url = urljoin(BACKEND_SERVICE_URL, f"/stream?channel=agents.input.{_AGENT_ID}")
#             headers = {
#                 "Accept": "text/event-stream",
#                 "x-mcd-token": _get_mc_login_token(),
#             }
#             client = sseclient.SSEClient(url, headers=headers)
#             global _sse_client
#             _sse_client = client
#             for event in client:
#                 try:
#                     event = json.loads(event.data)
#                     event_type = event.get("type")
#                     if event_type in ("heartbeat", "welcome") or event.get("heartbeat"):
#                         logger.info(f"{event_type}: {event.get('ts') or event.get('heartbeat')}")
#                         with _heartbeat_condition:
#                             global _last_heartbeat
#                             _last_heartbeat = datetime.now()
#                             _heartbeat_condition.notify()
#                         continue
#                     operation_id = event.get("operation_id")
#                     query, timeout = _get_query_from_event(event)
#                     if operation_id:
#                         if query:
#                             _schedule_query(operation_id, query, timeout)
#                         else:  # connection test
#                             _push_results_to_backend(operation_id, {
#                                 ATTRIBUTE_NAME_RESULT: {
#                                     "ok": True,
#                                 },
#                                 ATTRIBUTE_NAME_TRACE_ID: operation_id,
#                             })
#                 except Exception as parse_ex:
#                     logger.debug(f"Failed to parse event: {parse_ex}, text: {event.data}")
#         except Exception as ex:
#             logger.error(f"Connection failed: {ex}")
#             time.sleep(5)


def _run_results_publisher():
    while _publisher_running:
        with _results_condition:
            while not _pending_results and _publisher_running:
                _results_condition.wait()
            if not _publisher_running:
                break
            to_push_results = _pending_results.copy()
            _pending_results.clear()
        for operation_id, query_id in to_push_results:
            logger.info(f"Running results push: {operation_id}, query_id: {query_id}")
            _push_results_for_query(operation_id, query_id)
    logger.info("Results publisher thread stopped")


def _event_handler(event: Dict):
    operation_id = event.get("operation_id")
    query, timeout = _get_query_from_event(event)
    if operation_id:
        if query:
            _schedule_query(operation_id, query, timeout)
        else:  # connection test
            _push_results_to_backend(operation_id, {
                ATTRIBUTE_NAME_RESULT: {
                    "ok": True,
                },
                ATTRIBUTE_NAME_TRACE_ID: operation_id,
            })


HTTPConnection.default_socket_options = (
        HTTPConnection.default_socket_options
        + [
            (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
        ]
)
logger.info("TCP Keep-alive enabled")
th = Thread(target=_run_queries)
th.start()
# consumer_thread = Thread(target=_run_queries_receiver)
# consumer_thread.start()
publisher_thread = Thread(target=_run_results_publisher)
publisher_thread.start()

events = EventsClient(
    base_url=BACKEND_SERVICE_URL,
    agent_id=AGENT_ID,
    handler=_event_handler,
)
events.start()
_events_clients.append(events)

if __name__ == '__main__':
    app.run(host=SERVICE_HOST, port=SERVICE_PORT)
