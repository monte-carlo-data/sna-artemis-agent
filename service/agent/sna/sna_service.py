import json
import logging
import os
import sys
import uuid
from typing import Dict, Tuple, Optional, Any, List
from urllib.parse import urljoin

import requests
from snowflake.connector import DatabaseError, ProgrammingError

from agent.events.events_client import EventsClient
from agent.events.receiver_factory import ReceiverFactory
from agent.events.sse_client_receiver import SSEClientReceiverFactory
from agent.sna.operation_result import AgentOperationResult
from agent.sna.queries_runner import QueriesRunner
from agent.sna.results_publisher import ResultsPublisher
from agent.sna.sf_client import SnowflakeClient
from agent.sna.sf_query import SnowflakeQuery
from agent.utils.serde import (
    AgentSerializer,
    ATTRIBUTE_NAME_ERROR_TYPE,
    ATTRIBUTE_NAME_ERROR_ATTRS,
    ATTRIBUTE_NAME_ERROR,
    ATTRIBUTE_NAME_RESULT,
    ATTRIBUTE_NAME_TRACE_ID,
)
from agent.utils.settings import VERSION, BUILD_NUMBER
from agent.utils.utils import (
    BACKEND_SERVICE_URL,
    AGENT_ID,
    get_mc_login_token,
)

HEALTH_ENV_VARS = [
    "PYTHON_VERSION",
    "SERVER_SOFTWARE",
]
logger = logging.getLogger(__name__)


class SnaService:
    """
    SNA Service, it opens a connection to the Monte Carlo backend (using the token provided
    through the Streamlit application) and waits for events including queries to be run
    in Snowflake.
    By default, queries are received from the MC backend using a SSE (Server-sent events)
    connection, but new implementations (polling, gRPC, websockets, etc.) can be implemented by
    adding new "receivers" (see ReceiverFactory and BaseReceiver).
    Queries are processed by a background thread (see QueriesRunner) and executed asynchronously,
    we're taking advantage of stored procedures and Snowflake functions to be notified when
    the execution of the query completes (see query_completed/query_failed).
    When the result is ready we send it to the MC backend using another background thread (see
    ResultsPublisher).
    """

    def __init__(
        self,
        queries_runner: Optional[QueriesRunner] = None,
        results_publisher: Optional[ResultsPublisher] = None,
        events_client: Optional[EventsClient] = None,
        receiver_factory: Optional[ReceiverFactory] = None,
    ):
        self._queries_runner = queries_runner or QueriesRunner(handler=self._run_query)
        self._results_publisher = results_publisher or ResultsPublisher(
            handler=self._push_results
        )

        if events_client:
            self._events_client = events_client
            events_client.event_handler = self._event_handler
        else:
            self._events_client = EventsClient(
                base_url=BACKEND_SERVICE_URL,
                agent_id=AGENT_ID,
                handler=self._event_handler,
                receiver_factory=receiver_factory or SSEClientReceiverFactory(),
            )

    def start(self):
        self._queries_runner.start()
        self._results_publisher.start()
        self._events_client.start()

    def stop(self):
        self._queries_runner.stop()
        self._results_publisher.stop()
        self._events_client.stop()

    @classmethod
    def health_information(cls) -> Dict[str, Any]:
        return {
            "platform": "SNA",
            "version": VERSION,
            "build": BUILD_NUMBER,
            "env": cls._env_dictionary(),
        }

    def run_reachability_test(self) -> Dict[str, Any]:
        trace_id = str(uuid.uuid4())
        logger.info(f"Running reachability test, trace_id: {trace_id}")
        return self._execute_backend_operation(f"/api/v1/test/ping?trace_id={trace_id}")

    def query_completed(self, operation_id: str, query_id: str):
        """
        Invoked by the Snowflake stored procedure when a query execution is completed
        """
        self._schedule_push_results_for_query(operation_id, query_id)

    def query_failed(self, operation_id: str, code: int, msg: str, state: str):
        """
        Invoked by the Snowflake stored procedure when a query execution failed
        """
        result = SnowflakeClient.result_for_query_failed(operation_id, code, msg, state)
        self._push_results_to_backend(operation_id, result)

    @staticmethod
    def fetch_metrics() -> List[str]:
        """
        Fetches metrics using Snowpark Monitoring Services:
        https://docs.snowflake.com/en/developer-guide/snowpark-container-services/monitoring-services#accessing-compute-pool-metrics
        """
        response = requests.get(
            "http://discover.monitor.mc_app_compute_pool.snowflakecomputing.internal:9001/metrics"
        )
        lines = response.text.splitlines()
        return lines

    def push_metrics(self):
        """
        Fetches metrics using Snowpark Monitoring Services:
        https://docs.snowflake.com/en/developer-guide/snowpark-container-services/monitoring-services#accessing-compute-pool-metrics
        and pushes them to the MC backend.
        """
        metrics = self.fetch_metrics()
        self._push_results_to_backend(
            "metrics",
            {
                "metrics": metrics,
            },
        )

    def _event_handler(self, event: Dict[str, Any]):
        """
        Invoked by events client when an event is received with an agent operation to run
        """
        operation_id = event.get("operation_id")
        if operation_id:
            path: str = event.get("path", "")
            if path:
                logger.info(
                    f"Received agent operation: {path}, operation_id: {operation_id}"
                )
                self._execute_operation(path, operation_id, event)

    def _execute_operation(self, path: str, operation_id: str, event: Dict[str, Any]):
        if path.startswith("/api/v1/agent/execute/"):
            self._execute_agent_operation(operation_id, event)
        elif path == "/api/v1/test/health":
            self._execute_health(operation_id)
        else:
            logger.error(f"Invalid path received: {path}, operation_id: {operation_id}")

    def _execute_agent_operation(self, operation_id: str, event: Dict[str, Any]):
        query, timeout = self._get_query_from_event(operation_id, event)
        if query:
            self._schedule_query(operation_id, query, timeout)
        else:  # connection test
            self._push_results_to_backend(
                operation_id,
                {
                    ATTRIBUTE_NAME_RESULT: {
                        "ok": True,
                    },
                    ATTRIBUTE_NAME_TRACE_ID: operation_id,
                },
            )

    def _execute_health(self, operation_id: str):
        health_information = self.health_information()
        self._schedule_push_results(operation_id, health_information)

    @classmethod
    def _get_query_from_event(
        cls, operation_id: str, event: Dict
    ) -> Tuple[Optional[str], Optional[int]]:
        if legacy_query := event.get("query"):
            return legacy_query, None
        operation = event.get("operation", {})
        if operation.get("__mcd_size_exceeded__", False):
            logger.info("Downloading operation from orchestrator")
            operation = cls._download_operation(operation_id)
        commands = operation.get("commands", [])
        timeout: Optional[int] = None
        resolved_query: Optional[str] = None
        for command in commands:
            if (
                command.get("target") == "_cursor"
                and command.get("method") == "execute"
            ):
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

    def _schedule_query(self, operation_id: str, query: str, timeout: Optional[int]):
        self._queries_runner.schedule(SnowflakeQuery(operation_id, query, timeout))

    def _run_query(self, query: SnowflakeQuery):
        """
        Invoked by queries runner to run a query
        """
        try:
            result = SnowflakeClient.run_query(query)
            # if there's no result, the query was executed asynchronously
            # we'll get the result through query_completed/query_failed callbacks
            if result:
                self._push_results_to_backend(query.operation_id, result)
        except Exception as ex:
            logger.error(f"Query failed: {query.query}, error: {ex}")
            self._push_results_to_backend(
                query.operation_id, self._result_for_exception(ex)
            )

    def _schedule_push_results_for_query(self, operation_id: str, query_id: str):
        self._results_publisher.schedule_push_query_results(operation_id, query_id)

    def _schedule_push_results(self, operation_id: str, result: Dict[str, Any]):
        self._results_publisher.schedule_push_results(operation_id, result)

    @classmethod
    def _push_results(cls, result: AgentOperationResult):
        if result.query_id:
            cls._push_results_for_query(result.operation_id, result.query_id)
        elif result.result is not None:
            cls._push_results_to_backend(result.operation_id, result.result)
        else:
            logger.error(f"Invalid result for operation: {result.operation_id}")

    @classmethod
    def _push_results_for_query(cls, operation_id: str, query_id: str):
        """
        Invoked by results publisher to push results for a query
        """
        try:
            result = SnowflakeClient.result_for_query(query_id)
            cls._push_results_to_backend(operation_id, result)
        except Exception as ex:
            logger.error(f"Failed to push results for query: {query_id}, error: {ex}")

    @staticmethod
    def _result_for_exception(ex: Exception) -> Dict:
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
    def _push_results_to_backend(operation_id: str, result: Dict):
        logger.info(f"Sending query results to backend")
        try:
            results_url = urljoin(
                BACKEND_SERVICE_URL, f"/api/v1/agent/operations/{operation_id}/result"
            )
            result_str = json.dumps(
                {
                    "agent_id": AGENT_ID,
                    "result": result,
                },
                cls=AgentSerializer,
            )
            logger.info(f"Sending result to backend: {result_str[:500]}")
            response = requests.post(
                results_url,
                data=result_str,
                headers={
                    "Content-Type": "application/json",
                    **get_mc_login_token(),
                },
            )
            logger.info(
                f"Sent query results to backend, response: {response.status_code}"
            )
        except Exception as ex:
            logger.error(f"Failed to push results to backend: {ex}")

    @staticmethod
    def _execute_backend_operation(
        path: str, method: str = "GET", body: Optional[Dict] = None
    ) -> Dict[str, Any]:
        try:
            url = urljoin(BACKEND_SERVICE_URL, path)
            response = requests.request(
                method=method,
                url=url,
                json=body,
                headers={
                    "Content-Type": "application/json",
                    **get_mc_login_token(),
                },
            )
            logger.info(
                f"Sent backend request {path}, response: {response.status_code}"
            )
            response.raise_for_status()
            return response.json() or {"error": "empty response"}
        except Exception as ex:
            logger.error(f"Error sending request to backend: {ex}")
            return {
                "error": str(ex),
            }

    @classmethod
    def _download_operation(cls, operation_id: str) -> Dict:
        url = urljoin(
            BACKEND_SERVICE_URL, f"/api/v1/agent/operations/{operation_id}/request"
        )
        response = requests.get(
            url,
            headers={
                **get_mc_login_token(),
            },
        )
        return response.json()

    @staticmethod
    def _env_dictionary() -> Dict:
        env: Dict[str, Optional[str]] = {
            "PYTHON_SYS_VERSION": sys.version,
            "CPU_COUNT": str(os.cpu_count()),
        }
        env.update(
            {
                env_var: os.getenv(env_var)
                for env_var in HEALTH_ENV_VARS
                if os.getenv(env_var)
            }
        )
        return env
