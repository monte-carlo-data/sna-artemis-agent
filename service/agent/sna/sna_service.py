import logging
import uuid
from typing import Dict, Tuple, Optional, Any, List

import requests

from agent.backend.backend_client import BackendClient
from agent.events.events_client import EventsClient
from agent.events.receiver_factory import ReceiverFactory
from agent.events.sse_client_receiver import SSEClientReceiverFactory
from agent.sna.operation_result import AgentOperationResult
from agent.sna.queries_runner import QueriesRunner
from agent.sna.results_publisher import ResultsPublisher
from agent.sna.sf_client import SnowflakeClient
from agent.sna.sf_query import SnowflakeQuery
from agent.storage.storage_service import StorageService
from agent.utils import utils
from agent.utils.serde import (
    ATTRIBUTE_NAME_RESULT,
    ATTRIBUTE_NAME_TRACE_ID,
    decode_dictionary,
    ATTRIBUTE_NAME_ERROR_TYPE,
    ATTRIBUTE_NAME_ERROR,
)
from agent.utils.utils import (
    BACKEND_SERVICE_URL,
    AGENT_ID,
)

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
        storage_service: Optional[StorageService] = None,
    ):
        self._queries_runner = queries_runner or QueriesRunner(handler=self._run_query)
        self._results_publisher = results_publisher or ResultsPublisher(
            handler=self._push_results
        )
        self._storage = storage_service or StorageService()

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
    def health_information(cls, trace_id: Optional[str] = None) -> Dict[str, Any]:
        return utils.health_information(trace_id)

    def run_reachability_test(self, trace_id: Optional[str] = None) -> Dict[str, Any]:
        trace_id = trace_id or str(uuid.uuid4())
        logger.info(f"Running reachability test, trace_id: {trace_id}")
        return BackendClient.execute_operation(f"/api/v1/test/ping?trace_id={trace_id}")

    def query_completed(self, operation_id: str, query_id: str):
        """
        Invoked by the Snowflake stored procedure when a query execution is completed
        """
        logger.info(f"Query completed: {operation_id}, query_id: {query_id}")
        self._schedule_push_results_for_query(operation_id, query_id)

    def query_failed(self, operation_id: str, code: int, msg: str, state: str):
        """
        Invoked by the Snowflake stored procedure when a query execution failed
        """
        logger.info(f"Query failed: {operation_id}: {msg}")
        result = SnowflakeClient.result_for_query_failed(operation_id, code, msg, state)
        self._schedule_push_results(operation_id, result)

    @staticmethod
    def fetch_metrics() -> List[str]:
        """
        Fetches metrics using Snowpark Monitoring Services:
        https://docs.snowflake.com/en/developer-guide/snowpark-container-services/monitoring-services#accessing-compute-pool-metrics
        """
        response = requests.get(
            "http://discover.monitor.mcd_agent_compute_pool.snowflakecomputing.internal:9001/metrics"
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
        BackendClient.push_results(
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
        operation = event.get("operation", {})
        if operation.get("__mcd_size_exceeded__", False):
            logger.info("Downloading operation from orchestrator")
            event["operation"] = BackendClient.download_operation(operation_id)

        if path.startswith("/api/v1/agent/execute/snowflake"):
            self._execute_snowflake_operation(operation_id, event)
        elif path.startswith("/api/v1/agent/execute/storage"):
            self._execute_storage_operation(operation_id, event)
        elif path == "/api/v1/test/health":
            self._execute_health(operation_id, event)
        else:
            logger.error(f"Invalid path received: {path}, operation_id: {operation_id}")

    def _execute_snowflake_operation(self, operation_id: str, event: Dict[str, Any]):
        query, timeout = self._get_query_from_event(event)
        if query:
            self._schedule_query(operation_id, query, timeout)
        else:  # connection test
            BackendClient.push_results(
                operation_id,
                {
                    ATTRIBUTE_NAME_RESULT: {
                        "ok": True,
                    },
                    ATTRIBUTE_NAME_TRACE_ID: operation_id,
                },
            )

    def _execute_storage_operation(self, operation_id: str, event: Dict[str, Any]):
        try:
            storage_result = self._storage.execute_operation(decode_dictionary(event))
            result = {
                ATTRIBUTE_NAME_RESULT: storage_result,
            }
        except Exception as ex:
            logger.error(f"Storage operation failed: {ex}")
            result = {
                ATTRIBUTE_NAME_ERROR_TYPE: type(ex).__name__,
                ATTRIBUTE_NAME_ERROR: str(ex),
            }

        BackendClient.push_results(operation_id, result)

    def _execute_health(self, operation_id: str, event: Dict[str, Any]):
        trace_id = event.get("operation", {}).get("trace_id", operation_id)
        health_information = self.health_information(trace_id=trace_id)
        self._schedule_push_results(operation_id, health_information)

    @classmethod
    def _get_query_from_event(cls, event: Dict) -> Tuple[Optional[str], Optional[int]]:
        if legacy_query := event.get("query"):
            return legacy_query, None
        operation = event.get("operation", {})
        operation_type = operation.get("type")
        if operation_type == "snowflake_query":
            return operation.get("query"), operation.get("timeout")
        elif operation_type == "snowflake_connection_test":
            return None, None
        else:
            raise ValueError(f"Invalid operation type: {operation_type}")

    def _schedule_query(self, operation_id: str, query: str, timeout: Optional[int]):
        self._queries_runner.schedule(SnowflakeQuery(operation_id, query, timeout))

    @staticmethod
    def _run_query(query: SnowflakeQuery):
        """
        Invoked by queries runner to run a query
        """
        try:
            result = SnowflakeClient.run_query(query)
            # if there's no result, the query was executed asynchronously
            # we'll get the result through query_completed/query_failed callbacks
            if result:
                BackendClient.push_results(query.operation_id, result)
        except Exception as ex:
            logger.error(f"Query failed: {query.query}, error: {ex}")
            BackendClient.push_results(
                query.operation_id, SnowflakeClient.result_for_exception(ex)
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
            BackendClient.push_results(result.operation_id, result.result)
        else:
            logger.error(f"Invalid result for operation: {result.operation_id}")

    @classmethod
    def _push_results_for_query(cls, operation_id: str, query_id: str):
        """
        Invoked by results publisher to push results for a query
        """
        try:
            result = SnowflakeClient.result_for_query(query_id)
            BackendClient.push_results(operation_id, result)
        except Exception as ex:
            logger.error(f"Failed to push results for query: {query_id}, error: {ex}")
