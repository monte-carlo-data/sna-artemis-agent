import logging
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Tuple, Optional, Any, List, Callable

import requests

from agent.backend.backend_client import BackendClient
from agent.events.events_client import EventsClient
from agent.events.sse_client_receiver import SSEClientReceiver
from agent.sna.logs_service import LogsService
from agent.sna.metrics_service import MetricsService
from agent.sna.operation_result import AgentOperationResult
from agent.sna.operations_runner import Operation, OperationsRunner
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
)
from agent.utils.utils import BACKEND_SERVICE_URL

logger = logging.getLogger(__name__)

_ATTR_NAME_OPERATION = "operation"
_ATTR_NAME_OPERATION_ID = "operation_id"
_ATTR_NAME_OPERATION_TYPE = "type"
_ATTR_NAME_PATH = "path"
_ATTR_NAME_TRACE_ID = "trace_id"
_ATTR_NAME_SIZE_EXCEEDED = "__mcd_size_exceeded__"
_ATTR_NAME_LIMIT = "limit"
_ATTR_NAME_QUERY = "query"
_ATTR_NAME_TIMEOUT = "timeout"

_ATTR_OPERATION_TYPE_SNOWFLAKE_QUERY = "snowflake_query"
_ATTR_OPERATION_TYPE_SNOWFLAKE_TEST = "snowflake_connection_test"


class OperationMatchingType(Enum):
    EQUALS = "equals"
    STARTS_WITH = "starts_with"


@dataclass
class OperationMapping:
    path: str
    method: Callable[[str, Dict[str, Any]], None]
    schedule: bool = False
    matching_type: OperationMatchingType = OperationMatchingType.EQUALS


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
        ops_runner: Optional[OperationsRunner] = None,
        results_publisher: Optional[ResultsPublisher] = None,
        events_client: Optional[EventsClient] = None,
        storage_service: Optional[StorageService] = None,
    ):
        self._queries_runner = queries_runner or QueriesRunner(handler=self._run_query)
        self._ops_runner = ops_runner or OperationsRunner(
            handler=self._execute_scheduled_operation
        )
        self._results_publisher = results_publisher or ResultsPublisher(
            handler=self._push_results
        )
        self._storage = storage_service or StorageService()

        self._events_client = events_client or EventsClient(
            receiver=SSEClientReceiver(base_url=BACKEND_SERVICE_URL),
        )
        self._operations_mapping = [
            OperationMapping(
                path="/api/v1/agent/execute/snowflake",
                matching_type=OperationMatchingType.STARTS_WITH,
                method=self._execute_snowflake_operation,
            ),
            OperationMapping(
                path="/api/v1/agent/execute/storage",
                matching_type=OperationMatchingType.STARTS_WITH,
                method=self._execute_storage_operation,
            ),
            OperationMapping(
                path="/api/v1/test/health",
                method=self._execute_health,
                schedule=True,
            ),
            OperationMapping(
                path="/api/v1/snowflake/logs",
                method=self._push_logs,
                schedule=True,
            ),
            OperationMapping(
                path="/api/v1/snowflake/metrics",
                method=self._push_metrics,
                schedule=True,
            ),
        ]

    def start(self):
        self._queries_runner.start()
        self._ops_runner.start()
        self._results_publisher.start()
        self._events_client.start(handler=self._event_handler)

    def stop(self):
        self._queries_runner.stop()
        self._ops_runner.stop()
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

    def _event_handler(self, event: Dict[str, Any]):
        """
        Invoked by events client when an event is received with an agent operation to run
        """
        operation_id = event.get(_ATTR_NAME_OPERATION_ID)
        if operation_id:
            path: str = event.get(_ATTR_NAME_PATH, "")
            if path:
                logger.info(
                    f"Received agent operation: {path}, operation_id: {operation_id}"
                )
                self._execute_operation(path, operation_id, event)

    def _execute_operation(self, path: str, operation_id: str, event: Dict[str, Any]):
        operation = event.get(_ATTR_NAME_OPERATION, {})
        if operation.get(_ATTR_NAME_SIZE_EXCEEDED, False):
            logger.info("Downloading operation from orchestrator")
            event[_ATTR_NAME_OPERATION] = BackendClient.download_operation(operation_id)

        method, schedule = self._resolve_operation_method(path)
        if schedule:
            self._schedule_operation(operation_id, event)
        elif method:
            method(operation_id, event)
        else:
            logger.error(f"Invalid path received: {path}, operation_id: {operation_id}")

    def _resolve_operation_method(
        self,
        path: str,
    ) -> Tuple[Optional[Callable[[str, Dict[str, Any]], None]], bool]:
        for op in self._operations_mapping:
            if op.matching_type == OperationMatchingType.EQUALS:
                if path == op.path:
                    return op.method, op.schedule
            elif op.matching_type == OperationMatchingType.STARTS_WITH:
                if path.startswith(op.path):
                    return op.method, op.schedule
            else:
                raise ValueError(f"Invalid matching type: {op.matching_type}")
        return None, False

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
        result = self._storage.execute_operation(decode_dictionary(event))
        BackendClient.push_results(operation_id, result)

    def _execute_health(self, operation_id: str, event: Dict[str, Any]):
        trace_id = event.get(_ATTR_NAME_OPERATION, {}).get(
            _ATTR_NAME_TRACE_ID, operation_id
        )
        health_information = self.health_information(trace_id=trace_id)
        self._schedule_push_results(operation_id, health_information)

    def _schedule_operation(self, operation_id: str, event: Dict[str, Any]):
        self._ops_runner.schedule(Operation(operation_id, event))

    def _execute_scheduled_operation(self, op: Operation):
        method, _ = self._resolve_operation_method(op.event.get(_ATTR_NAME_PATH, ""))
        if method:
            method(op.operation_id, op.event)
        else:
            logger.error(
                f"No method mapped to operation path: {op.event.get(_ATTR_NAME_PATH)}"
            )

    def _push_logs(self, operation_id: str, event: Dict[str, Any]):
        operation = event.get(_ATTR_NAME_OPERATION, {})
        trace_id = operation.get(_ATTR_NAME_TRACE_ID, operation_id)
        limit = operation.get(_ATTR_NAME_LIMIT) or 1000
        try:
            self._schedule_push_results(
                operation_id,
                {
                    ATTRIBUTE_NAME_RESULT: LogsService.get_logs(limit),
                    ATTRIBUTE_NAME_TRACE_ID: trace_id,
                },
            )
        except Exception as ex:
            self._schedule_push_results(
                operation_id, SnowflakeClient.result_for_exception(ex)
            )

    def _push_metrics(self, operation_id: str, event: Dict[str, Any]):
        operation = event.get(_ATTR_NAME_OPERATION, {})
        trace_id = operation.get(_ATTR_NAME_TRACE_ID, operation_id)
        try:
            self._schedule_push_results(
                operation_id,
                {
                    ATTRIBUTE_NAME_RESULT: MetricsService.fetch_metrics(),
                    ATTRIBUTE_NAME_TRACE_ID: trace_id,
                },
            )
        except Exception as ex:
            self._schedule_push_results(
                operation_id, SnowflakeClient.result_for_exception(ex)
            )

    @classmethod
    def _get_query_from_event(cls, event: Dict) -> Tuple[Optional[str], Optional[int]]:
        operation = event.get(_ATTR_NAME_OPERATION, {})
        operation_type = operation.get(_ATTR_NAME_OPERATION_TYPE)
        if operation_type == _ATTR_OPERATION_TYPE_SNOWFLAKE_QUERY:
            return operation.get(_ATTR_NAME_QUERY), operation.get(_ATTR_NAME_TIMEOUT)
        elif operation_type == _ATTR_OPERATION_TYPE_SNOWFLAKE_TEST:
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
