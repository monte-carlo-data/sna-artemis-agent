import logging
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Tuple, Optional, Any, Callable

from agent.backend.backend_client import BackendClient
from agent.events.ack_sender import AckSender
from agent.events.events_client import EventsClient
from agent.events.sse_client_receiver import SSEClientReceiver
from agent.sna.config.config_manager import ConfigurationManager
from agent.sna.config.config_keys import (
    CONFIG_OPS_RUNNER_THREAD_COUNT,
    CONFIG_PUBLISHER_THREAD_COUNT,
    CONFIG_QUERIES_RUNNER_THREAD_COUNT,
)
from agent.sna.logs_service import LogsService
from agent.sna.metrics_service import MetricsService
from agent.sna.operation_result import AgentOperationResult, OperationAttributes
from agent.sna.operations_runner import Operation, OperationsRunner
from agent.sna.queries_runner import QueriesRunner
from agent.sna.queries_service import QueriesService
from agent.sna.results_processor import ResultsProcessor
from agent.sna.results_publisher import ResultsPublisher
from agent.sna.sf_queries import QUERY_RESTART_SERVICE
from agent.sna.sf_query import SnowflakeQuery
from agent.storage.storage_service import StorageService
from agent.utils import utils
from agent.utils.serde import (
    ATTRIBUTE_NAME_RESULT,
    ATTRIBUTE_NAME_TRACE_ID,
    decode_dictionary,
)
from agent.utils.settings import VERSION, BUILD_NUMBER
from agent.utils.utils import BACKEND_SERVICE_URL

logger = logging.getLogger(__name__)

_ATTR_NAME_OPERATION = "operation"
_ATTR_NAME_OPERATION_ID = "operation_id"
_ATTR_NAME_OPERATION_TYPE = "type"
_ATTR_NAME_PATH = "path"
_ATTR_NAME_TRACE_ID = "trace_id"
_ATTR_NAME_LIMIT = "limit"
_ATTR_NAME_QUERY = "query"
_ATTR_NAME_TIMEOUT = "timeout"
_ATTR_NAME_COMPRESS_RESPONSE_FILE = "compress_response_file"
_ATTR_NAME_RESPONSE_SIZE_LIMIT_BYTES = "response_size_limit_bytes"
_ATTR_NAME_EVENTS = "events"
_ATTR_NAME_PARAMETERS = "parameters"
_ATTR_NAME_CONFIG = "config"

_ATTR_NAME_SIZE_EXCEEDED = "__mcd_size_exceeded__"

_ATTR_OPERATION_TYPE_SNOWFLAKE_QUERY = "snowflake_query"
_ATTR_OPERATION_TYPE_SNOWFLAKE_TEST = "snowflake_connection_test"
_ATTR_OPERATION_TYPE_PUSH_METRICS = "push_metrics"
_PATH_PUSH_METRICS = "push_metrics"

_DEFAULT_COMPRESS_RESPONSE_FILE = True
_DEFAULT_RESPONSE_SIZE_LIMIT_BYTES = (
    5000000  # 5Mb, the same default value we have on the DC side
)


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
        config_manager: ConfigurationManager,
        queries_runner: Optional[QueriesRunner] = None,
        ops_runner: Optional[OperationsRunner] = None,
        results_publisher: Optional[ResultsPublisher] = None,
        events_client: Optional[EventsClient] = None,
        storage_service: Optional[StorageService] = None,
        ack_sender: Optional[AckSender] = None,
        queries_service: Optional[QueriesService] = None,
        logs_service: Optional[LogsService] = None,
    ):
        self._config_manager = config_manager
        self._queries_runner = queries_runner or QueriesRunner(
            handler=self._run_query,
            thread_count=config_manager.get_int_value(
                CONFIG_QUERIES_RUNNER_THREAD_COUNT, 1
            ),
        )
        self._ops_runner = ops_runner or OperationsRunner(
            handler=self._execute_scheduled_operation,
            thread_count=config_manager.get_int_value(
                CONFIG_OPS_RUNNER_THREAD_COUNT, 1
            ),
        )
        self._results_publisher = results_publisher or ResultsPublisher(
            handler=self._push_results,
            thread_count=config_manager.get_int_value(CONFIG_PUBLISHER_THREAD_COUNT, 1),
        )
        self._ack_sender = ack_sender or AckSender()
        self._queries_service = queries_service or QueriesService(
            config_manager=config_manager
        )
        self._logs_service = logs_service or LogsService(
            queries_service=self._queries_service
        )
        self._storage = storage_service or StorageService(
            config_manager=config_manager,
            queries_service=self._queries_service,
        )
        self._results_processor = ResultsProcessor(
            config_manager=self._config_manager,
            storage=self._storage,
        )

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
                schedule=True,
            ),
            OperationMapping(
                path="/api/v1/test/health",
                method=self._execute_health,
                schedule=True,
            ),
            OperationMapping(
                path="/api/v1/snowflake/logs",
                method=self._execute_get_logs,
                schedule=True,
            ),
            OperationMapping(
                path="/api/v1/snowflake/metrics",
                method=self._execute_get_metrics,
                schedule=True,
            ),
            OperationMapping(
                path=_PATH_PUSH_METRICS,
                method=self._execute_push_metrics,
                schedule=True,
            ),
            OperationMapping(
                path="/api/v1/upgrade",
                method=self._execute_upgrade,
                schedule=True,
            ),
        ]

    def start(self):
        self._queries_runner.start()
        self._ops_runner.start()
        self._results_publisher.start()
        self._events_client.start(handler=self._event_handler)
        self._ack_sender.start(handler=self._send_ack)

        logger.info(f"SNA Service Started: v{VERSION} (build #{BUILD_NUMBER})")

    def stop(self):
        self._queries_runner.stop()
        self._ops_runner.stop()
        self._results_publisher.stop()
        self._events_client.stop()
        self._ack_sender.stop()

    def health_information(self, trace_id: Optional[str] = None) -> Dict[str, Any]:
        health_info = utils.health_information(trace_id)
        health_info[_ATTR_NAME_PARAMETERS] = self._config_manager.get_all_values()
        return health_info

    def run_reachability_test(self, trace_id: Optional[str] = None) -> Dict[str, Any]:
        trace_id = trace_id or str(uuid.uuid4())
        logger.info(f"Running reachability test, trace_id: {trace_id}")
        return BackendClient.execute_operation(f"/api/v1/test/ping?trace_id={trace_id}")

    def query_completed(self, operation_json: str, query_id: str):
        """
        Invoked by the Snowflake stored procedure when a query execution is completed
        """
        operation_attributes = OperationAttributes.from_json(operation_json)
        operation_id = operation_attributes.operation_id
        logger.info(f"Query completed: {operation_id}, query_id: {query_id}")
        self._schedule_push_results_for_query(
            operation_id, query_id, operation_attributes
        )

    def query_failed(self, operation_json: str, code: int, msg: str, state: str):
        """
        Invoked by the Snowflake stored procedure when a query execution failed
        """
        operation_attributes = OperationAttributes.from_json(operation_json)
        operation_id = operation_attributes.operation_id
        logger.info(f"Query failed: {operation_id}: {msg}")
        result = QueriesService.result_for_query_failed(operation_id, code, msg, state)
        self._schedule_push_results(
            operation_id=operation_id,
            result=result,
            operation_attrs=operation_attributes,
        )

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
                self._ack_sender.schedule_ack(operation_id)
                self._execute_operation(path, operation_id, event)
        elif op_type := (event.get(_ATTR_NAME_OPERATION_TYPE)):
            if op_type == _ATTR_OPERATION_TYPE_PUSH_METRICS:
                self._push_metrics()

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
        query, timeout, operation_attrs = self._get_query_from_event(event)
        if query and operation_attrs:
            self._schedule_query(operation_id, query, timeout, operation_attrs)
        else:  # connection test
            self._schedule_push_results(
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
        self._schedule_push_results(operation_id, result)

    def _execute_health(self, operation_id: str, event: Dict[str, Any]):
        try:
            trace_id = event.get(_ATTR_NAME_OPERATION, {}).get(
                _ATTR_NAME_TRACE_ID, operation_id
            )
            health_information = self.health_information(trace_id=trace_id)
            self._schedule_push_results(operation_id, health_information)
        except Exception as ex:
            self._schedule_push_results(
                operation_id, QueriesService.result_for_exception(ex)
            )

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
            self._schedule_push_results(
                op.operation_id,
                QueriesService.result_for_error_message(
                    f"Unsupported operation path: {op.event.get(_ATTR_NAME_PATH)}"
                ),
            )

    def _execute_get_logs(self, operation_id: str, event: Dict[str, Any]):
        operation = event.get(_ATTR_NAME_OPERATION, {})
        trace_id = operation.get(_ATTR_NAME_TRACE_ID, operation_id)
        limit = operation.get(_ATTR_NAME_LIMIT) or 1000
        try:
            self._schedule_push_results(
                operation_id,
                {
                    ATTRIBUTE_NAME_RESULT: {
                        _ATTR_NAME_EVENTS: self._logs_service.get_logs(limit),
                    },
                    ATTRIBUTE_NAME_TRACE_ID: trace_id,
                },
            )
        except Exception as ex:
            self._schedule_push_results(
                operation_id, QueriesService.result_for_exception(ex)
            )

    def _execute_get_metrics(self, operation_id: str, event: Dict[str, Any]):
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
                operation_id, QueriesService.result_for_exception(ex)
            )

    def _push_metrics(self):
        self._schedule_operation(
            _PATH_PUSH_METRICS, {_ATTR_NAME_PATH: _PATH_PUSH_METRICS}
        )

    def _execute_push_metrics(self, operation_id: str, event: Dict[str, Any]):
        payload = {
            "format": "prometheus",
            "metrics": MetricsService.fetch_metrics(),
        }
        BackendClient.execute_operation("/api/v1/agent/metrics", "POST", payload)

    def _execute_upgrade(self, operation_id: str, event: Dict[str, Any]):
        """
        Compatible with /api/v1/upgrade operation from other platforms.
        It updates the configuration if there are parameters under operation and restarts the
        service.
        """
        try:
            operation = event.get(_ATTR_NAME_OPERATION, {})
            updates = operation.get(_ATTR_NAME_PARAMETERS, {})
            trace_id = operation.get(_ATTR_NAME_TRACE_ID, operation_id)
            if updates:
                self._config_manager.set_values(updates)
            self._restart_service()
            BackendClient.push_results(
                operation_id,
                {
                    ATTRIBUTE_NAME_RESULT: {
                        "updated": True,
                    },
                    ATTRIBUTE_NAME_TRACE_ID: trace_id,
                },
            )
        except Exception as ex:
            self._schedule_push_results(
                operation_id, QueriesService.result_for_exception(ex)
            )

    @classmethod
    def _get_query_from_event(
        cls,
        event: Dict,
    ) -> Tuple[Optional[str], Optional[int], Optional[OperationAttributes]]:
        operation = event.get(_ATTR_NAME_OPERATION, {})
        operation_type = operation.get(_ATTR_NAME_OPERATION_TYPE)
        operation_id = event.get(_ATTR_NAME_OPERATION_ID)
        if operation_id and operation_type == _ATTR_OPERATION_TYPE_SNOWFLAKE_QUERY:
            return (
                operation.get(_ATTR_NAME_QUERY),
                operation.get(_ATTR_NAME_TIMEOUT),
                OperationAttributes(
                    operation_id=operation_id,
                    compress_response_file=operation.get(
                        _ATTR_NAME_COMPRESS_RESPONSE_FILE,
                        _DEFAULT_COMPRESS_RESPONSE_FILE,
                    ),
                    response_size_limit_bytes=operation.get(
                        _ATTR_NAME_RESPONSE_SIZE_LIMIT_BYTES,
                        _DEFAULT_RESPONSE_SIZE_LIMIT_BYTES,
                    ),
                    trace_id=operation.get(_ATTR_NAME_TRACE_ID) or str(uuid.uuid4()),
                ),
            )
        elif operation_type == _ATTR_OPERATION_TYPE_SNOWFLAKE_TEST:
            return None, None, None
        else:
            raise ValueError(f"Invalid operation type: {operation_type}")

    def _schedule_query(
        self,
        operation_id: str,
        query: str,
        timeout: Optional[int],
        operation_attrs: OperationAttributes,
    ):
        self._queries_runner.schedule(
            SnowflakeQuery(
                operation_id=operation_id,
                query=query,
                timeout=timeout,
                operation_attrs=operation_attrs,
            )
        )

    def _send_ack(self, operation_id: str):
        logger.info(f"Sending ACK for operation={operation_id}")
        BackendClient.execute_operation(
            f"/api/v1/agent/operations/{operation_id}/ack", "POST"
        )

    def _run_query(self, query: SnowflakeQuery):
        """
        Invoked by queries runner to run a query
        """
        try:
            result = self._queries_service.run_query(query)
            # if there's no result, the query was executed asynchronously
            # we'll get the result through query_completed/query_failed callbacks
            if result:
                self._schedule_push_results(
                    operation_id=query.operation_id,
                    result=result,
                    operation_attrs=query.operation_attrs,
                )
        except Exception as ex:
            logger.error(f"Query failed: {query.query}, error: {ex}")
            self._schedule_push_results(
                query.operation_id, QueriesService.result_for_exception(ex)
            )

    def _schedule_push_results_for_query(
        self,
        operation_id: str,
        query_id: str,
        operation_attrs: OperationAttributes,
    ):
        self._results_publisher.schedule_push_query_results(
            operation_id, query_id, operation_attrs
        )

    def _schedule_push_results(
        self,
        operation_id: str,
        result: Dict[str, Any],
        operation_attrs: Optional[OperationAttributes] = None,
    ):
        self._results_publisher.schedule_push_results(
            operation_id=operation_id,
            result=result,
            operation_attrs=operation_attrs,
        )

    def _push_results(self, result: AgentOperationResult):
        self._ack_sender.operation_completed(result.operation_id)
        if result.query_id and result.operation_attrs is not None:
            self._push_results_for_query(
                result.operation_id, result.query_id, result.operation_attrs
            )
        elif result.result is not None:
            self._push_backend_results(
                result.operation_id, result.result, result.operation_attrs
            )
        else:
            logger.error(f"Invalid result for operation: {result.operation_id}")

    def _push_results_for_query(
        self, operation_id: str, query_id: str, operation_attrs: OperationAttributes
    ):
        """
        Invoked by results publisher to push results for a query
        """
        try:
            result = self._queries_service.result_for_query(query_id)
            self._push_backend_results(operation_id, result, operation_attrs)
        except Exception as ex:
            logger.error(f"Failed to push results for query: {query_id}, error: {ex}")

    def _restart_service(self):
        query_id = self._queries_service.run_query_async(QUERY_RESTART_SERVICE)
        logger.info(f"Restarted service, query ID: {query_id}")

    def _push_backend_results(
        self,
        operation_id: str,
        result: Dict[str, Any],
        operation_attrs: Optional[OperationAttributes],
    ):
        if operation_attrs:
            if not _ATTR_NAME_TRACE_ID in result:
                result[ATTRIBUTE_NAME_TRACE_ID] = operation_attrs.trace_id
            result = self._results_processor.process_result(result, operation_attrs)
        BackendClient.push_results(operation_id, result)
