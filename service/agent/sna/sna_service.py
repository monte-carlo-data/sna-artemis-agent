import logging
import uuid
from typing import Dict, Tuple, Optional, Any, List

from apollo.common.agent.constants import ATTRIBUTE_NAME_RESULT, ATTRIBUTE_NAME_TRACE_ID
from apollo.common.agent.serde import decode_dictionary
from apollo.egress.agent.config.config_keys import (
    CONFIG_QUERIES_RUNNER_THREAD_COUNT,
    CONFIG_IS_REMOTE_UPGRADABLE,
)
from apollo.egress.agent.config.config_manager import ConfigurationManager
from apollo.egress.agent.events.ack_sender import AckSender
from apollo.egress.agent.events.events_client import EventsClient
from apollo.egress.agent.service.base_egress_service import (
    BaseEgressAgentService,
    ATTR_NAME_OPERATION,
    ATTR_NAME_OPERATION_TYPE,
    ATTR_NAME_OPERATION_ID,
    ATTR_NAME_QUERY,
    ATTR_NAME_TIMEOUT,
    ATTR_NAME_COMPRESS_RESPONSE_FILE,
    ATTR_NAME_RESPONSE_SIZE_LIMIT_BYTES,
    ATTR_NAME_JOB_TYPE,
    ATTR_NAME_TRACE_ID,
    ATTR_NAME_PATH,
    OperationMapping,
    ATTR_NAME_PARAMETERS,
)
from apollo.egress.agent.service.login_token_provider import (
    LocalLoginTokenProvider,
    LoginTokenProvider,
)
from apollo.egress.agent.service.operation_result import OperationAttributes
from apollo.egress.agent.service.operations_runner import OperationsRunner
from apollo.egress.agent.service.results_publisher import ResultsPublisher
from apollo.egress.agent.utils.utils import LOCAL

from agent.sna.logs_service import LogsService
from agent.sna.metrics_service import MetricsService
from agent.sna.queries_runner import QueriesRunner
from agent.sna.queries_service import QueriesService
from agent.sna.sf_queries import QUERY_RESTART_SERVICE
from agent.sna.sf_query import SnowflakeQuery
from apollo.egress.agent.service.timer_service import TimerService

from agent.sna.sna_login_token_provider import SNALoginTokenProvider
from agent.storage.storage_service import StorageService
from agent.utils.settings import VERSION, BUILD_NUMBER
from agent.utils.utils import BACKEND_SERVICE_URL

logger = logging.getLogger(__name__)

_ATTR_OPERATION_TYPE_SNOWFLAKE_QUERY = "snowflake_query"
_ATTR_OPERATION_TYPE_SNOWFLAKE_TEST = "snowflake_connection_test"

_DEFAULT_COMPRESS_RESPONSE_FILE = True
_DEFAULT_RESPONSE_SIZE_LIMIT_BYTES = (
    20000000  # 20Mb, the same default value we have on the DC side for Snowflake agents
)

_SNOWFLAKE_HEALTH_ENV_VARS = [
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_HOST",
    "SNOWFLAKE_SERVICE_NAME",
]


class SnowflakeAgentError(Exception):
    pass


class SnaService(BaseEgressAgentService):
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
        logs_sender: Optional[TimerService] = None,
        login_token_provider: Optional[LoginTokenProvider] = None,
        enable_pre_signed_urls: bool = False,
    ):
        self._queries_service = queries_service or QueriesService(
            config_manager=config_manager
        )
        self._sna_storage = storage_service or StorageService(
            config_manager=config_manager,
            queries_service=self._queries_service,
        )
        if not login_token_provider:
            login_token_provider = (
                LocalLoginTokenProvider() if LOCAL else SNALoginTokenProvider()
            )
        super().__init__(
            backend_service_url=BACKEND_SERVICE_URL,
            platform="SNA",
            service_name="SNA",
            additional_env_vars=_SNOWFLAKE_HEALTH_ENV_VARS,
            config_manager=config_manager,
            login_token_provider=login_token_provider,
            logs_service=logs_service
            or LogsService(
                queries_service=self._queries_service,
            ),
            metrics_service=MetricsService(),
            storage_service=self._sna_storage,
            ops_runner=ops_runner,
            results_publisher=results_publisher,
            events_client=events_client,
            ack_sender=ack_sender,
            logs_sender=logs_sender,
            enable_pre_signed_urls=enable_pre_signed_urls,
        )
        self._queries_runner = queries_runner or QueriesRunner(
            handler=self._run_query,
            thread_count=config_manager.get_int_value(
                CONFIG_QUERIES_RUNNER_THREAD_COUNT, 1
            ),
        )
        self._operations_mapping.append(
            OperationMapping(
                path="/api/v1/snowflake/logs",
                method=self._execute_get_logs,
                schedule=True,
            )
        )
        self._operations_mapping.append(
            OperationMapping(
                path="/api/v1/snowflake/metrics",
                method=self._execute_get_metrics,
                schedule=True,
            )
        )
        self._operations_mapping.append(
            OperationMapping(
                path="/api/v1/upgrade",
                method=self._execute_upgrade,
                schedule=True,
            )
        )

    def start(self):
        self._queries_runner.start()
        super().start()

    def stop(self):
        self._queries_runner.stop()
        super().stop()

    def _execute_agent_operation(self, operation_id: str, event: Dict[str, Any]):
        path: str = event[ATTR_NAME_PATH]
        if path:
            if path.startswith("/api/v1/agent/execute/snowflake/"):
                self._execute_snowflake_operation(operation_id, event)
            elif path.startswith("/api/v1/agent/execute/storage/"):
                self._execute_storage_operation(operation_id, event)
        raise Exception(f"Unsupported operation path: {path}")

    def _internal_execute_agent_operation(
        self, event: Dict[str, Any]
    ) -> Dict[str, Any]:
        raise NotImplementedError()

    def _get_version(self) -> str:
        return VERSION

    def _get_build_number(self) -> str:
        return BUILD_NUMBER

    def fetch_metrics(self) -> List[str]:
        return self._metrics_service.fetch_metrics()

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
        result = self._sna_storage.execute_operation(decode_dictionary(event))
        self._schedule_push_results(operation_id, result)

    def _execute_upgrade(self, operation_id: str, event: Dict[str, Any]):
        """
        Compatible with /api/v1/upgrade operation from other platforms.
        It updates the configuration if there are parameters under operation and restarts the
        service.
        """
        try:
            if not self._config_manager.get_bool_value(
                CONFIG_IS_REMOTE_UPGRADABLE, True
            ):
                raise SnowflakeAgentError("Remote upgrades are disabled")
            operation = event.get(ATTR_NAME_OPERATION, {})
            updates = operation.get(ATTR_NAME_PARAMETERS, {})
            trace_id = operation.get(ATTR_NAME_TRACE_ID, operation_id)
            if updates:
                self._config_manager.set_values(updates)
            self._restart_service()
            self._backend_client.push_results(
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

    def _restart_service(self):
        query_id = self._queries_service.run_query_async(QUERY_RESTART_SERVICE)
        logger.info(f"Restarted service, query ID: {query_id}")

    @classmethod
    def _get_query_from_event(
        cls,
        event: Dict,
    ) -> Tuple[Optional[str], Optional[int], Optional[OperationAttributes]]:
        operation = event.get(ATTR_NAME_OPERATION, {})
        operation_type = operation.get(ATTR_NAME_OPERATION_TYPE)
        operation_id = event.get(ATTR_NAME_OPERATION_ID)
        if operation_id and operation_type == _ATTR_OPERATION_TYPE_SNOWFLAKE_QUERY:
            return (
                operation.get(ATTR_NAME_QUERY),
                operation.get(ATTR_NAME_TIMEOUT),
                OperationAttributes(
                    operation_id=operation_id,
                    compress_response_file=operation.get(
                        ATTR_NAME_COMPRESS_RESPONSE_FILE,
                        _DEFAULT_COMPRESS_RESPONSE_FILE,
                    ),
                    response_size_limit_bytes=operation.get(
                        ATTR_NAME_RESPONSE_SIZE_LIMIT_BYTES,
                        _DEFAULT_RESPONSE_SIZE_LIMIT_BYTES,
                    ),
                    job_type=operation.get(ATTR_NAME_JOB_TYPE),
                    trace_id=operation.get(ATTR_NAME_TRACE_ID) or str(uuid.uuid4()),
                ),
            )
        elif operation_type == _ATTR_OPERATION_TYPE_SNOWFLAKE_TEST:
            return None, None, None
        else:
            raise ValueError(f"Invalid operation type: {operation_type}")

    def _schedule_push_results_for_query(
        self,
        operation_id: str,
        query_id: str,
        operation_attrs: OperationAttributes,
    ):
        self._results_publisher.schedule_push_query_results(
            operation_id, query_id, operation_attrs
        )

    def _push_results_for_query(
        self, operation_id: str, query_id: str, operation_attrs: OperationAttributes
    ):
        """
        Invoked by results publisher to push results for a query
        """
        try:
            result = self._queries_service.result_for_query(query_id, operation_attrs)
            self._push_backend_results(operation_id, result, operation_attrs)
        except Exception as ex:
            logger.error(f"Failed to push results for query: {query_id}, error: {ex}")
