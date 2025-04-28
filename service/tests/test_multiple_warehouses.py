from contextlib import closing
from typing import Optional, Tuple
from unittest import TestCase
from unittest.mock import create_autospec, patch, Mock, ANY, call

from sqlalchemy import QueuePool

from agent.events.ack_sender import AckSender
from agent.events.base_receiver import BaseReceiver
from agent.events.events_client import EventsClient
from agent.events.heartbeat_checker import HeartbeatChecker
from agent.sna.config.config_keys import CONFIG_USE_CONNECTION_POOL, CONFIG_JOB_TYPES
from agent.sna.config.config_manager import ConfigurationManager
from agent.sna.config.config_persistence import ConfigurationPersistence
from agent.sna.operation_result import OperationAttributes
from agent.sna.operations_runner import OperationsRunner
from agent.sna.queries_runner import QueriesRunner
from agent.sna.queries_service import (
    QueriesService,
    JobTypesConfiguration,
    JobTypeConfiguration,
)
from agent.sna.results_publisher import ResultsPublisher
from agent.sna.sf_query import SnowflakeQuery
from agent.sna.sna_service import SnaService
from agent.sna.timer_service import TimerService

_QUERY_LOGS_JOB_TYPES_CONFIG = JobTypesConfiguration(
    job_types=[
        JobTypeConfiguration(
            job_type="query_logs",
            warehouse_name="QUERY_LOGS_WH",
            pool_size=1,
        ),
        JobTypeConfiguration(
            job_type="sql_query",
            warehouse_name="SQL_QUERY_WH",
            pool_size=2,
        ),
    ],
)


class MultiWarehouseTests(TestCase):
    def setUp(self):
        self._mock_queries_runner = create_autospec(QueriesRunner)
        self._mock_ops_runner = create_autospec(OperationsRunner)
        self._mock_results_publisher = create_autospec(ResultsPublisher)
        self._ack_sender = create_autospec(AckSender)
        self._logs_sender = create_autospec(TimerService)
        self._config_persistence = create_autospec(ConfigurationPersistence)
        self._config_manager = ConfigurationManager(
            persistence=self._config_persistence
        )
        self._events_client = EventsClient(
            receiver=create_autospec(BaseReceiver),
            heartbeat_checker=create_autospec(HeartbeatChecker),
        )

    @patch.object(QueriesService, "_create_connection_pool")
    def test_query_logs_query_execution(self, mock_create_connection_pool: Mock):
        def get_config_value(key: str) -> Optional[str]:
            if key == CONFIG_USE_CONNECTION_POOL:
                return "true"
            elif key == CONFIG_JOB_TYPES:
                return _QUERY_LOGS_JOB_TYPES_CONFIG.to_json()
            return None

        self._config_persistence.get_value.side_effect = get_config_value

        mock_default_pool, mock_default_cursor = self._create_mock_pool()
        mock_ql_pool, mock_ql_cursor = self._create_mock_pool()
        mock_sq_pool, mock_sq_cursor = self._create_mock_pool()

        def create_connection_pool(pool_size: int, warehouse_name: str):
            if warehouse_name == "QUERY_LOGS_WH":
                return mock_ql_pool
            elif warehouse_name == "SQL_QUERY_WH":
                return mock_sq_pool
            return mock_default_pool

        mock_create_connection_pool.side_effect = create_connection_pool

        queries_service = QueriesService(
            config_manager=self._config_manager,
        )
        service = SnaService(
            queries_runner=self._mock_queries_runner,
            ops_runner=self._mock_ops_runner,
            results_publisher=self._mock_results_publisher,
            events_client=self._events_client,
            ack_sender=self._ack_sender,
            queries_service=queries_service,
            config_manager=self._config_manager,
            logs_sender=self._logs_sender,
        )
        service.start()

        mock_create_connection_pool.assert_has_calls(
            [
                call(pool_size=3, warehouse_name="MCD_AGENT_WH"),
                call(pool_size=1, warehouse_name="QUERY_LOGS_WH"),
                call(pool_size=2, warehouse_name="SQL_QUERY_WH"),
            ]
        )

        # run a query logs query and confirm it executes the query in the query logs pool
        self._run_job_type_query(
            service, "SELECT * FROM table", "query_logs", mock_ql_cursor
        )

        # now run a metadata query and assert it uses the default pool
        self._run_job_type_query(
            service, "SELECT * FROM table", "metadata", mock_default_cursor
        )

        # now run a sql-query query and assert it uses the default pool
        self._run_job_type_query(
            service, "SELECT * FROM table", "sql_query", mock_sq_cursor
        )

        # now run a query with no job type and assert it uses the default pool
        self._run_job_type_query(
            service, "SELECT * FROM table", None, mock_default_cursor
        )

    @staticmethod
    def _create_mock_pool() -> Tuple[Mock, Mock]:
        mock_pool = create_autospec(QueuePool)
        mock_cursor = Mock()
        mock_cursor.__iter__ = Mock(return_value=iter([]))
        mock_connection = Mock()
        mock_connection.cursor.return_value = closing(mock_cursor)  # type: ignore
        mock_pool.connect.return_value = mock_connection

        return mock_pool, mock_cursor

    def _run_job_type_query(
        self, service: SnaService, query: str, job_type: Optional[str], cursor: Mock
    ):
        cursor.reset_mock()
        service._run_query(
            SnowflakeQuery(
                operation_id="1234",
                query=query,
                timeout=ANY,
                operation_attrs=OperationAttributes(
                    operation_id="1234",
                    trace_id="5432",
                    compress_response_file=False,
                    response_size_limit_bytes=100000,
                    job_type=job_type,
                ),
            )
        )

        # assert the query was executed in the query logs pool
        cursor.execute.assert_called_once_with("SELECT * FROM table")
