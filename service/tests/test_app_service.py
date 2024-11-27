from unittest import TestCase
from unittest.mock import create_autospec, patch, ANY

from agent.events.ack_sender import AckSender
from agent.events.base_receiver import BaseReceiver
from agent.events.events_client import EventsClient
from agent.events.heartbeat_checker import HeartbeatChecker
from agent.sna.config.config_manager import ConfigurationManager
from agent.sna.config.config_persistence import ConfigurationPersistence
from agent.sna.config.local_config import LocalConfig
from agent.sna.operations_runner import OperationsRunner, Operation
from agent.sna.queries_runner import QueriesRunner
from agent.sna.queries_service import QueriesService
from agent.sna.results_publisher import ResultsPublisher
from agent.sna.sf_query import SnowflakeQuery
from agent.sna.sna_service import SnaService
from agent.utils.serde import (
    ATTRIBUTE_NAME_ERROR,
    ATTRIBUTE_NAME_ERROR_ATTRS,
    ATTRIBUTE_NAME_ERROR_TYPE,
)
from agent.utils.utils import BACKEND_SERVICE_URL

_QUERY_OPERATION = {
    "operation_id": "1234",
    "operation": {
        "type": "snowflake_query",
        "query": "SELECT * FROM table",
    },
    "path": "/api/v1/agent/execute/snowflake/query",
}

_HEALTH_OPERATION = {
    "operation_id": "1234",
    "operation": {
        "trace_id": "5432",
    },
    "path": "/api/v1/test/health",
}


class AppServiceTests(TestCase):
    def setUp(self):
        self._mock_events_client = create_autospec(EventsClient)
        self._mock_queries_runner = create_autospec(QueriesRunner)
        self._mock_ops_runner = create_autospec(OperationsRunner)
        self._mock_results_publisher = create_autospec(ResultsPublisher)
        self._ack_sender = create_autospec(AckSender)
        self._queries_service = create_autospec(QueriesService)
        self._config_persistence = create_autospec(ConfigurationPersistence)
        self._config_manager = ConfigurationManager(
            persistence=self._config_persistence
        )
        self._service = SnaService(
            queries_runner=self._mock_queries_runner,
            ops_runner=self._mock_ops_runner,
            results_publisher=self._mock_results_publisher,
            events_client=self._mock_events_client,
            ack_sender=self._ack_sender,
            queries_service=self._queries_service,
            config_manager=self._config_manager,
        )

    def test_service_start_stop(self):
        self._service.start()
        self._mock_queries_runner.start.assert_called_once()
        self._mock_results_publisher.start.assert_called_once()
        self._mock_events_client.start.assert_called_once()

        self._service.stop()
        self._mock_queries_runner.stop.assert_called_once()
        self._mock_results_publisher.stop.assert_called_once()
        self._mock_events_client.stop.assert_called_once()

    def test_query_execution(self):
        events_client = EventsClient(
            receiver=create_autospec(BaseReceiver),
            heartbeat_checker=create_autospec(HeartbeatChecker),
        )
        service = SnaService(
            queries_runner=self._mock_queries_runner,
            ops_runner=self._mock_ops_runner,
            results_publisher=self._mock_results_publisher,
            events_client=events_client,
            ack_sender=self._ack_sender,
            queries_service=self._queries_service,
            config_manager=self._config_manager,
        )
        service.start()
        events_client._event_received(_QUERY_OPERATION)
        self._mock_queries_runner.schedule.assert_called_once_with(
            SnowflakeQuery(
                operation_id=ANY,
                query="SELECT * FROM table",
                timeout=ANY,
            )
        )

        service.query_completed("1234", "5678")
        self._mock_results_publisher.schedule_push_query_results.assert_called_once_with(
            "1234", "5678"
        )

        # test query failed flow
        service.query_failed("1234", 1678, "error msg", "error state")
        self._mock_results_publisher.schedule_push_results.assert_called_once_with(
            "1234",
            {
                ATTRIBUTE_NAME_ERROR: "error msg",
                ATTRIBUTE_NAME_ERROR_ATTRS: {"errno": 1678, "sqlstate": "error state"},
                ATTRIBUTE_NAME_ERROR_TYPE: "DatabaseError",
            },
        )

        # test query failed with programming error
        self._mock_results_publisher.reset_mock()
        service.query_failed("1234", 2043, "error msg", "error state")
        self._mock_results_publisher.schedule_push_results.assert_called_once_with(
            "1234",
            {
                ATTRIBUTE_NAME_ERROR: "error msg",
                ATTRIBUTE_NAME_ERROR_ATTRS: {"errno": 2043, "sqlstate": "error state"},
                ATTRIBUTE_NAME_ERROR_TYPE: "ProgrammingError",
            },
        )

    def test_health_operation(self):
        events_client = EventsClient(
            receiver=create_autospec(BaseReceiver),
            heartbeat_checker=create_autospec(HeartbeatChecker),
        )
        self._config_persistence.get_all_values.return_value = {
            "setting_1": "value_1",
            "setting_2": "value_2",
        }
        service = SnaService(
            queries_runner=self._mock_queries_runner,
            ops_runner=self._mock_ops_runner,
            results_publisher=self._mock_results_publisher,
            events_client=events_client,
            ack_sender=self._ack_sender,
            queries_service=self._queries_service,
            config_manager=self._config_manager,
        )
        service.start()
        events_client._event_received(_HEALTH_OPERATION)
        operation = Operation(
            operation_id="1234",
            event=_HEALTH_OPERATION,
        )
        self._mock_ops_runner.schedule.assert_called_once_with(operation)

        # now simulate the operations runner executed the operation
        self._service._execute_scheduled_operation(operation)
        health_info = service.health_information(
            _HEALTH_OPERATION["operation"]["trace_id"]
        )
        self.assertEqual(
            health_info["parameters"], {"setting_1": "value_1", "setting_2": "value_2"}
        )
        self._mock_results_publisher.schedule_push_results.assert_called_once_with(
            "1234",
            health_info,
        )

    def test_reachability_test(self):
        health_response = {"status": "ok"}
        with patch("requests.request") as mock_request:
            mock_request.return_value.status_code = 200
            mock_request.return_value.json.return_value = health_response
            result = self._service.run_reachability_test(trace_id="1234")
            mock_request.assert_called_once_with(
                method="GET",
                url=BACKEND_SERVICE_URL + "/api/v1/test/ping?trace_id=1234",
                json=None,
                headers={
                    "x-mcd-id": "local-token-id",
                    "x-mcd-token": "local-token-secret",
                },
            )
            self.assertEqual(health_response, result)

    def test_reachability_test_failed(self):
        with patch("requests.request") as mock_request:
            mock_request.return_value.raise_for_status.side_effect = Exception(
                "ping failed"
            )
            result = self._service.run_reachability_test()
            self.assertEqual({"error": "ping failed"}, result)
