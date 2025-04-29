import gzip
import json
from copy import deepcopy
from unittest import TestCase
from unittest.mock import create_autospec, patch, ANY, Mock

from agent.events.ack_sender import AckSender
from agent.events.base_receiver import BaseReceiver
from agent.events.events_client import EventsClient
from agent.events.heartbeat_checker import HeartbeatChecker
from agent.sna.config.config_manager import ConfigurationManager
from agent.sna.config.config_persistence import ConfigurationPersistence
from agent.sna.operation_result import OperationAttributes, AgentOperationResult
from agent.sna.operations_runner import OperationsRunner, Operation
from agent.sna.queries_runner import QueriesRunner
from agent.sna.queries_service import QueriesService
from agent.sna.results_publisher import ResultsPublisher
from agent.sna.sf_query import SnowflakeQuery
from agent.sna.sna_service import SnaService
from agent.sna.timer_service import TimerService
from agent.storage.storage_service import StorageService
from agent.utils.serde import (
    ATTRIBUTE_NAME_ERROR,
    ATTRIBUTE_NAME_ERROR_ATTRS,
    ATTRIBUTE_NAME_ERROR_TYPE,
    ATTRIBUTE_NAME_RESULT,
    ATTRIBUTE_NAME_TRACE_ID,
)
from agent.utils.utils import BACKEND_SERVICE_URL

_QUERY_OPERATION = {
    "operation_id": "1234",
    "operation": {
        "type": "snowflake_query",
        "query": "SELECT * FROM table",
        "trace_id": "5432",
        "compress_response_file": False,
        "response_size_limit_bytes": 100000,
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
        self._logs_sender = create_autospec(TimerService)
        self._service = SnaService(
            queries_runner=self._mock_queries_runner,
            ops_runner=self._mock_ops_runner,
            results_publisher=self._mock_results_publisher,
            events_client=self._mock_events_client,
            ack_sender=self._ack_sender,
            queries_service=self._queries_service,
            config_manager=self._config_manager,
            logs_sender=self._logs_sender,
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

    @patch("requests.put")
    def test_query_execution(self, mock_requests_put: Mock):
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
            logs_sender=self._logs_sender,
        )
        service.start()
        events_client._event_received(_QUERY_OPERATION)
        operation_attrs = OperationAttributes(
            operation_id="1234",
            trace_id="5432",
            compress_response_file=False,
            response_size_limit_bytes=100000,
        )
        self._mock_queries_runner.schedule.assert_called_once_with(
            SnowflakeQuery(
                operation_id="1234",
                query="SELECT * FROM table",
                timeout=ANY,
                operation_attrs=operation_attrs,
            )
        )

        service.query_completed(operation_attrs.to_json(), "5678")
        self._mock_results_publisher.schedule_push_query_results.assert_called_once_with(
            "1234", "5678", operation_attrs
        )
        # now simulate the publisher called its handler
        query_result = {
            ATTRIBUTE_NAME_RESULT: {
                "all_results": [],
                "description": [],
                "rowcount": 0,
            },
        }
        self._queries_service.result_for_query.return_value = query_result
        service._push_results_for_query("1234", "5678", operation_attrs)
        mock_requests_put.assert_called_once_with(
            ANY,
            data=ANY,
            headers={
                "Content-Type": "application/json",
                "x-mcd-id": "local-token-id",
                "x-mcd-token": "local-token-secret",
            },
        )
        url = mock_requests_put.call_args[0][0]
        self.assertTrue(url.endswith("/api/v1/agent/operations/1234/result"))
        sent_data = mock_requests_put.call_args[1]["data"]
        sent_dict = json.loads(sent_data)
        self.assertEqual({"result": query_result}, sent_dict)

        # test query failed flow
        service.query_failed(
            operation_attrs.to_json(), 1678, "error msg", "error state"
        )
        self._mock_results_publisher.schedule_push_results.assert_called_once_with(
            operation_id="1234",
            result={
                ATTRIBUTE_NAME_ERROR: "error msg",
                ATTRIBUTE_NAME_ERROR_ATTRS: {"errno": 1678, "sqlstate": "error state"},
                ATTRIBUTE_NAME_ERROR_TYPE: "DatabaseError",
            },
            operation_attrs=operation_attrs,
        )

        # test query failed with programming error
        self._mock_results_publisher.reset_mock()
        service.query_failed(
            operation_attrs.to_json(), 2043, "error msg", "error state"
        )
        self._mock_results_publisher.schedule_push_results.assert_called_once_with(
            operation_id="1234",
            result={
                ATTRIBUTE_NAME_ERROR: "error msg",
                ATTRIBUTE_NAME_ERROR_ATTRS: {"errno": 2043, "sqlstate": "error state"},
                ATTRIBUTE_NAME_ERROR_TYPE: "ProgrammingError",
            },
            operation_attrs=operation_attrs,
        )

    @patch("requests.put")
    def test_query_execution_pre_signed_url(self, mock_requests_put: Mock):
        events_client = EventsClient(
            receiver=create_autospec(BaseReceiver),
            heartbeat_checker=create_autospec(HeartbeatChecker),
        )
        storage = create_autospec(StorageService)
        service = SnaService(
            queries_runner=self._mock_queries_runner,
            ops_runner=self._mock_ops_runner,
            results_publisher=self._mock_results_publisher,
            events_client=events_client,
            ack_sender=self._ack_sender,
            queries_service=self._queries_service,
            config_manager=self._config_manager,
            storage_service=storage,
            logs_sender=self._logs_sender,
        )
        service.start()
        query_operation = deepcopy(_QUERY_OPERATION)
        query_operation["operation"]["response_size_limit_bytes"] = 1
        events_client._event_received(query_operation)
        operation_attrs = OperationAttributes(
            operation_id="1234",
            trace_id="5432",
            compress_response_file=False,
            response_size_limit_bytes=1,
        )
        self._mock_queries_runner.schedule.assert_called_once_with(
            SnowflakeQuery(
                operation_id="1234",
                query="SELECT * FROM table",
                timeout=ANY,
                operation_attrs=operation_attrs,
            )
        )

        service.query_completed(operation_attrs.to_json(), "5678")
        self._mock_results_publisher.schedule_push_query_results.assert_called_once_with(
            "1234", "5678", operation_attrs
        )
        large_result = {
            ATTRIBUTE_NAME_RESULT: {
                "big_result": True,
            }
        }
        storage.generate_presigned_url.return_value = "http://presigned.url"
        self._config_persistence.get_value.return_value = None
        service._push_results(
            AgentOperationResult(
                operation_id="1234",
                result=deepcopy(large_result),
                operation_attrs=operation_attrs,
            ),
        )
        storage.write.assert_called_once_with(
            key="responses/5432",
            obj_to_write=ANY,
        )
        saved_data = storage.write.call_args[1]["obj_to_write"]
        storage.generate_presigned_url.assert_called_once_with("responses/5432", 3600)
        self.assertEqual(
            {
                **large_result,
                ATTRIBUTE_NAME_TRACE_ID: "5432",
            },
            json.loads(saved_data),
        )
        mock_requests_put.assert_called_once()

        # test compression now
        storage.reset_mock()

        operation_attrs = OperationAttributes(
            operation_id="1234",
            trace_id="5432",
            compress_response_file=True,
            response_size_limit_bytes=1,
        )
        service._push_results(
            AgentOperationResult(
                operation_id="1234",
                result=deepcopy(large_result),
                operation_attrs=operation_attrs,
            ),
        )
        storage.write.assert_called_once_with(
            key="responses/5432",
            obj_to_write=ANY,
        )
        compressed_data = storage.write.call_args[1]["obj_to_write"]
        saved_dict = json.loads(gzip.decompress(compressed_data).decode())
        self.assertEqual(
            {
                **large_result,
                ATTRIBUTE_NAME_TRACE_ID: "5432",
            },
            saved_dict,
        )
        storage.generate_presigned_url.assert_called_once_with("responses/5432", 3600)

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
            logs_sender=self._logs_sender,
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
            operation_id="1234",
            result=health_info,
            operation_attrs=None,
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
