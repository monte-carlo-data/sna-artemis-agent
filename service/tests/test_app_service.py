from unittest import TestCase
from unittest.mock import create_autospec, patch, ANY

from agent.events.events_client import EventsClient
from agent.events.receiver_factory import ReceiverFactory
from agent.sna.queries_runner import QueriesRunner
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
        self._mock_receiver_factory = create_autospec(ReceiverFactory)
        self._mock_events_client = create_autospec(EventsClient)
        self._mock_queries_runner = create_autospec(QueriesRunner)
        self._mock_results_publisher = create_autospec(ResultsPublisher)
        self._service = SnaService(
            queries_runner=self._mock_queries_runner,
            results_publisher=self._mock_results_publisher,
            events_client=self._mock_events_client,
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
            receiver_factory=self._mock_receiver_factory,
            base_url="http://localhost",
            handler=lambda x: None,
        )
        service = SnaService(
            queries_runner=self._mock_queries_runner,
            results_publisher=self._mock_results_publisher,
            events_client=events_client,
        )
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
            receiver_factory=self._mock_receiver_factory,
            base_url="http://localhost",
            handler=lambda x: None,
        )
        service = SnaService(
            queries_runner=self._mock_queries_runner,
            results_publisher=self._mock_results_publisher,
            events_client=events_client,
        )
        events_client._event_received(_HEALTH_OPERATION)
        self._mock_results_publisher.schedule_push_results.assert_called_once_with(
            "1234",
            service.health_information(_HEALTH_OPERATION["operation"]["trace_id"]),
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
                    "x-mcd-id": "no-token-id",
                    "x-mcd-token": "no-token-secret",
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
