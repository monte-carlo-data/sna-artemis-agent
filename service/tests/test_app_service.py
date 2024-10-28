from unittest import TestCase
from unittest.mock import create_autospec, patch, ANY

from agent.events.events_client import EventsClient
from agent.events.receiver_factory import ReceiverFactory
from agent.sna.queries_runner import QueriesRunner
from agent.sna.results_publisher import ResultsPublisher
from agent.sna.sf_query import SnowflakeQuery
from agent.sna.sna_service import SnaService


_QUERY_OPERATION = {
    "operation_id": "1234",
    "operation": {
        "commands": [
            {
                "target": "_cursor",
                "method": "execute",
                "args": ["SELECT * FROM table"]
            },
        ]
    }
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
            agent_id="test-agent",
            handler=lambda x: None,
        )
        service = SnaService(
            queries_runner=self._mock_queries_runner,
            results_publisher=self._mock_results_publisher,
            events_client=events_client,
        )
        events_client._event_received(_QUERY_OPERATION)
        self._mock_queries_runner.schedule.assert_called_once_with(SnowflakeQuery(
            operation_id=ANY,
            query="SELECT * FROM table",
            timeout=ANY,
        ))

        service.query_completed("1234", "5678")
        self._mock_results_publisher.schedule_push_results.assert_called_once_with("1234", "5678")
