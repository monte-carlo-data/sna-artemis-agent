from unittest import TestCase
from unittest.mock import patch, create_autospec, Mock

from apollo.egress.agent.backend.backend_client import BackendClient
from apollo.egress.agent.config.config_manager import ConfigurationManager
from apollo.egress.agent.config.local_config import LocalConfig
from apollo.egress.agent.events.ack_sender import AckSender
from apollo.egress.agent.events.base_receiver import BaseReceiver
from apollo.egress.agent.events.events_client import EventsClient
from apollo.egress.agent.events.heartbeat_checker import HeartbeatChecker
from apollo.egress.agent.service.operations_runner import OperationsRunner, Operation
from apollo.egress.agent.service.results_publisher import ResultsPublisher
from apollo.egress.agent.service.timer_service import TimerService
from requests import Response, HTTPError

from agent.sna.metrics_service import MetricsService, SnowparkMetricsService
from agent.sna.queries_runner import QueriesRunner
from agent.sna.sna_service import SnaService

_REQUEST_METRICS_OPERATION = {
    "type": "push_metrics",
}


class MetricsServiceTests(TestCase):
    def setUp(self):
        self._events_client = create_autospec(EventsClient)
        self._mock_queries_runner = create_autospec(QueriesRunner)
        self._mock_ops_runner = Mock()
        self._mock_ops_runner.queue_depth.return_value = 0
        self._mock_ops_runner.thread_count = 1
        self._mock_results_publisher = create_autospec(ResultsPublisher)
        self._config_manager = ConfigurationManager(
            persistence=LocalConfig(prefix="SNA")
        )
        self._service = SnaService(
            queries_runner=self._mock_queries_runner,
            ops_runner=self._mock_ops_runner,
            results_publisher=self._mock_results_publisher,
            events_client=self._events_client,
            config_manager=self._config_manager,
            ack_sender=create_autospec(AckSender),
            logs_sender=create_autospec(TimerService),
        )
        self._service.start()

    @patch("socket.getaddrinfo")
    @patch("requests.get")
    def test_collect_metrics(self, mock_get: Mock, mock_getaddrinfo: Mock):
        mock_getaddrinfo.return_value = [
            (0, 0, 0, "", ("1.2.3.4", 0)),
            (0, 0, 0, "", ("1.2.3.4", 0)),
            (0, 0, 0, "", ("5.6.7.8", 0)),
        ]
        mock_get.side_effect = [
            create_autospec(Response, text="line1\nline2\n"),
            create_autospec(Response, text="line3\nline4\nline5\n"),
        ]
        lines = SnowparkMetricsService.fetch_metrics()
        self.assertEqual(5, len(lines))
        self.assertEqual("line1", lines[0])
        self.assertEqual("line5", lines[4])

    @patch("socket.getaddrinfo")
    @patch("requests.get")
    def test_collect_metrics_failure(self, mock_get: Mock, mock_getaddrinfo: Mock):
        mock_getaddrinfo.return_value = [
            (0, 0, 0, "", ("1.2.3.4", 0)),
            (0, 0, 0, "", ("1.2.3.4", 0)),
            (0, 0, 0, "", ("5.6.7.8", 0)),
        ]
        response_1 = create_autospec(Response, text="line1\nline2\n")
        response_1.raise_for_status.side_effect = HTTPError(
            "url", 500, "msg", None, None
        )
        mock_get.side_effect = [
            response_1,
            create_autospec(Response, text="line3\nline4\nline5\n"),
        ]
        lines = SnowparkMetricsService.fetch_metrics()
        self.assertEqual(3, len(lines))
        self.assertEqual("line3", lines[0])
        self.assertEqual("line5", lines[2])

    @patch.object(MetricsService, "fetch_metrics")
    @patch.object(BackendClient, "execute_operation")
    def test_metrics_push(self, mock_execute_operation: Mock, mock_fetch_metrics: Mock):
        # Trigger metrics push directly (simulates MetricsTimer triggering)
        self._service._push_metrics()
        operation = Operation(
            operation_id="push_metrics",
            event={
                "path": "push_metrics",
            },
        )

        self._mock_ops_runner.schedule.assert_called_once_with(operation)

        # now simulate the operations runner executed the operation
        mock_fetch_metrics.return_value = ["line1", "line2"]
        self._service._execute_scheduled_operation(operation)
        mock_fetch_metrics.assert_called_once()
        mock_execute_operation.assert_called_once_with(
            "/api/v1/agent/metrics",
            "POST",
            {"format": "prometheus", "metrics": ["line1", "line2"]},
        )
