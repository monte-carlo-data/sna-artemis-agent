from unittest import TestCase
from unittest.mock import patch, create_autospec, Mock

from requests import Response, HTTPError

from agent.sna.metrics_service import MetricsService


class MetricsServiceTests(TestCase):
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
        lines = MetricsService.fetch_metrics()
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
        lines = MetricsService.fetch_metrics()
        self.assertEqual(3, len(lines))
        self.assertEqual("line3", lines[0])
        self.assertEqual("line5", lines[2])
