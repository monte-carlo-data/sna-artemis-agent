from unittest import TestCase
from unittest.mock import patch, Mock

from agent.sna.logs_service import LogsService


class LogsServiceTests(TestCase):
    def setUp(self):
        self._queries_service = Mock()
        self._service = LogsService(queries_service=self._queries_service)

    def test_fetch_logs(self):
        self._queries_service.run_query_and_fetch_all.return_value = (
            [
                ("[2021-01-01 00:00:00] log line 1",),
                ("[2021-01-01 00:00:01] log line 2",),
            ],
            [
                ("log_line",),
            ],
        )

        logs = self._service.get_logs(100)
        self.assertEqual(2, len(logs))
        self.assertEqual("2021-01-01 00:00:00", logs[0]["timestamp"])
        self.assertEqual("log line 1", logs[0]["message"])
        self.assertEqual("2021-01-01 00:00:01", logs[1]["timestamp"])
        self.assertEqual("log line 2", logs[1]["message"])
