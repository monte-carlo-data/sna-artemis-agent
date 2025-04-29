from unittest import TestCase
from unittest.mock import patch, Mock

from agent.sna.logs_service import LogsService


class LogsServiceTests(TestCase):
    def setUp(self):
        self._queries_service = Mock()
        self._service = LogsService(queries_service=self._queries_service)

    @patch("agent.sna.logs_service.LOCAL", False)
    def test_fetch_logs(self):
        self._queries_service.run_query_and_fetch_all.return_value = (
            [
                ("[2021-01-01 00:00:00] log line 1",),
                ("[2021-01-01 00:00:01] log line 2",),
                ("no ts line",),
                ("[2025-04-28 22:46:21 +0000] [7] [INFO] Booting worker with pid: 7",),
            ],
            [
                ("log_line",),
            ],
        )

        logs = self._service.get_logs(100)
        self.assertEqual(4, len(logs))
        self.assertEqual("2021-01-01 00:00:00", logs[0]["timestamp"])
        self.assertEqual("log line 1", logs[0]["message"])
        self.assertEqual("2021-01-01 00:00:01", logs[1]["timestamp"])
        self.assertEqual("log line 2", logs[1]["message"])
        self.assertEqual("", logs[2]["timestamp"])
        self.assertEqual("no ts line", logs[2]["message"])
        self.assertEqual("2025-04-28 22:46:21 +0000", logs[3]["timestamp"])
        self.assertEqual("[7] [INFO] Booting worker with pid: 7", logs[3]["message"])
