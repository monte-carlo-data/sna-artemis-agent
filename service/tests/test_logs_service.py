from unittest import TestCase
from unittest.mock import patch, Mock

from agent.sna.logs_service import LogsService
from agent.sna.sf_client import SnowflakeClient


class LogsServiceTests(TestCase):
    @patch.object(SnowflakeClient, "run_query_and_fetch_all")
    def test_fetch_logs(self, run_query_mock: Mock):
        run_query_mock.return_value = (
            [
                ("[2021-01-01 00:00:00] log line 1",),
                ("[2021-01-01 00:00:01] log line 2",),
            ],
            [
                ("log_line",),
            ],
        )

        logs = LogsService.get_logs(100)
        self.assertEqual(2, len(logs))
        self.assertEqual("2021-01-01 00:00:00", logs[0]["timestamp"])
        self.assertEqual("log line 1", logs[0]["message"])
        self.assertEqual("2021-01-01 00:00:01", logs[1]["timestamp"])
        self.assertEqual("log line 2", logs[1]["message"])
