from typing import List, Dict, Any, Tuple

from agent.sna.queries_service import QueriesService


class LogsService:
    def __init__(self, queries_service: QueriesService):
        self._queries_service = queries_service

    def get_logs(self, limit: int) -> List[Dict[str, Any]]:
        logs, _ = self._queries_service.run_query_and_fetch_all(
            "CALL APP_PUBLIC.SERVICE_LOGS(?)", [limit]
        )
        return self._parse_logs(logs)

    @classmethod
    def _parse_logs(cls, logs: List[Tuple]) -> List[Dict[str, Any]]:
        return [
            cls._parse_log_line(log[0])
            for log in logs
            if len(log) >= 1 and isinstance(log[0], str)
        ]

    @staticmethod
    def _parse_log_line(log_line: str) -> Dict[str, Any]:
        if log_line.startswith("["):
            parts = log_line[1:].split("] ", 1)
            if len(parts) == 2:
                return {
                    "timestamp": parts[0],
                    "message": parts[1],
                }
        return {
            "timestamp": "",
            "message": log_line,
        }
