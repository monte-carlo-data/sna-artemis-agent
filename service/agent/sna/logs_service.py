from typing import List, Dict, Any, Tuple

from agent.sna.sf_client import SnowflakeClient


class LogsService:
    @classmethod
    def get_logs(cls, limit: int) -> List[Dict[str, Any]]:
        logs, _ = SnowflakeClient.run_query_and_fetch_all(
            "CALL app_public.service_logs(?)", [limit]
        )
        return cls._parse_logs(logs)

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
