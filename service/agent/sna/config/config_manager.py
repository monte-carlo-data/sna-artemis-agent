import os
from typing import Dict, Optional

from agent.sna.config.db_config import DbConfig
from agent.sna.config.local_config import LocalConfig
from agent.utils.utils import LOCAL

_USE_DB_PERSISTENCE = (
    os.getenv("USE_DB_PERSISTENCE", "false" if LOCAL else "true").lower() == "true"
)
_config_persistence = DbConfig() if _USE_DB_PERSISTENCE else LocalConfig()

CONFIG_USE_CONNECTION_POOL = "USE_CONNECTION_POOL"
CONFIG_CONNECTION_POOL_SIZE = "CONNECTION_POOL_SIZE"
CONFIG_QUERIES_RUNNER_THREAD_COUNT = "QUERIES_RUNNER_THREAD_COUNT"
CONFIG_OPS_RUNNER_THREAD_COUNT = "OPS_RUNNER_THREAD_COUNT"
CONFIG_PUBLISHER_THREAD_COUNT = "PUBLISHER_THREAD_COUNT"
CONFIG_USE_SYNC_QUERIES = "USE_SYNC_QUERIES"


class ConfigurationManager:
    @classmethod
    def get_str_value(cls, key: str, default_value: str) -> str:
        return cls._get_value(key) or default_value

    @classmethod
    def get_int_value(cls, key: str, default_value: int) -> int:
        if value := cls._get_value(key):
            return int(value)
        else:
            return default_value

    @classmethod
    def get_bool_value(cls, key: str, default_value: bool) -> bool:
        if value := cls._get_value(key):
            return value.lower() == "true"
        else:
            return default_value

    @staticmethod
    def _get_value(key: str) -> Optional[str]:
        return _config_persistence.get_value(key)

    @staticmethod
    def set_values(values: Dict[str, str]):
        for key, value in values.items():
            _config_persistence.set_value(key, value)
