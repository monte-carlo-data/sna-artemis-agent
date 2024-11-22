from typing import Dict, Optional

from agent.sna.config.config_persistence import ConfigurationPersistence

CONFIG_USE_CONNECTION_POOL = "USE_CONNECTION_POOL"
CONFIG_CONNECTION_POOL_SIZE = "CONNECTION_POOL_SIZE"
CONFIG_QUERIES_RUNNER_THREAD_COUNT = "QUERIES_RUNNER_THREAD_COUNT"
CONFIG_OPS_RUNNER_THREAD_COUNT = "OPS_RUNNER_THREAD_COUNT"
CONFIG_PUBLISHER_THREAD_COUNT = "PUBLISHER_THREAD_COUNT"
CONFIG_USE_SYNC_QUERIES = "USE_SYNC_QUERIES"
CONFIG_STAGE_NAME = "STAGE_NAME"


class ConfigurationManager:
    def __init__(self, persistence: ConfigurationPersistence):
        self._persistence = persistence

    def get_str_value(self, key: str, default_value: str) -> str:
        return self._get_value(key) or default_value

    def get_int_value(self, key: str, default_value: int) -> int:
        if value := self._get_value(key):
            return int(value)
        else:
            return default_value

    def get_bool_value(self, key: str, default_value: bool) -> bool:
        if value := self._get_value(key):
            return value.lower() == "true"
        else:
            return default_value

    def _get_value(self, key: str) -> Optional[str]:
        return self._persistence.get_value(key)

    def set_values(self, values: Dict[str, str]):
        for key, value in values.items():
            self._persistence.set_value(key, value)
