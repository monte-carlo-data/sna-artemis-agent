import os
from typing import Optional

from agent.sna.config.config_persistence import ConfigurationPersistence

_SNA_ENV_PREFIX = "SNA"


class LocalConfig(ConfigurationPersistence):
    def get_value(self, key: str) -> Optional[str]:
        return os.getenv(f"{_SNA_ENV_PREFIX}_{key}")

    def set_value(self, key: str, value: str):
        raise NotImplementedError(
            "You cannot update config settings in a local environment, update env vars instead"
        )
