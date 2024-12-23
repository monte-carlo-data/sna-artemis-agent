import logging
import os
from typing import Optional, Dict

from agent.sna.config.config_persistence import ConfigurationPersistence
from agent.sna.sf_connection import create_connection
from agent.sna.sf_queries import QUERY_LOAD_CONFIG, QUERY_UPDATE_CONFIG
from agent.utils.utils import get_application_name

_CONFIG_TABLE_NAME = os.getenv("CONFIG_TABLE_NAME", "CONFIG.APP_CONFIG")

logger = logging.getLogger(__name__)


class DbConfig(ConfigurationPersistence):
    """
    Loads/stores configuration settings from/to the CONFIG.APP_CONFIG table in the app database.
    """

    def __init__(self):
        self._values = self._load_values_from_db()
        logger.info(f"Loaded configuration from DB: {self._values}")

    def get_value(self, key: str) -> Optional[str]:
        return self._values.get(key)

    def set_value(self, key: str, value: str):
        query = QUERY_UPDATE_CONFIG.format(table=_CONFIG_TABLE_NAME)
        with create_connection(self._get_config_warehouse_name()) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (key, value, key, value))
                conn.commit()
        self._values = self._load_values_from_db()

    def get_all_values(self) -> Dict[str, str]:
        return self._values

    @classmethod
    def _load_values_from_db(cls):
        logger.info(f"Loading configuration from DB table: {_CONFIG_TABLE_NAME}")
        with create_connection(cls._get_config_warehouse_name()) as conn:
            with conn.cursor() as cursor:
                cursor.execute(QUERY_LOAD_CONFIG.format(table=_CONFIG_TABLE_NAME))
                return {key: value for key, value in cursor}

    @staticmethod
    def _get_config_warehouse_name() -> str:
        return os.getenv("SNA_WAREHOUSE_NAME", f"{get_application_name()}_WH")
