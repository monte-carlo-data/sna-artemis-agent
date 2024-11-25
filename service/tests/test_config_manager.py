from contextlib import closing
from unittest import TestCase
from unittest.mock import create_autospec, patch, Mock, call

from agent.sna.config.config_manager import ConfigurationManager
from agent.sna.config.config_persistence import ConfigurationPersistence
from agent.sna.config.db_config import DbConfig

_EXPECTED_UPDATE_QUERY = """
MERGE INTO MCD_AGENT.CONFIG.APP_CONFIG C
USING (SELECT ? AS key) S
ON S.key=C.key 
WHEN MATCHED THEN UPDATE SET value=? 
WHEN NOT MATCHED THEN INSERT (key, value) VALUES (?, ?)
"""


class ConfigManagerTests(TestCase):
    def setUp(self):
        self._persistence = create_autospec(ConfigurationPersistence)
        self._config_manager = ConfigurationManager(persistence=self._persistence)

    def test_type_conversion(self):
        self._persistence.get_value.return_value = "true"
        self.assertTrue(self._config_manager.get_bool_value("key", False))
        self._persistence.get_value.return_value = "false"
        self.assertFalse(self._config_manager.get_bool_value("key", True))
        self._persistence.get_value.return_value = "123"
        self.assertEqual(123, self._config_manager.get_int_value("key", 0))
        self._persistence.get_value.return_value = "test"
        self.assertEqual("test", self._config_manager.get_str_value("key", "default"))

    def test_default_values(self):
        self._persistence.get_value.return_value = None
        self.assertEqual(
            "default", self._config_manager.get_str_value("key", "default")
        )
        self.assertEqual(123, self._config_manager.get_int_value("key", 123))
        self.assertTrue(self._config_manager.get_bool_value("key", True))

    def test_set_values(self):
        self._config_manager.set_values(
            {
                "key1": "value1",
                "key2": "value2",
            }
        )
        self._persistence.set_value.assert_has_calls(
            [
                call("key1", "value1"),
                call("key2", "value2"),
            ]
        )

    @patch("agent.sna.config.db_config.create_connection")
    def test_db_persistence(self, mock_create_connection: Mock):
        data = [
            ("key1", "value1"),
            ("key2", "value2"),
        ]
        mock_cursor = Mock()
        mock_cursor.__iter__ = Mock(return_value=iter(data))
        mock_connection = Mock()
        mock_connection.cursor.return_value = closing(mock_cursor)  # type: ignore
        mock_create_connection.return_value = closing(mock_connection)  # type: ignore

        persistence = DbConfig()
        mock_cursor.execute.assert_called_once_with(
            "SELECT key, value FROM MCD_AGENT.CONFIG.APP_CONFIG"
        )
        self.assertIsNone(persistence.get_value("key"))
        self.assertEqual("value1", persistence.get_value("key1"))
        self.assertEqual("value2", persistence.get_value("key2"))

        mock_cursor.reset_mock()
        persistence.set_value("key", "value")
        mock_cursor.execute.assert_has_calls(
            [
                call(_EXPECTED_UPDATE_QUERY, ("key", "value", "key", "value")),
                call("SELECT key, value FROM MCD_AGENT.CONFIG.APP_CONFIG"),
            ]
        )
