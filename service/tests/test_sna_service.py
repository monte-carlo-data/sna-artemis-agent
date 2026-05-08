import logging
from unittest import TestCase
from unittest.mock import Mock, patch

from apollo.egress.agent.config.config_manager import ConfigurationManager

from agent.sna.sna_service import SnaService


class BuildLogsServiceTests(TestCase):
    """Unit tests for SnaService._build_logs_service activation gate."""

    def setUp(self):
        self._config_manager = Mock(spec=ConfigurationManager)

    def test_enabled_by_default_returns_in_process_service(self):
        # Default for IN_PROCESS_LOGS_ENABLED is True; default for level is "INFO".
        # ConfigurationManager.get_bool_value(key, default) and .get_str_value(key, default)
        # return the default when the key is absent — emulate that.
        self._config_manager.get_bool_value.side_effect = lambda key, default: default
        self._config_manager.get_str_value.side_effect = lambda key, default: default

        sentinel = Mock(name="in_process_logs_service")
        with patch(
            "agent.sna.sna_service.setup_in_process_log_shipping",
            return_value=sentinel,
        ) as mock_setup:
            result = SnaService._build_logs_service(self._config_manager)

        self.assertIs(result, sentinel)
        mock_setup.assert_called_once_with(level=logging.INFO)

    def test_custom_level_passes_through(self):
        # Operator sets IN_PROCESS_LOGS_LEVEL=WARNING in CONFIG.APP_CONFIG —
        # ensures _build_logs_service actually reads the config value rather
        # than hardcoding the default.
        self._config_manager.get_bool_value.side_effect = lambda key, default: default
        self._config_manager.get_str_value.return_value = "WARNING"

        with patch(
            "agent.sna.sna_service.setup_in_process_log_shipping",
        ) as mock_setup:
            SnaService._build_logs_service(self._config_manager)

        mock_setup.assert_called_once_with(level=logging.WARNING)

    def test_explicit_disable_returns_none(self):
        # Operator sets IN_PROCESS_LOGS_ENABLED=False in CONFIG.APP_CONFIG.
        self._config_manager.get_bool_value.return_value = False

        with patch(
            "agent.sna.sna_service.setup_in_process_log_shipping",
        ) as mock_setup:
            result = SnaService._build_logs_service(self._config_manager)

        self.assertIsNone(result)
        mock_setup.assert_not_called()

    def test_invalid_log_level_raises(self):
        # DEBUG is rejected by the allowlist — would surface third-party-library
        # content (request bodies, tokens) in shipped logs.
        self._config_manager.get_bool_value.side_effect = lambda key, default: default
        self._config_manager.get_str_value.return_value = "DEBUG"

        with patch("agent.sna.sna_service.setup_in_process_log_shipping"):
            with self.assertRaises(ValueError) as ctx:
                SnaService._build_logs_service(self._config_manager)

        self.assertIn("DEBUG", str(ctx.exception))
