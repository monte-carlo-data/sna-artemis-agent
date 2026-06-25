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

    def test_invalid_log_level_falls_back_to_info(self):
        # A misconfigured level must not crash-loop the agent. Both a rejected
        # DEBUG (security control) and a plain typo fall back to INFO — a safe
        # level that still never ships DEBUG content — with a loud warning.
        self._config_manager.get_bool_value.side_effect = lambda key, default: default

        for bad_level in ("DEBUG", "INFOO"):
            with self.subTest(level=bad_level):
                self._config_manager.get_str_value.return_value = bad_level
                with patch(
                    "agent.sna.sna_service.setup_in_process_log_shipping",
                ) as mock_setup:
                    with self.assertLogs(
                        "agent.sna.sna_service", level=logging.WARNING
                    ) as logs:
                        SnaService._build_logs_service(self._config_manager)

                mock_setup.assert_called_once_with(level=logging.INFO)
                self.assertTrue(
                    any("falling back to INFO" in line for line in logs.output)
                )
