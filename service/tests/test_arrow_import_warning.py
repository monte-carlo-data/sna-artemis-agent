import logging
from unittest import TestCase

from agent.utils.utils import silence_arrow_import_warning

_LOGGER_NAME = "snowflake.connector.cursor"

_ARROW_WARNING = (
    "Failed to import ArrowResult. No Apache Arrow result set format can be used. "
    "ImportError: Error loading shared library libstdc++.so.6: No such file or directory"
)


class SilenceArrowImportWarningTests(TestCase):
    def setUp(self):
        self._logger = logging.getLogger(_LOGGER_NAME)
        self._original_filters = list(self._logger.filters)
        silence_arrow_import_warning()

    def tearDown(self):
        self._logger.filters = self._original_filters

    def _emitted(self, message: str) -> bool:
        record = logging.LogRecord(
            _LOGGER_NAME, logging.WARNING, __file__, 0, message, (), None
        )
        return bool(self._logger.filter(record))

    def test_arrow_import_warning_is_dropped(self):
        self.assertFalse(self._emitted(_ARROW_WARNING))

    def test_other_records_are_kept(self):
        self.assertTrue(self._emitted("Query execution failed"))
