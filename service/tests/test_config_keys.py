import logging
from unittest import TestCase

from agent.sna.config.config_keys import resolve_log_level


class ResolveLogLevelTests(TestCase):
    def test_each_allowed_level_returns_logging_constant(self):
        cases = {
            "INFO": logging.INFO,
            "WARNING": logging.WARNING,
            "ERROR": logging.ERROR,
            "CRITICAL": logging.CRITICAL,
        }
        for level_str, expected in cases.items():
            with self.subTest(level=level_str):
                self.assertEqual(expected, resolve_log_level(level_str))

    def test_case_insensitive(self):
        self.assertEqual(logging.INFO, resolve_log_level("info"))
        self.assertEqual(logging.WARNING, resolve_log_level("Warning"))
        self.assertEqual(logging.ERROR, resolve_log_level("eRrOr"))

    def test_warn_aliases_warning(self):
        self.assertEqual(logging.WARNING, resolve_log_level("WARN"))
        self.assertEqual(logging.WARNING, resolve_log_level("warn"))

    def test_whitespace_tolerated(self):
        self.assertEqual(logging.ERROR, resolve_log_level("  ERROR  "))
        self.assertEqual(logging.INFO, resolve_log_level("\tINFO\n"))

    def test_debug_raises_with_security_framed_message(self):
        with self.assertRaises(ValueError) as ctx:
            resolve_log_level("DEBUG")
        msg = str(ctx.exception)
        # The error must surface the offending value AND explain why DEBUG
        # specifically is rejected — security framing is part of the contract.
        self.assertIn("DEBUG", msg)
        self.assertIn("third-party", msg)

    def test_unknown_level_raises(self):
        with self.assertRaises(ValueError) as ctx:
            resolve_log_level("BOGUS")
        self.assertIn("BOGUS", str(ctx.exception))

    def test_empty_string_raises(self):
        with self.assertRaises(ValueError):
            resolve_log_level("")

    def test_none_raises(self):
        with self.assertRaises(ValueError):
            resolve_log_level(None)  # type: ignore[arg-type]
