"""SNA-local config keys backed by CONFIG.APP_CONFIG.

Apollo's `config_keys` module covers framework-level settings (thread
counts, intervals, flags). The keys here are SNA consumer policy — they
gate behaviors that only make sense when the SNA agent is the consumer
of agent-common's egress framework, so they live in this repo rather
than upstream.
"""

import logging

CONFIG_IN_PROCESS_LOGS_ENABLED = "IN_PROCESS_LOGS_ENABLED"
CONFIG_IN_PROCESS_LOGS_LEVEL = "IN_PROCESS_LOGS_LEVEL"

# DEBUG is intentionally excluded — third-party libraries log request
# bodies and tokens at DEBUG, which would surface in shipped logs.
_LOG_LEVEL_ALLOWLIST = {
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "WARN": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}


def resolve_log_level(level_str: str) -> int:
    """Map a configured log-level string to a `logging` level constant.

    Raises ValueError on anything outside the allowlist (notably DEBUG).
    """
    normalized = (level_str or "").strip().upper()
    if normalized not in _LOG_LEVEL_ALLOWLIST:
        raise ValueError(
            f"Invalid {CONFIG_IN_PROCESS_LOGS_LEVEL}={level_str!r}. "
            f"Allowed: {', '.join(sorted(_LOG_LEVEL_ALLOWLIST))}. "
            "DEBUG is excluded — it surfaces third-party-library content "
            "(request bodies, tokens) into shipped logs."
        )
    return _LOG_LEVEL_ALLOWLIST[normalized]
