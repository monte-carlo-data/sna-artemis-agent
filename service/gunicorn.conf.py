"""Gunicorn configuration for sna-artemis-agent.

The post_worker_init hook registers signal handlers after gunicorn has
finished initializing the worker (including setting up its own signal
handlers). Without this, gunicorn's init_signals() overrides the
application's handlers, and graceful shutdown logic (notify orchestrator,
stop threads) won't run on workers that receive SIGTERM.
"""


def post_worker_init(worker: object) -> None:
    from agent.main import service

    service.register_signal_handlers()
