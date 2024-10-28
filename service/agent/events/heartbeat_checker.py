import datetime
import logging
from threading import Thread, Condition
from typing import Callable

logger = logging.getLogger(__name__)


class HeartbeatChecker:
    def __init__(
        self,
        heartbeat_missing_handler: Callable[[], None],
        inactivity_timeout_seconds: int = 120,
    ):
        self._inactivity_timeout_seconds = inactivity_timeout_seconds
        self._handler = heartbeat_missing_handler
        self._condition = Condition()
        self._last_heartbeat = datetime.datetime.now()
        self._stopped = False

    def start(self):
        th = Thread(target=self._run_heartbeat_checker)
        th.start()

    def stop(self):
        self._stopped = True
        self.heartbeat_received()  # wake up the thread to stop running

    def heartbeat_received(self):
        with self._condition:
            self._last_heartbeat = datetime.datetime.now()
            self._condition.notify()

    def _run_heartbeat_checker(self):
        logger.info("Heartbeat monitor started")
        while not self._stopped:
            with self._condition:
                self._condition.wait(timeout=self._inactivity_timeout_seconds / 2)
                elapsed_time = datetime.datetime.now() - self._last_heartbeat
            if self._stopped:
                break
            if elapsed_time.total_seconds() > self._inactivity_timeout_seconds:
                logger.error("Heartbeat timeout")
                self._handler()
        logger.info("Heartbeat monitor stopped")
