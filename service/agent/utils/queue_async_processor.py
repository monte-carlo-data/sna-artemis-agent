from threading import Condition, Thread
from typing import Callable, List, TypeVar, Generic

from agent.utils.utils import get_logger

logger = get_logger(__name__)

T = TypeVar("T")


class QueueAsyncProcessor(Generic[T]):
    def __init__(self, name: str, handler: Callable[[T], None]):
        self._name = name
        self._handler = handler

        self._condition = Condition()
        self._queue: List[T] = []
        self._running = True

    def start(self):
        th = Thread(target=self._run)
        th.start()

    def stop(self):
        with self._condition:
            self._running = False
            self._condition.notify()

    def schedule(self, o: T):
        with self._condition:
            self._queue.append(o)
            self._condition.notify()

    def _run(self):
        logger.info(f"{self._name} started")
        while self._running:
            with self._condition:
                while not self._queue and self._running:
                    self._condition.wait()
                if not self._running:
                    break
                to_execute = self._queue.copy()
                self._queue.clear()
            for o in to_execute:
                self._handler(o)
        logger.info(f"{self._name} thread stopped")
