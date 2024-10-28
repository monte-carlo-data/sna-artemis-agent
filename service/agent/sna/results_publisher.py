from threading import Condition, Thread
from typing import List, Tuple, Callable

from agent.utils.queue_async_processor import QueueAsyncProcessor
from agent.utils.utils import get_logger

logger = get_logger(__name__)


class ResultsPublisher(QueueAsyncProcessor[Tuple[str, str]]):
    def __init__(self, handler: Callable[[str, str], None]):
        self._results_handler = handler
        super().__init__(name="ResultsPublisher", handler=self._handler_wrapper)

    def schedule_push_results(self, operation_id: str, query_id: str):
        self.schedule((operation_id, query_id))

    def _handler_wrapper(self, args: Tuple[str, str]):
        operation_id, query_id = args
        logger.info(f"Running results push: {operation_id}, query_id: {query_id}")
        self._results_handler(operation_id, query_id)

