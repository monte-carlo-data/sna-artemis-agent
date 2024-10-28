import logging
from threading import Condition, Thread
from typing import Callable, List

from agent.sna.sf_query import SnowflakeQuery
from agent.utils.queue_async_processor import QueueAsyncProcessor

logger = logging.getLogger(__name__)


class QueriesRunner(QueueAsyncProcessor[SnowflakeQuery]):
    def __init__(self, handler: Callable[[SnowflakeQuery], None]):
        self._queries_handler = handler
        super().__init__(name="QueriesRunner", handler=self._handler_wrapper)

    def _handler_wrapper(self, query: SnowflakeQuery):
        logger.info(f"Running operation: {query.operation_id}, query: {query.query}")
        self._queries_handler(query)
