import logging
from typing import Callable

from agent.sna.sf_query import SnowflakeQuery
from agent.utils.queue_async_processor import QueueAsyncProcessor

logger = logging.getLogger(__name__)


class QueriesRunner(QueueAsyncProcessor[SnowflakeQuery]):
    """
    This class is responsible for processing queries to be executed.
    Currently, it uses a queue and the given number of threads to execute them.
    The handler is used to execute the query.
    """

    def __init__(
        self, handler: Callable[[SnowflakeQuery], None], thread_count: int = 1
    ):
        self._queries_handler = handler
        super().__init__(
            name="QueriesRunner",
            handler=self._handler_wrapper,
            thread_count=thread_count,
        )

    def _handler_wrapper(self, query: SnowflakeQuery):
        logger.info(
            f"Running operation: {query.operation_id}, query: {query.query[:500]}"
        )
        self._queries_handler(query)
