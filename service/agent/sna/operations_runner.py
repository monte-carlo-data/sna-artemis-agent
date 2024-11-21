import logging
from dataclasses import dataclass
from typing import Callable, Dict, Any

from agent.sna.sf_query import SnowflakeQuery
from agent.utils.queue_async_processor import QueueAsyncProcessor

logger = logging.getLogger(__name__)


@dataclass
class Operation:
    operation_id: str
    event: Dict[str, Any]


class OperationsRunner(QueueAsyncProcessor[Operation]):
    """
    This class is responsible for processing other operations (not queries) to be executed,
    like fetch_logs, fetch_metrics, etc.
    Currently, it uses a queue and a single thread to execute them.
    The handler is used to execute the operation.
    """

    def __init__(self, handler: Callable[[Operation], None]):
        self._ops_handler = handler
        super().__init__(name="OperationsRunner", handler=self._handler_wrapper)

    def _handler_wrapper(self, operation: Operation):
        logger.info(f"Running operation: {operation.operation_id}")
        self._ops_handler(operation)
