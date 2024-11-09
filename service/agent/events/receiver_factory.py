from abc import ABC, abstractmethod
from typing import Dict, Callable

from agent.events.base_receiver import BaseReceiver


class ReceiverFactory(ABC):
    """
    Base factory class used to create receiver objects.
    """

    @abstractmethod
    def create_receiver(
        self, base_url: str, handler: Callable[[Dict], None]
    ) -> BaseReceiver:
        pass
