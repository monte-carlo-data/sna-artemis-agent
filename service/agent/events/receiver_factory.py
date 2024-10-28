from abc import ABC, abstractmethod
from typing import Dict, Callable

from agent.events.base_receiver import BaseReceiver


class ReceiverFactory(ABC):
    @abstractmethod
    def create_receiver(
        self, base_url: str, agent_id: str, handler: Callable[[Dict], None]
    ) -> BaseReceiver:
        pass
