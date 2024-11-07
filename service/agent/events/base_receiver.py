from abc import ABC, abstractmethod


class BaseReceiver(ABC):
    """
    BaseReceiver class, receivers are used to produce events received from the backend.
    """

    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def stop(self):
        pass
