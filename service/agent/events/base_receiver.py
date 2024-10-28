from abc import ABC, abstractmethod


class BaseReceiver(ABC):
    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def stop(self):
        pass