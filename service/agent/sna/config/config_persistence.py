from abc import ABC, abstractmethod
from typing import Optional


class ConfigurationPersistence(ABC):
    @abstractmethod
    def get_value(self, key: str) -> Optional[str]:
        raise NotImplementedError()

    @abstractmethod
    def set_value(self, key: str, value: str):
        raise NotImplementedError()
