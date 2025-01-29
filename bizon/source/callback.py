from abc import ABC, abstractmethod
from typing import List

from bizon.source.config import SourceConfig
from bizon.source.models import SourceIteration


class AbstractSourceCallback(ABC):
    @abstractmethod
    def __init__(self, config: SourceConfig):
        pass

    @abstractmethod
    def on_iterations_written(self, iterations: List[SourceIteration]):
        pass


class NoOpSourceCallback(AbstractSourceCallback):
    def __init__(self, config: SourceConfig):
        pass

    def on_iterations_written(self, iterations: List[SourceIteration]):
        """No-op callback"""
        pass
