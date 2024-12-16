from abc import ABC, abstractmethod

from bizon.destinations.destination import AbstractDestination
from bizon.engine.queue.config import AbstractQueueConfig
from bizon.transform.transform import Transform


class AbstractQueueConsumer(ABC):
    def __init__(self, config: AbstractQueueConfig, destination: AbstractDestination, transform: Transform):
        self.config = config
        self.destination = destination
        self.transform = transform

    @abstractmethod
    def run(self):
        pass
