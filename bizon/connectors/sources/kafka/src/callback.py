from typing import List

from bizon.source.callback import AbstractSourceCallback
from bizon.source.models import SourceIteration

from .config import KafkaSourceConfig


class KafkaSourceCallback(AbstractSourceCallback):
    def __init__(self, config: KafkaSourceConfig):
        super().__init__(config)

    def on_iterations_written(self, iterations: List[SourceIteration]):
        """Commit the offsets of the iterations"""

        # TODO: Implement the callback

        pass
