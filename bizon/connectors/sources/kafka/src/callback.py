from typing import List

from loguru import logger

from bizon.source.callback import AbstractSourceCallback
from bizon.source.models import SourceIteration

from .config import KafkaSourceConfig


class KafkaSourceCallback(AbstractSourceCallback):
    def __init__(self, config: KafkaSourceConfig):
        super().__init__(config)
        self.config: KafkaSourceConfig = config

    def on_iterations_written(self, iterations: List[SourceIteration]):
        """No-op: Offset management is handled entirely in KafkaSource.commit()"""
        # With simplified architecture, messages are automatically marked as processed
        # when consumed from the async poller. The commit() method handles all offset logic.
        logger.debug(
            f"Processed {sum(len(iter.records) for iter in iterations)} records across {len(iterations)} iterations"
        )

    def on_iteration_failed(self, iteration: SourceIteration, error: Exception):
        """No-op: Failed iterations will not be committed by the commit() method"""
        logger.warning(f"Iteration failed with {len(iteration.records)} records: {error}")
        # In the simplified architecture, failed iterations are handled by not calling commit()
        # so their offsets remain uncommitted and will be reprocessed
