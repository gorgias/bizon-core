from loguru import logger

from bizon.destinations.destination import AbstractDestination
from bizon.engine.pipeline.consumer import AbstractQueueConsumer
from bizon.engine.queue.queue import QUEUE_TERMINATION, AbstractQueue, QueueMessage
from bizon.transform.transform import Transform

from .config import PythonQueueConfig


class PythonQueueConsumer(AbstractQueueConsumer):
    def __init__(
        self, config: PythonQueueConfig, queue: AbstractQueue, destination: AbstractDestination, transform: Transform
    ):
        super().__init__(config, destination=destination, transform=transform)
        self.queue = queue

    def run(self) -> None:
        while True:

            # Retrieve the message from the queue
            queue_message: QueueMessage = self.queue.get()

            # Apply the transformation
            df_source_records = self.transform.apply_transforms(df_source_records=queue_message.df_source_records)

            if queue_message.signal == QUEUE_TERMINATION:
                logger.info("Received termination signal, waiting for destination to close gracefully ...")
                self.destination.write_records_and_update_cursor(
                    df_source_records=df_source_records,
                    iteration=queue_message.iteration,
                    extracted_at=queue_message.extracted_at,
                    pagination=queue_message.pagination,
                    last_iteration=True,
                )
                break

            self.destination.write_records_and_update_cursor(
                df_source_records=df_source_records,
                iteration=queue_message.iteration,
                extracted_at=queue_message.extracted_at,
                pagination=queue_message.pagination,
            )
            self.queue.task_done()
