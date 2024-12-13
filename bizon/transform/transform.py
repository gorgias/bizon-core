import polars as pl
from loguru import logger
from .config import TransformModel


class Transform:

    def __init__(self, transforms: list[TransformModel]):
        self.transforms = transforms

    def apply_transforms(self, df_source_records: pl.DataFrame) -> pl.DataFrame:
        """ Apply transformation on df_source_records"""

        # Process the transformations
        for transform in self.transforms:

            if transform.python:
                logger.debug(f"Applying transform {transform.label}")
                # Execute the Python code in the desired context
                local_vars = {"df": df_source_records, "pl": pl}  # Provide the DataFrame and Polars module as context
                exec(transform.python, {}, local_vars)
                df_source_records = local_vars["df"]  # Retrieve the updated DataFrame

        return df_source_records