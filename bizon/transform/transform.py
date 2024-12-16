import json
import textwrap

import polars as pl
from loguru import logger

from .config import TransformModel


class Transform:

    def __init__(self, transforms: list[TransformModel]):
        self.transforms = transforms

    def apply_transforms(self, df_source_records: pl.DataFrame) -> pl.DataFrame:
        """Apply transformation on df_source_records"""

        # Process the transformations
        for transform in self.transforms:

            logger.debug(f"Applying transform {transform.label}")

            # Create a function to be executed in the desired context
            def my_transform(data: dict) -> str:

                # Start writing here
                local_vars = {"data": data}

                # Normalize the indentation of the Python code
                normalized_python = textwrap.dedent(transform.python)

                exec(normalized_python, {}, local_vars)

                # Stop writing here
                return json.dumps(local_vars["data"])

            df_source_records = df_source_records.with_columns(
                pl.col("data").str.json_decode().map_elements(my_transform, return_dtype=pl.String).alias("data")
            )

        return df_source_records
