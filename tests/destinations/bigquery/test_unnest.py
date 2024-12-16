import polars as pl
from datetime import datetime
from bizon.destinations.models import destination_record_schema
from bizon.destinations.bigquery.src.destination import BigQueryDestination
from bizon.destinations.bigquery.src.config import BigQueryColumn

def test_unnest_records_to_biguqery():


    df_destination_records = pl.DataFrame(
        data={
            "source_record_id": ["1"],
            "source_timestamp": [datetime.now()],
            "source_data": ['{"id": 1, "name": "Alice", "created_at": "2021-01-01 00:00:00"}'],
            "bizon_extracted_at": [datetime.now()],
            "bizon_loaded_at": [datetime.now()],
            "bizon_id": ["1"],
        },
        schema=destination_record_schema
    )

    assert df_destination_records.height > 0


    res = BigQueryDestination.unnest_data(
        df_destination_records=df_destination_records,
        record_schema=[
            BigQueryColumn(name="id", type="INTEGER", mode="REQUIRED"),
            BigQueryColumn(name="name", type="STRING", mode="REQUIRED"),
            # BigQueryColumn(name="created_at", type="DATETIME", mode="REQUIRED"),
        ]
    )

    assert res.height == 1
