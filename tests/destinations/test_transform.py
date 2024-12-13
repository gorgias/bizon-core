from bizon.transform.transform import TransformModel
from bizon.source.models import source_record_schema
import polars as pl
import json


def test_simple_python_transform():
    # Define the transformation

    # Create dummy df_source_data
    data = [
        {
            "name": "John",
            "age": 8,
        },
        {
            "name": "Jane",
            "age": 9,
        },
        {
            "name": "Jack",
            "age": 10,
        }
    ]

    df_source_data = pl.DataFrame(
        {
            "id": ["1", "2", "3"],
            "data": [json.dumps(row) for row in data],
            "timestamp": [20, 30, 40],
        },
        schema=source_record_schema
    )

    # Transform 'data' column to make names lowercase
    def transform_data(data):

        # Start writing here
        if 'name' in data:
            data['name'] = data['name'].lower()

        # Stop writing here
        return json.dumps(data)

    df_source_data = df_source_data.with_columns(pl.col("data").str.json_decode().map_elements(transform_data))

    assert df_source_data["data"].str.json_decode().to_list()[0] == { "name": "john", "age": 8 }
