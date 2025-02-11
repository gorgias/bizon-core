import json

import polars as pl

from bizon.source.models import source_record_schema
from bizon.transform.transform import Transform, TransformModel


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
        },
    ]

    df_source_data = pl.DataFrame(
        {
            "id": ["1", "2", "3"],
            "data": [json.dumps(row) for row in data],
            "timestamp": [20, 30, 40],
            "destination_id": ["persons", "persons", "persons"],
        },
        schema=source_record_schema,
    )

    transform = Transform(
        transforms=[
            TransformModel(
                label="transform_data",
                python="""
                if 'name' in data:
                    data['name'] = data['name'].upper()
                """,
            )
        ]
    )

    df_source_data = transform.apply_transforms(df_source_records=df_source_data)

    assert df_source_data["data"].str.json_decode().to_list()[0] == {"name": "JOHN", "age": 8}
