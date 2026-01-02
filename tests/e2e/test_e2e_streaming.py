import json
import os

import yaml

from bizon.engine.engine import RunnerFactory


def test_e2e_dummy_streaming_to_file():
    BIZON_CONFIG_DUMMY_TO_FILE = f"""
      name: test_job_3

      source:
        name: dummy
        stream: creatures
        sync_mode: stream # This will make the source to stream data to the destination
        authentication:
          type: api_key
          params:
            token: dummy_key
        max_iterations: 4

      destination:
        name: file
        config:
          unnest: true
          record_schemas:
            - destination_id: routed
              record_schema:
                - name: id
                  type: string
                  nullable: false
                - name: name
                  type: string
                  nullable: false
                - name: age
                  type: integer
                  nullable: false

            - destination_id: creatures
              record_schema:
                - name: id
                  type: string
                  nullable: false
                - name: name
                  type: string
                  nullable: false
                - name: age
                  type: integer
                  nullable: false

      transforms:
        - label: transform_data
          python: |
            if 'name' in data:
              data['name'] = data['name'].upper()

      engine:
        runner:
          type: stream
        backend:
          type: postgres
          config:
            database: bizon_test
            schema: public
            syncCursorInDBEvery: 2
            host: {os.environ.get("POSTGRES_HOST", "localhost")}
            port: 5432
            username: postgres
            password: bizon
      """

    runner = RunnerFactory.create_from_config_dict(yaml.safe_load(BIZON_CONFIG_DUMMY_TO_FILE))

    runner.run()

    records_extracted = {}
    with open("creatures.json") as file:
        for line in file.readlines():
            record: dict = json.loads(line.strip())
            records_extracted[record["id"]] = record["name"]

    assert set(records_extracted.keys()) == set([9898, 88787])
    assert records_extracted[9898] == "BIZON"
