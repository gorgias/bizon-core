import json
import os
import tempfile

import yaml

from bizon.engine.engine import RunnerFactory


def test_e2e_dummy_to_file():

    with tempfile.NamedTemporaryFile(delete=False) as temp:

        BIZON_CONFIG_DUMMY_TO_FILE = f"""
        name: test_job_3

        source:
          name: dummy
          stream: creatures
          sync_mode: stream
          authentication:
            type: api_key
            params:
              token: dummy_key
          max_iterations: 4

        destination:
          name: file
          config:
            dummy: "dummy"
            unnest: true
            record_schemas:
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
              - destination_id: plants
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
        with open(temp.name, "r") as file:
            for line in file.readlines():
                record: dict = json.loads(line.strip())
                records_extracted[record["source_record_id"]] = record["source_data"]

        assert set(records_extracted.keys()) == set(["9898", "88787", "98", "3333", "56565"])
        assert json.loads(records_extracted["9898"]).get("name") == "BIZON"
