import pytest
import yaml

from bizon.engine.engine import RunnerFactory
from bizon.engine.pipeline.models import PipelineReturnStatus

BIZON_CONFIG_DUMMY_TO_FILE = """
"""


def test_e2e_pipeline_should_stop():
    runner = RunnerFactory.create_from_config_dict(yaml.safe_load(BIZON_CONFIG_DUMMY_TO_FILE))

    status = runner.run()

    assert status == PipelineReturnStatus.TRANSFORM_ERROR
