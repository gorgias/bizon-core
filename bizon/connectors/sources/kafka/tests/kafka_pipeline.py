import os

from bizon.engine.engine import RunnerFactory

if __name__ == "__main__":
    runner = RunnerFactory.create_from_yaml(filepath=os.path.abspath("test-pipeline-streaming-v2.yml"))
    runner.run()
