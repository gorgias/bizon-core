import csv
import io
import time
from typing import Any, List, Tuple

from loguru import logger
from pydantic import Field
from requests.auth import AuthBase

from bizon.source.auth.builder import AuthBuilder
from bizon.source.auth.config import AuthType
from bizon.source.config import SourceConfig
from bizon.source.models import SourceIteration, SourceRecord
from bizon.source.source import AbstractSource


class SanaSourceConfig(SourceConfig):
    query: str = Field(..., description="Query to get the data from the Sana Insight API")
    domain: str = Field(..., description="Domain of the Sana instance")


class SanaSource(AbstractSource):
    def __init__(self, config: SanaSourceConfig):
        super().__init__(config)
        self.config: SanaSourceConfig = config
        self.base_url = f"https://{config.domain}.sana.ai/api/v1"

    def get_authenticator(self) -> AuthBase:
        if self.config.authentication.type.value == AuthType.OAUTH:
            return AuthBuilder.oauth2(params=self.config.authentication.params)

    @staticmethod
    def streams() -> List[str]:
        return ["insight_report"]

    @staticmethod
    def get_config_class() -> SourceConfig:
        return SanaSourceConfig

    def check_connection(self) -> Tuple[bool | Any | None]:
        return True, None

    def get_total_records_count(self) -> int | None:
        return None

    def create_insight_report_job(self, query: str) -> str:
        """Create an insight report for the given query"""
        response = self.session.post(f"{self.base_url}/reports/query", json={"query": query, "format": "csv"})
        return response.json()["data"]["jobId"]

    def get_insight_report_job(self, job_id: str) -> dict:
        """Get an insight report job for the given job id"""
        response = self.session.get(f"{self.base_url}/reports/jobs/{job_id}")
        return response.json()

    def get_insight_report(self, pagination: dict) -> SourceIteration:
        """Return all insight report for the given query"""

        job_id = self.create_insight_report_job(self.config.query)
        logger.info(f"Created insight report job {job_id} for query {self.config.query}")

        response = self.get_insight_report_job(job_id)
        status = response["data"]["status"]
        while status != "successful":
            time.sleep(3)
            response = self.get_insight_report_job(job_id)
            status = response["data"]["status"]
            logger.info(f"Insight report job {job_id} is {status}")

        link = response["data"]["link"]["url"]
        logger.info(f"Link for insight report job {job_id} is {link}")

        csv_response = self.session.get(link)
        csv_content = csv_response.content.decode("utf-8")

        reader = csv.DictReader(io.StringIO(csv_content))
        data = [SourceRecord(id=str(i), data=row) for i, row in enumerate(reader)]

        return SourceIteration(records=data, next_pagination={})

    def get(self, pagination: dict = None) -> SourceIteration:
        if self.config.stream == "insight_report":
            return self.get_insight_report(pagination)

        raise NotImplementedError(f"Stream {self.config.stream} not implemented for Sana")
