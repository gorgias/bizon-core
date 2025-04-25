from typing import Any, List, Tuple

from pydantic import Field
from requests.auth import AuthBase

from bizon.source.auth.builder import AuthBuilder
from bizon.source.auth.config import AuthType
from bizon.source.config import SourceConfig
from bizon.source.models import SourceIteration, SourceRecord
from bizon.source.source import AbstractSource


class NoticeableSourceConfig(SourceConfig):
    project_id: str = Field(..., description="Project ID of the Noticeable account")


class NoticeableSource(AbstractSource):
    def __init__(self, config: NoticeableSourceConfig):
        super().__init__(config)
        self.config: NoticeableSourceConfig = config
        self.url_graphql = "https://api.noticeable.io/graphql"

    def get_authenticator(self) -> AuthBase:
        if self.config.authentication.type.value == AuthType.API_KEY:
            return AuthBuilder.token(params=self.config.authentication.params)

    @staticmethod
    def streams() -> List[str]:
        return ["email_opened_events"]

    @staticmethod
    def get_config_class() -> SourceConfig:
        return NoticeableSourceConfig

    def check_connection(self) -> Tuple[bool | Any | None]:
        return True, None

    def get_total_records_count(self) -> int | None:
        return None

    def run_graphql_query(self, query: str) -> dict:
        """Run a graphql query and return the response"""

        payload = {"query": query}

        response = self.session.post(self.url_graphql, json=payload)

        data = response.json()
        return data

    def _get_pagination_str(self, pagination: dict) -> str:
        if not pagination:
            pagination_str = ""
        else:
            pagination_str = f', after: "{pagination.get("endCursor")}"'

        return pagination_str

    def get_email_opened_events(self, pagination: dict) -> SourceIteration:
        """Return all email opened events for the given project"""

        pagination_str = self._get_pagination_str(pagination=pagination)

        query = """
        query {
            project(projectId: "$project_id") {
                name
                emailEvents(first: 100, type: OPEN PAGINATION_STRING) {
                    pageInfo {
                        endCursor
                        hasNextPage
                        hasPreviousPage
                        startCursor
                    }
                    edges {
                        cursor
                        node {
                            id
                            dispatchId
                            createdAt
                            recipient
                            subject
                        }
                    }
                }
            }
        }
        """.replace(
            "PAGINATION_STRING", pagination_str
        ).replace(
            "$project_id", self.config.project_id
        )

        data = self.run_graphql_query(query)

        # Parse edges from response
        edges = data.get("data", {}).get("project", {}).get("emailEvents", {}).get("edges", [])

        records = [SourceRecord(id=edge["node"]["id"], data=edge["node"]) for edge in edges]
        # Get pagination info from response
        pagination_info = data.get("data", {}).get("project", {}).get("emailEvents", {}).get("pageInfo", {})
        next_pagination = pagination_info if pagination_info.get("hasNextPage") else {}

        return SourceIteration(records=records, next_pagination=next_pagination)

    def get(self, pagination: dict = None) -> SourceIteration:
        if self.config.stream == "email_opened_events":
            return self.get_email_opened_events(pagination)

        raise NotImplementedError(f"Stream {self.config.stream} not implemented for Noticeable")
