import itertools
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Any, List, Literal, Tuple

from loguru import logger
from pydantic import BaseModel, Field
from requests.auth import AuthBase

from bizon.source.auth.authenticators.cookies import CookiesAuthParams
from bizon.source.auth.builder import AuthBuilder
from bizon.source.auth.config import AuthConfig, AuthType
from bizon.source.config import SourceConfig
from bizon.source.models import SourceIteration, SourceRecord
from bizon.source.source import AbstractSource

BASE_URL = "https://app.periscopedata.com"

URL_DATABASES = f"{BASE_URL}/welcome/remaining_state/site_models"


class PeriscopeCookies(BaseModel):
    cf_bm: str = Field(..., description="Cloudflare bm cookie")
    periscope_session: str = Field(..., description="Periscope session cookie")


class PeriscopeCookiesAuthParams(CookiesAuthParams):
    cookies: PeriscopeCookies = Field(..., description="Cookies configuration")


class PeriscopeAuthConfig(AuthConfig):
    type: Literal[AuthType.COOKIES]
    params: PeriscopeCookiesAuthParams


class PeriscopeSourceConfig(SourceConfig):
    authentication: PeriscopeAuthConfig
    workspace_name: str = Field(..., description="Name of the workspace")
    client_site_id: int = Field(..., description="Client site ID")
    database_id: int = Field(..., description="Fetch charts connected to this Database ID")
    x_csrf_token: str = Field(..., description="CSRF token for the requests")


class PeriscopeSource(AbstractSource):
    def __init__(self, config: PeriscopeSourceConfig):
        super().__init__(config)
        self.config: PeriscopeSourceConfig = config

    @staticmethod
    def streams() -> List[str]:
        return [
            "charts",
            "dashboards_metadata",
            "dashboards",
            "databases",
            "users",
            "views",
        ]

    @staticmethod
    def get_config_class() -> AbstractSource:
        return PeriscopeSourceConfig

    @property
    def cookies(self) -> dict:
        return {
            "__cf_bm": self.config.authentication.params.cookies.cf_bm,
            "periscope_session": self.config.authentication.params.cookies.periscope_session,
        }

    @property
    def http_params(self) -> dict:
        return {
            "client_site_id": self.config.client_site_id,
        }

    def check_connection(self) -> Tuple[bool | Any | None]:
        return True, None

    def get_authenticator(self) -> AuthBase:
        if self.config.authentication.type == AuthType.COOKIES:
            return AuthBuilder.cookies(
                params=CookiesAuthParams(
                    cookies=self.cookies,
                    headers={
                        "authority": "app.periscopedata.com",
                        "accept": "application/json, text/javascript, */*; q=0.01",
                        "accept-language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
                        "sec-ch-ua": '"Chromium";v="118", "Google Chrome";v="118", "Not=A?Brand";v="99"',
                        "sec-ch-ua-mobile": "?0",
                        "sec-ch-ua-platform": '"macOS"',
                        "sec-fetch-dest": "empty",
                        "sec-fetch-mode": "cors",
                        "sec-fetch-site": "same-origin",
                        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
                        "x-requested-with": "XMLHttpRequest",
                        "x-csrf-token": self.config.x_csrf_token,
                    },
                )
            )

        raise NotImplementedError(f"Auth type {self.config.authentication.type} not implemented for Periscope")

    def get_total_records_count(self) -> int | None:
        return None

    @staticmethod
    def transform_response_to_source_iteration(records: List[dict]) -> SourceIteration:
        return SourceIteration(
            next_pagination=dict(),
            records=[
                SourceRecord(
                    id=record["id"],
                    data=record,
                )
                for record in records
            ],
        )

    def get_dashboards(self, pagination: dict = None) -> SourceIteration:
        response = self.session.call(
            method="GET",
            url=f"{BASE_URL}/login_state/dashboards",
            params=self.http_params,
        )
        records_json = response.json()["Dashboard"]
        return self.transform_response_to_source_iteration(records_json)

    def get_dashboards_metadata(self, pagination: dict = None) -> SourceIteration:
        params = {
            "client_site_id": self.config.client_site_id,
            "filters": [{"name": "typeFilter", "input": "Dashboard"}],
            "limit": 2000,
            "query_plan": None,
        }

        response = self.session.call(
            method="POST",
            url=f"{BASE_URL}/global_search/search",
            json=params,
        )
        records_json = response.json()["results"]["data"]
        return self.transform_response_to_source_iteration(records_json)

    def get_dashboard_ids(self) -> List[int]:
        source_iteration = self.get_dashboards()
        return [record.id for record in source_iteration.records]

    def _extract_raw_text_from_textbox(self, data: dict) -> str:
        raw_text = []

        def clean_text(text: str):
            """Strip Byte Order Mark (BOM) and other unwanted whitespace."""
            return text.replace("\ufeff", "").strip()

        def traverse_nodes(nodes):
            for node in nodes:
                if node["object"] == "text":
                    for leaf in node["leaves"]:
                        raw_text.append(clean_text(leaf["text"]))
                elif node["type"] == "link" and "data" in node and "url" in node["data"]:
                    link_text = []
                    for leaf in node["nodes"][0]["leaves"]:  # Assume a single text node in link
                        link_text.append(clean_text(leaf["text"]))
                    # Format as Markdown link
                    raw_text.append(f"[{''.join(link_text)}]({node['data']['url']})")
                elif "nodes" in node:  # If there are nested nodes
                    traverse_nodes(node["nodes"])

        if not data["text_data"]:
            return ""

        # Start traversal from the root nodes
        traverse_nodes(data["text_data"]["document"]["nodes"])

        return " ".join(raw_text)

    def _get_charts(self, dashboard_id: int) -> List[dict]:
        MAXIMUM_ITERATION: int = 1000
        iter_count: int = 0
        window_start: int = 0
        iter_charts: List[dict] = []
        charts_list: set = set()

        dashboard_charts: List[dict] = []

        for iter_count in range(MAXIMUM_ITERATION):
            # Break the loop if no more charts are available
            if iter_count > 0 and len(iter_charts) == 0:
                break

            # Prepare params for iteration
            params = {
                "current_dashboard": str(dashboard_id),
                "minimum_dashboard_fluid_row": window_start,
                "fluid_column_for_last_widget_in_row": 0,
            }
            params.update(self.http_params)

            try:
                response = self.session.call(
                    method="GET",
                    url=f"{BASE_URL}/welcome/remaining_widgets",
                    params=params,
                )

                if not response.ok:
                    print(f"Failed to fetch the dashboard with id: {dashboard_id}")
                    continue

                window_start += 50
                iter_count += 1
                iter_charts = response.json().get("Widget")

                iter_textboxes = response.json().get("TextBox")

                for chart in iter_charts:
                    if str(chart.get("database_id")) == str(self.config.database_id):
                        if chart.get("id") not in charts_list:
                            charts_list.add(chart.get("id"))

                            chart["raw_text"] = None

                            # In case the chart is a textbox, we parse the raw text
                            if chart.get("content_id"):
                                text_box = list(
                                    filter(
                                        lambda x: x.get("id") == chart.get("content_id"),
                                        iter_textboxes,
                                    )
                                )

                                if not text_box:
                                    logger.error(
                                        f"Failed to fetch the textbox with id: {chart.get('content_id')} for chart with id: {chart.get('id')}"
                                    )

                                if text_box:
                                    chart["raw_text"] = self._extract_raw_text_from_textbox(text_box[0])

                            dashboard_charts.append(chart)
            except Exception as e:
                logger.error(f"Failed to fetch the dashboard with id: {dashboard_id} with error: {e}")
                continue

        return dashboard_charts

    def get_charts(self, pagination: dict = None) -> SourceIteration:
        BATCH_SIZE = 10

        if not pagination:
            dashboard_ids = self.get_dashboard_ids()

            if not dashboard_ids:
                logger.warning("No dashboard found")
                return dict(), []

            pagination = {"dashboard_ids": dashboard_ids}

        with ThreadPoolExecutor(max_workers=BATCH_SIZE) as executor:
            result = list(executor.map(self._get_charts, pagination["dashboard_ids"][:BATCH_SIZE]))

        # Remove the first 10 dashboard ids if there are more
        if len(pagination["dashboard_ids"]) > BATCH_SIZE:
            pagination["dashboard_ids"] = pagination["dashboard_ids"][BATCH_SIZE:]
        else:
            # We end the pagination
            pagination = {}

        records = list(itertools.chain.from_iterable(result))

        return SourceIteration(
            next_pagination=pagination,
            records=[
                SourceRecord(
                    id=record["id"],
                    data=record,
                )
                for record in records
            ],
        )

    def get_views(self, pagination: dict = None) -> SourceIteration:
        response = self.session.call(
            method="GET",
            url=f"{BASE_URL}/login_state/sql_views",
            params=self.http_params,
        )
        records_json = response.json()["SqlView"]
        return self.transform_response_to_source_iteration(records_json)

    def get_users(self, pagination: dict = None) -> SourceIteration:
        response = self.session.call(
            method="GET",
            url=f"{BASE_URL}/users/owners",
            params=self.http_params,
        )
        records_json = response.json()
        return self.transform_response_to_source_iteration(records_json)

    def get_databases(self, pagination: dict = None) -> SourceIteration:
        response = self.session.call(
            method="GET",
            url=URL_DATABASES,
            params=self.http_params,
        )
        records_json = response.json()["Database"]
        return self.transform_response_to_source_iteration(records_json)

    def get(self, pagination: dict = None) -> SourceIteration:
        if self.config.stream == "dashboards":
            return self.get_dashboards(pagination)

        elif self.config.stream == "charts":
            return self.get_charts(pagination)

        elif self.config.stream == "dashboards_metadata":
            return self.get_dashboards_metadata(pagination)

        elif self.config.stream == "views":
            return self.get_views(pagination)

        elif self.config.stream == "users":
            return self.get_users(pagination)

        elif self.config.stream == "databases":
            return self.get_databases(pagination)

        raise NotImplementedError(f"Stream {self.config.stream} not implemented for Periscope")
