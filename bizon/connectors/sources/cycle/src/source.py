
from typing import Any, List, Optional, Tuple, Union
from requests.auth import AuthBase
from pydantic import Field
from bizon.source.config import SourceConfig
from bizon.source.models import SourceIteration
from bizon.source.source import AbstractSource
from bizon.source.auth.builder import AuthBuilder
from bizon.source.auth.config import AuthType
from bizon.source.models import SourceRecord


class CycleSourceConfig(SourceConfig):
    slug: str = Field(..., description="Slug of the Cycle account")


class CycleSource(AbstractSource):
    def __init__(self, config: CycleSourceConfig):
        super().__init__(config)
        self.config: CycleSourceConfig = config
        self.url_graphql = "https://api.product.cycle.app/graphql"

    def get_authenticator(self) -> AuthBase:
        if self.config.authentication.type.value == AuthType.API_KEY:
            return AuthBuilder.token(params=self.config.authentication.params)
    
    @staticmethod
    def streams() -> List[str]:
        return ["customers"]


    @staticmethod
    def get_config_class() -> SourceConfig:
        return CycleSourceConfig

    def check_connection(self) -> Tuple[bool | Any | None]:
        return True, None

    def get_total_records_count(self) -> int | None:
        return None
    

    def run_graphql_query(self, query: str, variables: dict) -> dict:
        """ Run a graphql query and return the response """

        payload = {
            "query": query,
            "variables": variables
        }

        response = self.session.post(
            self.url_graphql,
            json=payload
        )
        
        data = response.json()
        return data
    
    def get_customers(self, pagination: dict) -> SourceIteration:
        """ Return all customers for the given slug """

        if not pagination :
            pagination_str = """
                size: 5
            """ 
        else:
            pagination_str = """
                size: 5
                where: {
                    cursor: "{pagination}"
                    direction: AFTER
                }
            """.format(pagination=pagination.get('endCursor'))

        query = """
            query Customers($slug: DefaultString!) {
            getProductBySlug(slug: $slug) {
                customers(pagination: {
                {pagination_str}
                }) {
                edges {
                    cursor
                    node {
                    id
                    email
                    name
                    company {
                        domain
                        id
                        name
                    }
                    }
                }
                pageInfo {
                    hasPreviousPage
                    hasNextPage
                    startCursor
                    endCursor
                }
                }
            }
            """.format(pagination_str=pagination_str)

        variables = {
            "slug": self.config.slug
        }

        data = self.run_graphql_query(query, variables)

        # TODO - set the next pagination from the pageInfo result from the response
        next_pagination = data.get("data", {}).get("getProductBySlug", {}).get("customers", {}).get("pageInfo", {})

        edges = data.get("data", {}).get("getProductBySlug", {}).get("customers", {}).get("edges", [])
        
        records = []
        for customer in edges:
            customer_data = customer.get("node", {})
            records.append(
                SourceRecord(
                    id=customer_data["id"],
                    data=customer_data,
                )
            )
        
        return SourceIteration(records=records, next_pagination=next_pagination)

    
    def get(self, pagination: dict = None) -> SourceIteration:
        if self.config.stream == "customers":
            return self.get_customers(pagination)

        raise NotImplementedError(f"Stream {self.config.stream} not implemented for Cycle")
