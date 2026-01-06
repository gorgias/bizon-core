# Reference Connector: Annotated Production Example

This document provides a fully annotated, production-ready connector that demonstrates best practices. Use this as a reference when creating new connectors.

## Directory Structure

```
bizon/connectors/sources/acme/
├── config/
│   └── api_key.example.yml
└── src/
    ├── __init__.py          # Empty file (required for Python package)
    ├── config.py            # Configuration model
    └── source.py            # Main implementation
```

---

## File 1: `config.py`

```python
"""
Acme API Source Configuration

This file defines:
1. Available streams (as an Enum)
2. Source-specific configuration options (as a Pydantic model)
"""

from enum import Enum
from typing import List, Optional
from pydantic import Field
from bizon.source.config import SourceConfig


# ============================================================
# STREAMS ENUM
# ============================================================
# Define one enum value per API resource/endpoint you want to sync.
# The string value should match what users will put in their YAML config.

class AcmeStreams(str, Enum):
    """
    Available streams for the Acme API.

    Each stream corresponds to one API resource type.
    Users select a stream in their YAML config: `stream: customers`
    """
    CUSTOMERS = "customers"      # GET /customers - List all customers
    ORDERS = "orders"            # GET /orders - List all orders
    PRODUCTS = "products"        # GET /products - List all products


# ============================================================
# SOURCE CONFIG
# ============================================================
# Extend SourceConfig with any source-specific options.
# These become available in the YAML config under `source:`.

class AcmeSourceConfig(SourceConfig):
    """
    Configuration for the Acme source connector.

    Inherits from SourceConfig which provides:
    - name: str (connector name, auto-set)
    - stream: str (which stream to sync)
    - authentication: AuthConfig (auth settings)
    - sync_mode: SourceSyncModes (full_refresh, incremental, stream)
    - max_iterations: Optional[int] (limit pagination iterations)
    """

    # Override stream type to use our enum for validation
    stream: AcmeStreams

    # Source-specific options with descriptions and validation
    page_size: int = Field(
        default=100,
        ge=1,          # Minimum value
        le=250,        # Maximum value (Acme API limit)
        description="Number of records to fetch per API call. Max: 250."
    )

    # Optional: Filter by specific IDs
    customer_ids: List[str] = Field(
        default_factory=list,
        description="Optional list of customer IDs to fetch. If empty, fetches all."
    )

    # Optional: Include related data
    include_metadata: bool = Field(
        default=False,
        description="Whether to include extended metadata in responses."
    )
```

### Key Points - config.py

| Line | What It Does | Why |
|------|--------------|-----|
| `class AcmeStreams(str, Enum)` | Defines available streams | Provides validation and autocomplete |
| `stream: AcmeStreams` | Override base class type | Ensures only valid streams are accepted |
| `Field(default=..., ge=..., le=...)` | Pydantic validation | Catches config errors early |
| `default_factory=list` | For mutable defaults | Avoids Python mutable default gotcha |

---

## File 2: `source.py`

```python
"""
Acme API Source Connector

This connector fetches data from the Acme API and produces SourceRecords
for the Bizon pipeline to process.

Production features demonstrated:
- Retry logic with exponential backoff
- Rate limit handling (Retry-After header)
- Custom headers for API versioning
- Multiple streams with stream-specific logic
- Proper error handling in check_connection()
- Pagination with cursor-based approach
"""

from typing import Any, List, Optional, Tuple

from loguru import logger                    # Bizon uses loguru for logging
from requests import Session
from requests.adapters import HTTPAdapter
from requests.auth import AuthBase
from urllib3.util.retry import Retry

from bizon.source.auth.builder import AuthBuilder
from bizon.source.auth.config import AuthType
from bizon.source.config import SourceConfig
from bizon.source.models import SourceIteration, SourceRecord
from bizon.source.source import AbstractSource

from .config import AcmeSourceConfig, AcmeStreams


# ============================================================
# CONSTANTS
# ============================================================
# Define API-specific constants at the top for easy modification.

BASE_URL = "https://api.acme.com/v2"
API_VERSION = "2024-01-15"  # Acme API requires version header


# ============================================================
# SOURCE IMPLEMENTATION
# ============================================================

class AcmeSource(AbstractSource):
    """
    Acme API source connector.

    Inherits from AbstractSource which provides:
    - self.config: The config object (AcmeSourceConfig)
    - self.session: A requests.Session (created via get_session())
    - Automatic auth header injection via get_authenticator()
    """

    def __init__(self, config: AcmeSourceConfig):
        """
        Initialize the source.

        IMPORTANT: Call super().__init__() first - it sets up:
        - self.config
        - self.session (via get_session())
        - Auth headers (via get_authenticator())
        """
        super().__init__(config)
        # Type hint for IDE autocomplete on config options
        self.config: AcmeSourceConfig = config

    # ----------------------------------------------------------
    # SESSION CONFIGURATION
    # ----------------------------------------------------------
    # Override get_session() to customize retry behavior and headers.
    # This is called once during __init__ and cached as self.session.

    def get_session(self) -> Session:
        """
        Configure the HTTP session with retry logic and custom headers.

        This method is called automatically by AbstractSource.__init__().
        The returned session is stored as self.session.
        """
        session = Session()

        # Configure automatic retries
        # See: https://urllib3.readthedocs.io/en/stable/reference/urllib3.util.html#urllib3.util.Retry
        retries = Retry(
            total=10,                              # Max retry attempts
            backoff_factor=2,                      # Wait 2^n seconds between retries
            status_forcelist=[                     # HTTP codes to retry on
                429,  # Too Many Requests (rate limit)
                500,  # Internal Server Error
                502,  # Bad Gateway
                503,  # Service Unavailable
                504,  # Gateway Timeout
            ],
            allowed_methods=["GET", "POST"],       # Methods to retry
            respect_retry_after_header=True,       # Honor Retry-After header on 429
        )

        # Mount the retry adapter for HTTPS requests
        session.mount("https://", HTTPAdapter(max_retries=retries))

        # Add custom headers required by the API
        session.headers.update({
            "X-API-Version": API_VERSION,          # API versioning
            "Accept": "application/json",          # Request JSON responses
            "Content-Type": "application/json",    # For POST requests
        })

        return session

    # ----------------------------------------------------------
    # AUTHENTICATION
    # ----------------------------------------------------------
    # Return the appropriate authenticator based on config.
    # The authenticator modifies requests to add auth headers.

    def get_authenticator(self) -> AuthBase:
        """
        Return the authentication handler.

        This is called automatically by AbstractSource to inject auth
        into every request made via self.session.

        Supported auth types:
        - API_KEY/BEARER: Adds "Authorization: Bearer <token>" header
        - OAUTH: Same as above, but handles token refresh automatically
        - BASIC: Adds "Authorization: Basic <base64>" header
        - None: No authentication (public API)
        """
        # Handle no authentication case
        if self.config.authentication is None:
            return None

        auth_type = self.config.authentication.type

        if auth_type in [AuthType.API_KEY, AuthType.BEARER]:
            # Token-based auth: Authorization: Bearer <token>
            return AuthBuilder.token(params=self.config.authentication.params)

        elif auth_type == AuthType.OAUTH:
            # OAuth 2.0 with automatic token refresh
            return AuthBuilder.oauth2(params=self.config.authentication.params)

        elif auth_type == AuthType.BASIC:
            # Basic auth: Authorization: Basic <base64(user:pass)>
            return AuthBuilder.basic(params=self.config.authentication.params)

        # Unknown auth type - return None and let request fail
        logger.warning(f"Unknown auth type: {auth_type}, proceeding without auth")
        return None

    # ----------------------------------------------------------
    # REQUIRED STATIC METHODS
    # ----------------------------------------------------------
    # These are called by Bizon's discovery system to understand
    # what this connector provides.

    @staticmethod
    def streams() -> List[str]:
        """
        Return list of available stream names.

        Used by:
        - `bizon stream list acme` CLI command
        - Source discovery system
        """
        return [stream.value for stream in AcmeStreams]

    @staticmethod
    def get_config_class() -> SourceConfig:
        """
        Return the config class for this source.

        Used by Bizon to validate YAML config against the correct schema.
        """
        return AcmeSourceConfig

    # ----------------------------------------------------------
    # CONNECTION CHECK
    # ----------------------------------------------------------
    # Verify the connector can reach the API with the given credentials.

    def check_connection(self) -> Tuple[bool, Optional[Any]]:
        """
        Test API connectivity and authentication.

        Returns:
            Tuple of (success: bool, error_message: str or None)

        This is called by Bizon before starting a sync to verify
        the configuration is valid.
        """
        try:
            # Make a lightweight API call to verify connectivity
            # Most APIs have a /me, /users, or simple list endpoint
            response = self.session.get(f"{BASE_URL}/me")
            response.raise_for_status()

            # Optionally validate the response
            data = response.json()
            if not data.get("id"):
                return False, "Invalid API response: missing user ID"

            return True, None

        except Exception as e:
            # Return the error message for debugging
            return False, str(e)

    def get_total_records_count(self) -> Optional[int]:
        """
        Return total record count if the API provides it.

        Used for progress reporting. Return None if not available.
        """
        # Most list endpoints return total in response headers or body
        # Example: {"total": 1234, "results": [...]}
        try:
            if self.config.stream == AcmeStreams.CUSTOMERS:
                response = self.session.get(
                    f"{BASE_URL}/customers",
                    params={"page_size": 1}  # Minimal request
                )
                return response.json().get("total")
        except Exception:
            pass

        return None  # Not available or not implemented

    # ----------------------------------------------------------
    # DATA FETCHING - STREAM IMPLEMENTATIONS
    # ----------------------------------------------------------
    # Each stream has its own method for fetching data.
    # All return SourceIteration with records and pagination state.

    def get_customers(self, pagination: dict = None) -> SourceIteration:
        """
        Fetch customers from the Acme API.

        Args:
            pagination: Dict containing pagination state from previous call.
                       None on first call, then contains cursor from prev response.

        Returns:
            SourceIteration with:
            - records: List[SourceRecord] - fetched records
            - next_pagination: dict - state for next call, {} if done
        """
        # Build request parameters
        params = {
            "page_size": self.config.page_size,
            "include_metadata": str(self.config.include_metadata).lower(),
        }

        # Add cursor if this is a subsequent page
        if pagination and pagination.get("cursor"):
            params["cursor"] = pagination["cursor"]

        # Filter by specific IDs if configured
        if self.config.customer_ids:
            params["ids"] = ",".join(self.config.customer_ids)

        # Make the API request
        response = self.session.get(f"{BASE_URL}/customers", params=params)
        response.raise_for_status()
        data = response.json()

        # Convert API responses to SourceRecords
        records = [
            SourceRecord(
                id=customer["id"],           # Unique identifier (required)
                data=customer,               # Full record data (required)
                # timestamp=customer.get("updated_at"),  # Optional: for ordering
            )
            for customer in data.get("results", [])
        ]

        # Determine if there are more pages
        # Pattern: cursor-based pagination with has_more flag
        if data.get("has_more") and data.get("next_cursor"):
            next_pagination = {"cursor": data["next_cursor"]}
        else:
            next_pagination = {}  # Empty dict = no more pages

        logger.info(f"Fetched {len(records)} customers, has_more: {data.get('has_more', False)}")

        return SourceIteration(records=records, next_pagination=next_pagination)

    def get_orders(self, pagination: dict = None) -> SourceIteration:
        """
        Fetch orders from the Acme API.

        Demonstrates: Offset-based pagination
        """
        # Calculate offset from pagination state
        offset = pagination.get("offset", 0) if pagination else 0

        params = {
            "limit": self.config.page_size,
            "offset": offset,
        }

        response = self.session.get(f"{BASE_URL}/orders", params=params)
        response.raise_for_status()
        data = response.json()

        records = [
            SourceRecord(id=order["id"], data=order)
            for order in data.get("data", [])  # Different key than customers!
        ]

        # Check if there are more pages
        total = data.get("total", 0)
        if offset + len(records) < total:
            next_pagination = {"offset": offset + self.config.page_size}
        else:
            next_pagination = {}

        return SourceIteration(records=records, next_pagination=next_pagination)

    def get_products(self, pagination: dict = None) -> SourceIteration:
        """
        Fetch products from the Acme API.

        Demonstrates: URL-based pagination (next URL in response)
        """
        # Use next URL if available, otherwise start from beginning
        if pagination and pagination.get("next_url"):
            url = pagination["next_url"]
            params = {}  # URL already contains params
        else:
            url = f"{BASE_URL}/products"
            params = {"page_size": self.config.page_size}

        response = self.session.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        records = [
            SourceRecord(id=product["id"], data=product)
            for product in data.get("items", [])  # Yet another key!
        ]

        # Check for next page URL
        next_url = data.get("links", {}).get("next")
        next_pagination = {"next_url": next_url} if next_url else {}

        return SourceIteration(records=records, next_pagination=next_pagination)

    # ----------------------------------------------------------
    # MAIN DISPATCH
    # ----------------------------------------------------------
    # The get() method is the main entry point called by Bizon.
    # It dispatches to stream-specific methods.

    def get(self, pagination: dict = None) -> SourceIteration:
        """
        Main entry point for fetching data.

        This method is called repeatedly by the Bizon pipeline:
        1. First call: pagination=None
        2. Subsequent calls: pagination=result.next_pagination from previous call
        3. Stops when: next_pagination is empty {}

        Args:
            pagination: Pagination state from previous call (None on first call)

        Returns:
            SourceIteration with records and next_pagination
        """
        # Dispatch to stream-specific method
        if self.config.stream == AcmeStreams.CUSTOMERS:
            return self.get_customers(pagination)

        elif self.config.stream == AcmeStreams.ORDERS:
            return self.get_orders(pagination)

        elif self.config.stream == AcmeStreams.PRODUCTS:
            return self.get_products(pagination)

        # Handle unknown stream (shouldn't happen with enum validation)
        raise NotImplementedError(
            f"Stream '{self.config.stream}' is not implemented for Acme source"
        )
```

### Key Points - source.py

| Section | What It Does | Common Alternatives |
|---------|--------------|---------------------|
| `get_session()` | Configures retries and headers | Skip for default retry behavior |
| `get_authenticator()` | Returns auth handler | Return `None` for public APIs |
| `streams()` | Lists available streams | Hard-coded list if no enum |
| `check_connection()` | Tests connectivity | Any lightweight authenticated endpoint |
| `get_total_records_count()` | Returns total for progress | Return `None` if not available |
| `get_customers()` | Cursor-based pagination | Most common pattern |
| `get_orders()` | Offset-based pagination | Used by older APIs |
| `get_products()` | URL-based pagination | Common in REST APIs |
| `get()` | Dispatches to stream methods | Required entry point |

---

## File 3: `config/api_key.example.yml`

```yaml
# Acme API Source Configuration
# Authentication: API Key
#
# To get your API key:
# 1. Log in to https://dashboard.acme.com
# 2. Go to Settings > API Keys
# 3. Click "Create API Key" and copy the token

name: acme_customers_to_logger

source:
  name: acme
  stream: customers  # Options: customers, orders, products
  authentication:
    type: api_key
    params:
      token: <YOUR_API_KEY>

  # Optional: Limit to specific customer IDs
  # customer_ids:
  #   - "cust_123abc"
  #   - "cust_456def"

  # Optional: Include extended metadata
  # include_metadata: true

  # Results per page (1-250, default: 100)
  page_size: 100

destination:
  name: logger  # Prints records to console (for testing)
  config:
    dummy: bizon

# Optional: Use in-memory backend for testing
# engine:
#   backend:
#     type: sqlite_in_memory
#     config:
#       database: not_used
#       schema: not_used
```

---

## Common Patterns Reference

### When to Use Each Pattern

| Your API Has | Use This Pattern | Example |
|--------------|------------------|---------|
| `cursor` or `next_cursor` field | Cursor-based | Notion, Stripe, most modern APIs |
| `next` URL in response | URL-based | PokeAPI, GitHub, REST standard |
| `offset` + `limit` params | Offset-based | Legacy APIs, SQL-style pagination |
| `page` number param | Page-based | Similar to offset, use offset logic |
| No pagination / small dataset | Single response | Return `next_pagination={}` immediately |

### Error Handling Best Practices

```python
def check_connection(self) -> Tuple[bool, Optional[Any]]:
    try:
        response = self.session.get(f"{BASE_URL}/endpoint")
        response.raise_for_status()
        return True, None
    except requests.exceptions.HTTPError as e:
        # Specific HTTP errors
        return False, f"HTTP {e.response.status_code}: {e.response.text[:200]}"
    except requests.exceptions.ConnectionError:
        return False, f"Cannot connect to {BASE_URL}"
    except requests.exceptions.Timeout:
        return False, "Request timed out"
    except Exception as e:
        return False, str(e)
```

### Logging Best Practices

```python
from loguru import logger

# Info: Progress updates
logger.info(f"Fetched {len(records)} records from page {page_num}")

# Warning: Non-fatal issues
logger.warning(f"No records found for customer_id={customer_id}")

# Error: Failures (usually in try/except)
logger.error(f"Failed to fetch page: {e}")

# Debug: Detailed info (only shown with DEBUG log level)
logger.debug(f"Request params: {params}")
```
