# BigQuery Iceberg Destination Connector

This connector writes data to Apache Iceberg tables stored in Google Cloud Storage (GCS) and exposes them as BigLake external tables in BigQuery. It supports multiple catalog types, custom schemas, partitioning, clustering, and upsert operations.

## Architecture Overview
```mermaid
graph TB
    subgraph "Data Sources"
        S1[Kafka]
        S2[Other Sources]
    end

    subgraph "Bizon Pipeline"
        BP[Bizon Engine]
        DC[Destination Connector]
    end

    subgraph "Configuration"
        GC[Global Config]
        TC[Table Config]
        UC[Upsert Config]
    end

    subgraph "Iceberg Layer"
        IC[Iceberg Catalog<br/>SQL/REST/Hive]
        IT[Iceberg Tables]
        MD[Metadata Files]
    end

    subgraph "Storage Layer"
        GCS[Google Cloud Storage]
        PF[Parquet/ORC/Avro Files]
    end

    subgraph "Query Layer"
        BQ[BigQuery]
        BL[BigLake External Tables]
    end

    S1 --> BP
    S2 --> BP
    BP --> DC
    GC --> DC
    TC --> DC
    UC --> DC
    DC --> IC
    IC --> IT
    IT --> MD
    IT --> PF
    MD --> GCS
    PF --> GCS
    BL --> GCS
    BQ --> BL

    style S1 fill:#e1f5e1
    style S2 fill:#e1f5e1
    style GCS fill:#4285f4
    style BQ fill:#4285f4
    style IC fill:#ff9800
```

## Data Flow Process

```mermaid
flowchart TD
    Start([Start: Receive Data Batch])

    CheckDest{Multiple<br/>Destinations?}

    GroupData[Group Records by<br/>destination_id]

    ProcessSingle[Process Single<br/>Destination]
    ProcessMulti[Process Multiple<br/>Destinations in Parallel]

    ExtractSchema{Custom Schema<br/>Configured?}

    ParseJSON[Extract & Parse<br/>source_data JSON]
    UseDefault[Use Default<br/>Bizon Schema]

    CheckTable{Table<br/>Exists?}

    CreateTable[Create Iceberg Table<br/>with Schema]
    UseTable[Use Existing Table]

    CheckUpsert{Upsert<br/>Enabled?}

    PerformUpsert[Perform Upsert<br/>Based on join_cols]
    AppendData[Append Data<br/>to Table]

    UpdateBigLake[Update BigLake<br/>Metadata URI]

    Success([Success])
    Error([Error with Retry])

    Start --> CheckDest
    CheckDest -->|Yes| GroupData
    CheckDest -->|No| ProcessSingle
    GroupData --> ProcessMulti
    ProcessSingle --> ExtractSchema
    ProcessMulti --> ExtractSchema

    ExtractSchema -->|Yes| ParseJSON
    ExtractSchema -->|No| UseDefault
    ParseJSON --> CheckTable
    UseDefault --> CheckTable

    CheckTable -->|No| CreateTable
    CheckTable -->|Yes| UseTable
    CreateTable --> CheckUpsert
    UseTable --> CheckUpsert

    CheckUpsert -->|Yes| PerformUpsert
    CheckUpsert -->|No| AppendData

    PerformUpsert --> UpdateBigLake
    AppendData --> UpdateBigLake

    UpdateBigLake --> Success
    UpdateBigLake -.->|Failure| Error
    Error -.->|Retry| CheckUpsert

    style Start fill:#e1f5fe
    style Success fill:#c8e6c9
    style Error fill:#ffccbc
    style PerformUpsert fill:#fff3e0
```

## Configuration

The connector uses a flexible `catalog_config` structure that allows you to configure any type of Iceberg catalog:

### SQL Catalog (Default)

```yaml
bigquery_iceberg:
  project_id: "your-project"
  dataset_id: "your_dataset"
  gcs_warehouse_bucket: "your-bucket"
  catalog_config:
    default:
      type: "sql"
      uri: "postgresql+psycopg2://username:password@localhost/mydatabase"
      init_catalog_tables: false
```

### REST Catalog

```yaml
bigquery_iceberg:
  project_id: "your-project"
  dataset_id: "your_dataset"
  gcs_warehouse_bucket: "your-bucket"
  catalog_config:
    production:
      type: "rest"
      uri: "http://your-catalog-server:8181"
      warehouse: "gs://your-bucket/iceberg"
```

### File-based Catalog (GCS)

```yaml
bigquery_iceberg:
  project_id: "your-project"
  dataset_id: "your_dataset"
  gcs_warehouse_bucket: "your-bucket"
  catalog_config:
    default:
      type: "hive"
      warehouse: "gs://your-bucket/iceberg"
```

## Features

- **Flexible Catalog Support**: Works with SQL, REST, and file-based Iceberg catalogs
- **BigQuery Integration**: Automatically creates and updates BigLake external tables
- **Schema Management**: Converts Polars DataFrames to Iceberg schemas automatically
- **Namespace Management**: Automatic creation and management of Iceberg namespaces
- **Table Partitioning**: Support for field-based partitioning with automatic transform selection
- **Batch Processing**: Configurable batch sizes for optimal performance
- **Multiple File Formats**: Supports Parquet, ORC, and Avro table formats
- **Upsert Operations**: Full support for merge operations with configurable behavior
- **Multi-Destination Routing**: Route data to different tables based on destination_id
- **Parallel Processing**: Process multiple destinations concurrently
- **Automatic Retry**: Built-in retry logic with exponential backoff

## Upsert Operations

The connector supports full upsert (merge) operations for updating existing records and inserting new ones.

### Upsert Configuration

```mermaid
stateDiagram-v2
    [*] --> CheckConfig: Data Arrives

    CheckConfig --> UpsertEnabled: Upsert Configured
    CheckConfig --> AppendOnly: No Upsert Config

    UpsertEnabled --> MatchRecords: Find Matching Records<br/>by join_cols

    MatchRecords --> RecordExists: Record Found
    MatchRecords --> NoRecord: No Match

    RecordExists --> UpdateRecord: when_matched_update_all=true
    RecordExists --> SkipUpdate: when_matched_update_all=false

    NoRecord --> InsertRecord: when_not_matched_insert_all=true
    NoRecord --> SkipInsert: when_not_matched_insert_all=false

    UpdateRecord --> [*]: Complete
    SkipUpdate --> [*]: Complete
    InsertRecord --> [*]: Complete
    SkipInsert --> [*]: Complete
    AppendOnly --> [*]: Append All Records
```

### Global Upsert Configuration
```yaml
destination:
  name: bigquery_iceberg
  config:
    # Global upsert configuration (applies to all tables)
    upsert:
      enabled: true
      join_cols: ["id"]  # Optional: columns to match on
      when_matched_update_all: true      # Update existing records
      when_not_matched_insert_all: true  # Insert new records
      case_sensitive: false              # Case-insensitive matching
```

### Per-Table Upsert Configuration
```yaml
destination_table_config:
  - destination_id: "customers"
    upsert:
      enabled: true
      join_cols: ["customer_id"]         # Single key upsert
      when_matched_update_all: true      # Update all fields
      when_not_matched_insert_all: true  # Insert new customers
  
  - destination_id: "products"
    upsert:
      enabled: true
      join_cols: ["product_id", "variant_id"]  # Composite key
      when_matched_update_all: false           # Insert-only mode
      when_not_matched_insert_all: true
```

### Upsert Scenarios

1. **Full Upsert** (Default):
   - Updates existing records and inserts new ones
   - Best for maintaining current state of entities

2. **Insert-Only**:
   - Only inserts new records, skips existing ones
   - Useful for append-only event streams

3. **Update-Only**:
   - Only updates existing records, ignores new ones
   - Useful for updating reference data

## Destination Table Configuration

The connector supports multi-table routing with custom schema mapping:

### Basic Configuration (New Format)
```yaml
destination_table_config:
  - destination_id: "user_events"
    iceberg_schema:
      - name: uid       # Field name in Iceberg table
        type: long
      - name: event
        type: string
      - name: event_timestamp
        type: timestamp
```

### Legacy Format (Still Supported)
```yaml
destination_table_config:
  - destination_id: "user_events"
    iceberg_schema:
      user_id:
        target_field: "uid"
        iceberg_type: "long"
      event_name:
        target_field: "event"
        iceberg_type: "string"
      created_at:
        target_field: "event_timestamp"
        iceberg_type: "timestamp"
```

**Important**: When using schema configuration:
- `time_partitioning` fields use the source field names from your data
- The new list format is simpler and recommended for new configurations

### Multi-Table Configuration (New Format)
```yaml
destination_table_config:
  - destination_id: "user_events"
    iceberg_schema:
      - name: uid
        type: long
      - name: event
        type: string
  - destination_id: "orders"
    iceberg_schema:
      - name: order_id
        type: string
      - name: customer_id
        type: long
      - name: total_amount
        type: double
```

## Partitioning

The connector supports Iceberg time-based partitioning for improved query performance:

### Time-based Partitioning
```yaml
time_partitioning:
  - field: "_bizon_loaded_at"    # Time field to partition by
    type: DAY                    # YEAR, MONTH, DAY, or HOUR
  - field: "event_timestamp"     # Multiple time partitions supported
    type: HOUR
```

### Schema Mapping with Time Partitioning
When using schema mapping with time partitioning:
```yaml
time_partitioning:
  - field: "created_at"  # Source field name (before mapping)
    type: DAY
destination_table_config:
  - destination_id: "events"
    iceberg_schema:
      created_at:
        target_field: "event_timestamp"
        iceberg_type: "timestamp"
      user_id:
        target_field: "user_account_id"
        iceberg_type: "long"
```

**Transform Types:**
- **YEAR**: Partition by year (e.g., 2024)
- **MONTH**: Partition by month (e.g., 2024-03)  
- **DAY**: Partition by day (e.g., 2024-03-15)
- **HOUR**: Partition by hour (e.g., 2024-03-15-14)

**Benefits:**
- Faster query performance by pruning irrelevant partitions
- Better data organization and management
- Reduced scan costs in BigQuery
- Improved concurrent write performance

## Iceberg Data Types

The connector supports the following Iceberg primitive types for field mapping:

### Numeric Types
- `int` - 32-bit signed integers
- `long` - 64-bit signed integers
- `float` - 32-bit IEEE floating point
- `double` - 64-bit IEEE floating point

### Text and Binary
- `string` - UTF-8 encoded strings
- `binary` - Arbitrary byte arrays

### Temporal Types
- `date` - Calendar dates (year, month, day)
- `timestamp` - Timestamp with microsecond precision
- `time` - Time of day (hour, minute, second, microsecond)

### Other Types
- `boolean` - Boolean values (true/false)

### Default Bizon Schema
If no custom `iceberg_schema` is provided, the connector uses this default mapping:
```yaml
# Default schema (applied automatically if not specified)
iceberg_schema:
  - name: _bizon_extracted_at
    type: timestamp
  - name: _bizon_id
    type: string
  - name: _bizon_loaded_at
    type: timestamp
  - name: _source_record_id
    type: string
  - name: _source_timestamp
    type: timestamp
  - name: _source_data
    type: string
```

## Namespace Configuration

The connector supports Iceberg namespace organization for better table management:

### Basic Namespace Setup
```yaml
bigquery_iceberg:
  iceberg_namespace: bizon  # Default namespace
  # ... other config
```

### Environment-Specific Namespaces
```yaml
# Production
iceberg_namespace: analytics

# Development  
iceberg_namespace: dev_bizon

# Team-specific
iceberg_namespace: data_engineering
```

**Namespace Benefits:**
- **Organization**: Group related tables logically
- **Multi-tenancy**: Separate environments or teams
- **Security**: Namespace-level access controls
- **Automatic Creation**: Namespaces are created automatically if they don't exist

## Setup Requirements

1. **GCS Bucket**: For storing Iceberg data files
2. **BigLake Connection**: For BigQuery to access GCS data
3. **Iceberg Catalog**: SQL database, REST service, or file-based catalog
4. **IAM Permissions**: Appropriate permissions for GCS and BigQuery access

## Example Configurations

See the provided example files for different use cases:

- **`example.iceberg.yaml`** - Basic configuration with Kafka source
- **`example.advanced.yaml`** - Advanced configurations with multiple catalog types  
- **`example.dev.yaml`** - Simplified setup for development and testing

## Usage

The connector integrates seamlessly with Bizon pipelines:

```bash
# Run with the basic example
bizon run example.iceberg.yaml

# Run with development configuration  
bizon run example.dev.yaml
```

The destination handles schema creation, data writing, and BigQuery table management automatically.

## Testing

Run the test suite to verify functionality:

```bash
poetry run pytest tests/connectors/destinations/bigquery_iceberg/ -v
```

## Error Handling & Retry Logic

```mermaid
graph TD
    Op[Operation]
    Try[Try Operation]
    Success[Success]
    Fail[Failure]
    Check{Retryable<br/>Error?}
    Retry[Wait & Retry]
    Counter{Max<br/>Attempts?}
    FinalFail[Final Failure]
    
    Op --> Try
    Try --> Success
    Try --> Fail
    Fail --> Check
    Check -->|Yes| Counter
    Check -->|No| FinalFail
    Counter -->|No| Retry
    Counter -->|Yes| FinalFail
    Retry --> Try
    
    style Success fill:#c8e6c9
    style FinalFail fill:#ffcdd2
```

The connector implements comprehensive retry logic for:
- Network failures (ConnectionError, SSLError, Timeout)
- GCS service errors (ServerError, ServiceUnavailable)
- Transient Iceberg catalog issues
- BigLake metadata update failures

## Performance Optimization

1. **Batch Size**: Configure `write_batch_size` based on your data volume (default: 1000)
2. **File Size**: Set `target_file_size_mb` for optimal query performance (128-512 MB recommended)
3. **Time Partitioning**: Use time-based partitioning for large datasets to improve query performance
4. **Parallel Processing**: Multi-destination writes are processed concurrently (up to 4 workers)
5. **Upsert Optimization**: Use appropriate join columns and minimize the number of join keys

## Architecture

1. **Data Writing**: Uses pyiceberg to write or upsert Polars DataFrames to Iceberg format in GCS
2. **Schema Management**: Automatically converts column types and handles schema evolution
3. **BigQuery Integration**: Creates external tables that reference the Iceberg metadata
4. **Metadata Updates**: Keeps BigQuery external tables synchronized with latest Iceberg snapshots
5. **Upsert Engine**: Leverages Iceberg's native merge capabilities for efficient updates
