# Clickstream Data Processing Pipeline

A real-time clickstream data processing demo using **Kafka**, **PostgreSQL**, **Valkey**, and **OpenSearch** running on **Aiven** managed services. The goal of this project is to demonstrate how easy it is to implement real-time data pipelines that process clickstream data for an actual e-commerce company (now defunct).



## Data Flow

![Data Flow](assets/dataflow.png)

## Prerequisites

- **Python 3.12** (required)
- **uv** ([install](https://docs.astral.sh/uv/)) or **pip**
- **Git**
- **Terraform** >= 1.5.7 and [Aiven](https://aiven.io/) account



## Quick Start

```bash
# Clone and setup
git clone https://github.com/tonypiazza/aiven-assignment.git
cd aiven-assignment
git pull

# Install dependencies (choose one)
uv venv && source .venv/bin/activate && uv sync && uv pip install -e .
# OR: python -m venv .venv && source .venv/bin/activate && pip install -e .

# Copy example and update terraform.tfvars with your Aiven credentials
cp terraform/terraform.tfvars.example terraform/terraform.tfvars

# Provision infrastructure
cd terraform && terraform init && terraform apply && cd ..

# Generate .env from Terraform outputs (auto-configures all services)
clickstream config generate

# Run pipelines
clickstream consumer start
clickstream producer start --limit 10000
clickstream consumer stop
clickstream status

# Cleanup
cd terraform && terraform destroy
```
> For local development with Docker, see: [local-docker.md](local-docker.md)



## CLI Reference

```bash
clickstream --help                        # Show all commands
clickstream status                        # Service health and counts
clickstream config show                   # Current configuration
clickstream config generate               # Generate .env from Terraform outputs

clickstream consumer start                # Start consumers (1 per partition)
clickstream consumer stop                 # Stop all consumers
clickstream consumer restart              # Restart consumers (e.g., after config change)

clickstream producer start                # Batch mode (fastest)
clickstream producer start --limit N      # First N events only
clickstream producer start --realtime     # Real-time replay (1x speed)
clickstream producer start --realtime 5x  # 5x faster than real-time
clickstream producer stop                 # Stop producer

clickstream data reset [-y]               # Reset all data stores
clickstream analytics [--json]            # Funnel metrics
clickstream benchmark [--limit N] [-y]    # Benchmark consumer throughput
clickstream opensearch init               # Import dashboards
```



## Data Model

### Events

The included `data/events.csv` contains ~2.7M clickstream events which includes a small percentage of duplicates.

| Column | Type | Description |
|--------|------|-------------|
| `timestamp` | Unix ms | Event timestamp |
| `visitorid` | Integer | Unique visitor identifier |
| `event` | Enum | `view`, `addtocart`, or `transaction` |
| `itemid` | Integer | Product identifier |
| `transactionid` | Integer | Transaction ID (nullable) |

### Sessions

Sessions are aggregated from events using a configurable timeout:

| Column | Type | Description |
|--------|------|-------------|
| `session_id` | VARCHAR | Unique session identifier |
| `visitor_id` | BIGINT | Visitor identifier |
| `session_start` | TIMESTAMPTZ | Session start time |
| `session_end` | TIMESTAMPTZ | Session end time |
| `duration_seconds` | INTEGER | Session duration |
| `event_count` | INTEGER | Total events |
| `view_count` | INTEGER | Number of views |
| `cart_count` | INTEGER | Add-to-cart count |
| `transaction_count` | INTEGER | Transaction count |
| `items_viewed` | BIGINT[] | Viewed item IDs |
| `items_carted` | BIGINT[] | Carted item IDs |
| `items_purchased` | BIGINT[] | Purchased item IDs |
| `converted` | BOOLEAN | Had a transaction |



## Kafka Partitioning

Kafka partitioning involves several key tradeoffs:

- **Choosing the right partition count** - Too few partitions limits parallelism and throughput; too many increases broker overhead, memory usage, and end-to-end latency. Once created, increasing partitions is possible but decreasing is not (requires topic recreation).
- **Partition key selection** - The partition key determines which partition receives each message. Poor key selection can lead to "hot partitions" where one partition receives disproportionate traffic.
- **Ordering guarantees** - Kafka only guarantees message ordering within a single partition. If strict global ordering is required, you're limited to a single partition.
- **Consumer group rebalancing** - When consumers join or leave a group, partitions are reassigned, causing temporary processing delays.

For more information, use the following Aiven documentation links:
- [Partition Segments](https://aiven.io/docs/products/kafka/concepts/partition-segments)
- [Create Kafka Topics](https://aiven.io/docs/products/kafka/howto/create-topic)
- [Get Partition Details](https://aiven.io/docs/products/kafka/howto/get-topic-partition-details)

![Partition Throughput](assets/partitions.png)

The above chart shows that throughput scales with the number of Kafka partitions, as each partition is consumed by a dedicated consumer process. Benchmarks show gains up to 4 partitions, with diminishing returns as the volume of incoming data grows. To change partition count, set `kafka_events_topic_partitions` in `terraform/terraform.tfvars`, then apply:

```bash
cd terraform && terraform apply && cd ..
```

Run your own benchmarks with:

```bash
clickstream benchmark
```
This command can be useful for understanding the impact of both partitioning and replication on performance.



## OpenSearch (Optional)

OpenSearch is disabled by default. The OpenSearch consumer uses a separate Kafka consumer group (`clickstream-opensearch`), which enables **automatic backfill**: if you enable OpenSearch after events have already been consumed, the OpenSearch consumer will automatically read from the beginning of the topic and index all events.

### Enable OpenSearch

OpenSearch is not provisioned by default. To enable:

1. Set in `terraform/terraform.tfvars`:
   ```hcl
   opensearch_enabled = true
   ```

2. Apply the changes:
   ```bash
   cd terraform && terraform apply && cd ..
   ```

3. Regenerate `.env` to include OpenSearch credentials:
   ```bash
   clickstream config generate -f
   ```

4. Restart consumer:
   ```bash
   clickstream consumer restart
   ```

### Access Dashboards

First, initialize the dashboards:

```bash
clickstream opensearch init
```

The output should look like:

```
  Initializing OpenSearch
  ✓ OpenSearch is reachable
  ✓ Index 'clickstream-events' exists (99,987 documents)
  ✓ Imported index pattern: clickstream-events*
  ✓ Imported 5 visualizations
  ✓ Imported 2 dashboards

  Open dashboards at https://<your-opensearch-host>
```

Use the link to open the OpenSearch Dashboards web UI. The URL will be your Aiven OpenSearch service URL.

**NOTE**: When prompted for tenant, select **Global**.



## Cleanup

```bash
cd terraform && terraform destroy
```



## License

MIT
