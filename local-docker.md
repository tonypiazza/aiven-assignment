# Local Development with Docker

This guide covers running the clickstream pipeline locally using Docker Compose. For production deployment on Aiven managed services, see the main [README.md](README.md).

## Prerequisites

- **Python 3.12** (required)
- **uv** ([install](https://docs.astral.sh/uv/)) or **pip**
- **Docker** and **Docker Compose**
- **Git LFS**

## Quick Start

```bash
# Clone and setup
git clone https://github.com/tonypiazza/aiven-assignment.git
cd aiven-assignment
git pull

# Install dependencies (choose one)
uv venv && source .venv/bin/activate && uv sync && uv pip install -e .
# OR: python -m venv .venv && source .venv/bin/activate && pip install -e .

# Configure and start
cp .env.local.example .env
docker compose up -d                         # Core services (Kafka, PostgreSQL, Valkey)
# Or include OpenSearch: docker compose --profile opensearch-enabled up -d

# Run pipeline
clickstream consumer start                  # Start consumer(s) (Kafka -> PostgreSQL)
clickstream producer start --limit 10000    # Start producer (CSV -> Kafka)
clickstream consumer stop                   # Stop consumer(s)
clickstream status                          # Verify data flow

# Cleanup
docker compose down
```

## Kafka Partitioning

To change the number of Kafka partitions, set `KAFKA_EVENTS_TOPIC_PARTITIONS` in `.env`, then restart Docker:

```bash
# Without OpenSearch
docker compose down && docker compose up -d

# With OpenSearch
docker compose --profile opensearch-enabled down && docker compose --profile opensearch-enabled up -d
```

Run benchmarks to measure throughput:

```bash
clickstream benchmark
```

## OpenSearch (Optional)

OpenSearch is disabled by default. The OpenSearch consumer uses a separate Kafka consumer group (`clickstream-opensearch`), enabling automatic backfill from the beginning of the topic.

### Enabling OpenSearch

1. Set in `.env`:
   ```bash
   OPENSEARCH_ENABLED=true
   ```

2. Start with OpenSearch profile (or restart if already running):
   ```bash
   docker compose --profile opensearch-enabled up -d
   ```

3. If consumer is running, restart to pick up the change:
   ```bash
   clickstream consumer restart
   ```

### Access Dashboards

Initialize and access the dashboards:

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

  Open dashboards at http://localhost:5601
```

**NOTE**: When prompted for tenant, select **Global**.

## Configuration

See [.env.local.example](.env.local.example) for all available configuration options.

## Cleanup

```bash
# Without OpenSearch
docker compose down -v

# With OpenSearch (must include profile to stop those containers)
docker compose --profile opensearch-enabled down -v
```

The `-v` flag removes volumes, deleting all persisted data.
