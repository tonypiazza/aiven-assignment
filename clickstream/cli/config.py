# ==============================================================================
# Config Commands
# ==============================================================================
"""
Configuration management commands for the clickstream pipeline CLI.

Commands for showing and generating configuration files.
"""

import re
from datetime import datetime
from pathlib import Path
from typing import Annotated

import typer

from clickstream.cli.shared import (
    C,
    I,
    get_project_root,
)
from clickstream.utils.config import get_settings


# ==============================================================================
# Commands
# ==============================================================================


def config_show() -> None:
    """Display current configuration (hides secrets)."""
    settings = get_settings()

    print()
    print(f"{C.BOLD}Configuration{C.RESET}")
    print()

    # Kafka
    print(f"{C.CYAN}Kafka{C.RESET}")
    servers = settings.kafka.bootstrap_servers.split(",")
    for i, server in enumerate(servers):
        label = "  Bootstrap:  " if i == 0 else "              "
        print(f"{label}{C.WHITE}{server.strip()}{C.RESET}")
    kafka_ssl = (
        "mTLS (client certificates)" if settings.kafka.security_protocol == "SSL" else "disabled"
    )
    print(f"  SSL:        {C.WHITE}{kafka_ssl}{C.RESET}")
    topic_info = (
        f"{settings.kafka.events_topic} ({settings.kafka.events_topic_partitions} partitions)"
    )
    print(f"  Topic:      {C.WHITE}{topic_info}{C.RESET}")
    print()

    # PostgreSQL
    print(f"{C.CYAN}PostgreSQL{C.RESET}")
    print(f"  Host:       {C.WHITE}{settings.postgres.host}{C.RESET}")
    print(f"  Port:       {C.WHITE}{settings.postgres.port}{C.RESET}")
    print(f"  Database:   {C.WHITE}{settings.postgres.database}{C.RESET}")
    print(f"  Schema:     {C.WHITE}{settings.postgres.schema_name}{C.RESET}")
    print(f"  User:       {C.WHITE}{settings.postgres.user}{C.RESET}")
    print(f"  SSL:        {C.WHITE}{settings.postgres.sslmode}{C.RESET}")
    print()

    # OpenSearch
    print(f"{C.CYAN}OpenSearch{C.RESET}")
    print(f"  Host:       {C.WHITE}{settings.opensearch.host}{C.RESET}")
    print(f"  Port:       {C.WHITE}{settings.opensearch.port}{C.RESET}")
    opensearch_ssl = "enabled" if settings.opensearch.use_ssl else "disabled"
    print(f"  SSL:        {C.WHITE}{opensearch_ssl}{C.RESET}")
    print(f"  User:       {C.WHITE}{settings.opensearch.user}{C.RESET}")
    print(f"  Password:   {C.WHITE}{settings.opensearch.password}{C.RESET}")
    print(f"  Index:      {C.WHITE}{settings.opensearch.events_index}{C.RESET}")
    if settings.opensearch.use_ssl:
        # Aiven: Dashboards login page
        dashboard_url = f"https://{settings.opensearch.host}/app/login"
    else:
        # Local Docker: Dashboards on separate container, port 5601
        dashboard_url = f"http://{settings.opensearch.host}:5601"
    print(f"  Dashboard:  {C.WHITE}{dashboard_url}{C.RESET}")
    print()

    # Valkey
    print(f"{C.CYAN}Valkey{C.RESET}")
    print(f"  Host:       {C.WHITE}{settings.valkey.host}{C.RESET}")
    print(f"  Port:       {C.WHITE}{settings.valkey.port}{C.RESET}")
    valkey_ssl = "enabled" if settings.valkey.ssl else "disabled"
    print(f"  SSL:        {C.WHITE}{valkey_ssl}{C.RESET}")
    print()

    # Producer
    print(f"{C.CYAN}Producer{C.RESET}")
    print(f"  Data File:  {C.WHITE}{settings.producer.data_file}{C.RESET}")
    print()

    # Consumer
    print(f"{C.CYAN}Consumer{C.RESET}")
    print(f"  Group ID:   {C.WHITE}{settings.consumer.group_id}{C.RESET}")
    print(f"  Timeout:    {C.WHITE}{settings.consumer.session_timeout_minutes} minutes{C.RESET}")
    print()


def config_generate(
    terraform_dir: Annotated[
        Path,
        typer.Option(
            "--terraform-dir",
            "-t",
            help="Path to terraform directory",
        ),
    ] = Path("terraform"),
    output_file: Annotated[
        Path,
        typer.Option(
            "--output",
            "-o",
            help="Output .env file path",
        ),
    ] = Path(".env"),
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            "-f",
            help="Overwrite existing .env file without prompting",
        ),
    ] = False,
) -> None:
    """Generate .env file from Terraform outputs.

    Reads Terraform outputs and generates a .env file with all the
    configuration needed to run the clickstream pipeline against
    Aiven managed services.

    Prerequisites:
        1. Run 'terraform apply' in the terraform directory first
        2. Terraform state must be accessible

    Examples:
        clickstream config generate                    # Generate .env from ./terraform
        clickstream config generate -t /path/to/tf    # Custom terraform directory
        clickstream config generate -o .env.aiven     # Custom output file
        clickstream config generate -f                # Overwrite without prompting
    """
    import json as json_module

    print()
    print(f"  {C.BOLD}Generating configuration from Terraform{C.RESET}")

    # Resolve terraform directory
    project_root = get_project_root()
    if not terraform_dir.is_absolute():
        terraform_dir = project_root / terraform_dir

    # Check terraform directory exists
    if not terraform_dir.exists():
        print(f"{C.BRIGHT_RED}{I.CROSS} Terraform directory not found: {terraform_dir}{C.RESET}")
        raise typer.Exit(1)

    # Check for terraform state
    state_file = terraform_dir / "terraform.tfstate"
    if not state_file.exists():
        print(f"{C.BRIGHT_RED}{I.CROSS} Terraform state not found{C.RESET}")
        print(f"    Run '{C.WHITE}terraform apply{C.RESET}' first in {terraform_dir}")
        raise typer.Exit(1)

    print(f"  {C.BRIGHT_GREEN}{I.CHECK}{C.RESET} Found Terraform state")

    # Read terraform state file directly (no CLI dependency)
    try:
        with open(state_file) as f:
            state = json_module.load(f)
        outputs = state.get("outputs", {})
    except json_module.JSONDecodeError as e:
        print(f"{C.BRIGHT_RED}{I.CROSS} Failed to parse Terraform state{C.RESET}")
        print(f"    {C.DIM}{e}{C.RESET}")
        raise typer.Exit(1)
    except PermissionError:
        print(f"{C.BRIGHT_RED}{I.CROSS} Permission denied reading Terraform state{C.RESET}")
        raise typer.Exit(1)

    print(f"  {C.BRIGHT_GREEN}{I.CHECK}{C.RESET} Read Terraform outputs")

    # Extract values from terraform output structure
    def get_output(key: str, default: str = "") -> str:
        """Get value from terraform output, handling the nested structure."""
        if key in outputs:
            value = outputs[key].get("value")
            if value is None:
                return default
            # Handle list values (e.g., kafka uris) by joining with comma
            if isinstance(value, list):
                return ",".join(str(v) for v in value)
            return str(value)
        return default

    # Check for required outputs
    required_outputs = ["kafka_bootstrap_servers", "pg_host", "valkey_host"]
    missing = [key for key in required_outputs if not get_output(key)]
    if missing:
        print(
            f"{C.BRIGHT_RED}{I.CROSS} Missing required Terraform outputs: {', '.join(missing)}{C.RESET}"
        )
        print(f"    Ensure 'terraform apply' completed successfully")
        raise typer.Exit(1)

    # Read terraform.tfvars for topic configuration (if exists)
    tfvars_file = terraform_dir / "terraform.tfvars"
    kafka_topic = "clickstream-events"
    kafka_partitions = "3"
    pg_database = "clickstream"
    opensearch_enabled_tf = False
    aiven_api_token = ""

    if tfvars_file.exists():
        try:
            tfvars_content = tfvars_file.read_text()
            # Simple parsing for the values we need
            for line in tfvars_content.split("\n"):
                line = line.strip()
                if line.startswith("kafka_events_topic_name"):
                    match = re.search(r'=\s*"([^"]+)"', line)
                    if match:
                        kafka_topic = match.group(1)
                elif line.startswith("kafka_events_topic_partitions"):
                    match = re.search(r"=\s*(\d+)", line)
                    if match:
                        kafka_partitions = match.group(1)
                elif line.startswith("pg_database_name"):
                    match = re.search(r'=\s*"([^"]+)"', line)
                    if match:
                        pg_database = match.group(1)
                elif line.startswith("opensearch_enabled"):
                    opensearch_enabled_tf = "true" in line.lower()
                elif line.startswith("aiven_api_token"):
                    match = re.search(r'=\s*"([^"]+)"', line)
                    if match:
                        aiven_api_token = match.group(1)
        except Exception:
            pass  # Use defaults if parsing fails

    # OpenSearch enabled is determined by terraform.tfvars setting
    opensearch_host = get_output("opensearch_host")
    opensearch_enabled = opensearch_enabled_tf

    # Validate: if OpenSearch is enabled in tfvars, we must have connection details
    if opensearch_enabled and not opensearch_host:
        print(
            f"{C.BRIGHT_RED}{I.CROSS} OpenSearch is enabled in terraform.tfvars but no connection details found{C.RESET}"
        )
        print(f"    Ensure 'terraform apply' completed successfully")
        raise typer.Exit(1)

    # Write SSL certificate files for Aiven mTLS authentication
    certs_dir = project_root / "certs"
    certs_dir.mkdir(exist_ok=True)

    # CA certificate for server verification
    ca_cert = get_output("project_ca_cert")
    ca_cert_file = ""
    if ca_cert:
        ca_cert_path = certs_dir / "aiven-ca.pem"
        ca_cert_path.write_text(ca_cert)
        ca_cert_file = str(ca_cert_path.relative_to(project_root))
        print(f"  {C.BRIGHT_GREEN}{I.CHECK}{C.RESET} Wrote CA certificate to {ca_cert_file}")

    # Client certificate for mTLS authentication
    client_cert = get_output("kafka_access_cert")
    client_cert_file = ""
    if client_cert:
        client_cert_path = certs_dir / "aiven-client.pem"
        client_cert_path.write_text(client_cert)
        client_cert_file = str(client_cert_path.relative_to(project_root))
        print(
            f"  {C.BRIGHT_GREEN}{I.CHECK}{C.RESET} Wrote client certificate to {client_cert_file}"
        )

    # Client key for mTLS authentication
    client_key = get_output("kafka_access_key")
    client_key_file = ""
    if client_key:
        client_key_path = certs_dir / "aiven-client.key"
        client_key_path.write_text(client_key)
        # Set restrictive permissions on private key
        client_key_path.chmod(0o600)
        client_key_file = str(client_key_path.relative_to(project_root))
        print(f"  {C.BRIGHT_GREEN}{I.CHECK}{C.RESET} Wrote client key to {client_key_file}")

    # Build .env content
    env_lines = [
        "# ==============================================================================",
        "# Clickstream Pipeline - Aiven Configuration",
        "# ==============================================================================",
        f"# Generated from Terraform outputs on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "# ==============================================================================",
        "",
        "# ==============================================================================",
        "# Kafka Settings (Aiven)",
        "# ==============================================================================",
        f"KAFKA_BOOTSTRAP_SERVERS={get_output('kafka_bootstrap_servers')}",
        "# Aiven Kafka uses SSL with client certificates (mTLS), not SASL",
        "KAFKA_SECURITY_PROTOCOL=SSL",
        f"KAFKA_SSL_CA_FILE={ca_cert_file}" if ca_cert_file else "# KAFKA_SSL_CA_FILE=",
        f"KAFKA_SSL_CERT_FILE={client_cert_file}" if client_cert_file else "# KAFKA_SSL_CERT_FILE=",
        f"KAFKA_SSL_KEY_FILE={client_key_file}" if client_key_file else "# KAFKA_SSL_KEY_FILE=",
        "",
        "# Must match terraform.tfvars",
        f"KAFKA_EVENTS_TOPIC={kafka_topic}",
        f"KAFKA_EVENTS_TOPIC_PARTITIONS={kafka_partitions}",
        "",
        "# ==============================================================================",
        "# PostgreSQL Settings (Aiven)",
        "# ==============================================================================",
        f"PG_HOST={get_output('pg_host')}",
        f"PG_PORT={get_output('pg_port')}",
        f"PG_USER={get_output('pg_user')}",
        f"PG_PASSWORD={get_output('pg_password')}",
        "",
        "# Must match terraform.tfvars",
        f"PG_DATABASE={pg_database}",
        "PG_SCHEMA_NAME=clickstream",
        "PG_SSLMODE=require",
        "",
        "# ==============================================================================",
        "# OpenSearch Settings (Aiven) - Optional",
        "# ==============================================================================",
        f"OPENSEARCH_ENABLED={'true' if opensearch_enabled else 'false'}",
    ]

    if opensearch_enabled and opensearch_host:
        env_lines.extend(
            [
                f"OPENSEARCH_HOST={opensearch_host}",
                f"OPENSEARCH_PORT={get_output('opensearch_port', '443')}",
                f"OPENSEARCH_USER={get_output('opensearch_user', 'avnadmin')}",
                f"OPENSEARCH_PASSWORD={get_output('opensearch_password')}",
                "OPENSEARCH_USE_SSL=true",
                "OPENSEARCH_VERIFY_CERTS=true",
            ]
        )
    else:
        env_lines.extend(
            [
                "# OPENSEARCH_HOST=",
                "# OPENSEARCH_PORT=443",
                "# OPENSEARCH_USER=avnadmin",
                "# OPENSEARCH_PASSWORD=",
                "# OPENSEARCH_USE_SSL=true",
                "# OPENSEARCH_VERIFY_CERTS=true",
            ]
        )

    env_lines.extend(
        [
            "OPENSEARCH_EVENTS_INDEX=clickstream-events",
            "OPENSEARCH_CONSUMER_GROUP_ID=clickstream-opensearch",
            "",
            "# ==============================================================================",
            "# Valkey Settings (Aiven - Redis-compatible)",
            "# ==============================================================================",
            f"VALKEY_HOST={get_output('valkey_host')}",
            f"VALKEY_PORT={get_output('valkey_port')}",
            f"VALKEY_PASSWORD={get_output('valkey_password')}",
            "VALKEY_SSL=true",
            "VALKEY_SESSION_TTL_HOURS=24",
            "",
            "# ==============================================================================",
            "# Producer Settings",
            "# ==============================================================================",
            "PRODUCER_DATA_FILE=data/events.csv",
            "",
            "# ==============================================================================",
            "# Consumer Settings",
            "# ==============================================================================",
            "POSTGRESQL_CONSUMER_GROUP_ID=clickstream-postgresql",
            "CONSUMER_SESSION_TIMEOUT_MINUTES=30",
            "CONSUMER_AUTO_OFFSET_RESET=earliest",
            "",
            "# ==============================================================================",
            "# Aiven API Settings (enables fast status checks)",
            "# ==============================================================================",
            "# When configured, 'clickstream status' uses the Aiven API for service checks",
            "# instead of direct connections. This is much faster (~2s vs ~30s for Kafka).",
        ]
    )

    if aiven_api_token:
        env_lines.extend(
            [
                f"AIVEN_API_TOKEN={aiven_api_token}",
                f"AIVEN_PROJECT_NAME={get_output('project_name')}",
                "# Service names are derived automatically: {project}-kafka, {project}-pg, etc.",
                "# Uncomment to override:",
                "# AIVEN_KAFKA_SERVICE_NAME=",
                "# AIVEN_PG_SERVICE_NAME=",
                "# AIVEN_OPENSEARCH_SERVICE_NAME=",
                "# AIVEN_VALKEY_SERVICE_NAME=",
            ]
        )
    else:
        env_lines.extend(
            [
                "# AIVEN_API_TOKEN=",
                "# AIVEN_PROJECT_NAME=",
            ]
        )

    env_lines.append("")

    env_content = "\n".join(env_lines)

    # Check if output file exists
    if not output_file.is_absolute():
        output_file = project_root / output_file

    if output_file.exists() and not force:
        print(f"  {C.BRIGHT_YELLOW}{I.WARN}{C.RESET} File already exists: {output_file}")
        if not typer.confirm("    Overwrite?"):
            print(f"  {C.DIM}Aborted{C.RESET}")
            raise typer.Exit(0)

    # Write the file
    output_file.write_text(env_content)
    print(f"  {C.BRIGHT_GREEN}{I.CHECK}{C.RESET} Generated {output_file}")

    # Summary
    print()
    print(f"  {C.BOLD}Configuration Summary{C.RESET}")
    print(
        f"    Kafka:      {C.WHITE}{get_output('kafka_bootstrap_servers').split(',')[0]}{C.RESET}"
    )
    print(f"    PostgreSQL: {C.WHITE}{get_output('pg_host')}{C.RESET}")
    print(f"    Valkey:     {C.WHITE}{get_output('valkey_host')}{C.RESET}")
    if opensearch_enabled and opensearch_host:
        print(f"    OpenSearch: {C.WHITE}{opensearch_host}{C.RESET}")
    else:
        print(f"    OpenSearch: {C.DIM}disabled{C.RESET}")

    print()
    print(f"  Next steps:")
    print(f"    1. {C.WHITE}clickstream consumer start{C.RESET}")
    print(f"    2. {C.WHITE}clickstream producer start --limit 10000{C.RESET}")
    print()
