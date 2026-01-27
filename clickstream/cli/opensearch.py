# ==============================================================================
# OpenSearch Commands
# ==============================================================================
"""
OpenSearch commands for the clickstream pipeline CLI.

Commands for managing OpenSearch dashboards and visualizations.
"""

import typer

from clickstream.cli.shared import C, I, get_project_root
from clickstream.utils.config import get_settings


# ==============================================================================
# Commands
# ==============================================================================


def opensearch_init() -> None:
    """Initialize OpenSearch index pattern and import dashboards.

    Imports pre-built visualizations and dashboards for clickstream analytics.
    Requires the clickstream-events index to exist with data.

    Run 'clickstream consumer start' and 'clickstream producer start' first.
    """
    import warnings

    import requests
    import urllib3

    # Suppress SSL warnings for local development (verify_certs=False)
    warnings.filterwarnings("ignore", category=UserWarning, module="opensearchpy")
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    settings = get_settings()

    print()
    print(f"  {C.BOLD}Initializing OpenSearch{C.RESET}")

    # Check if OpenSearch is enabled
    if not settings.opensearch.enabled:
        print(f"  {C.BRIGHT_RED}{I.CROSS}{C.RESET} OpenSearch is not enabled")
        print(f"    Set {C.WHITE}OPENSEARCH_ENABLED=true{C.RESET} in .env")
        raise typer.Exit(1)

    # Check if OpenSearch is reachable
    try:
        from opensearchpy import OpenSearch

        client = OpenSearch(
            hosts=settings.opensearch.hosts,
            http_auth=(settings.opensearch.user, settings.opensearch.password),
            use_ssl=settings.opensearch.use_ssl,
            verify_certs=settings.opensearch.verify_certs,
            timeout=10,
        )
        client.info()
        print(f"  {C.BRIGHT_GREEN}{I.CHECK}{C.RESET} OpenSearch is reachable")
    except Exception as e:
        print(f"  {C.BRIGHT_RED}{I.CROSS}{C.RESET} OpenSearch is not reachable")
        print(f"    {C.DIM}{e}{C.RESET}")
        raise typer.Exit(1)

    # Check if index exists and has data
    index_name = settings.opensearch.events_index
    try:
        if not client.indices.exists(index=index_name):
            print(f"  {C.BRIGHT_RED}{I.CROSS}{C.RESET} Index '{index_name}' does not exist")
            print(
                f"    Run '{C.WHITE}clickstream consumer start{C.RESET}' and "
                f"'{C.WHITE}clickstream producer start{C.RESET}' first"
            )
            raise typer.Exit(1)

        # Check document count
        count_result = client.count(index=index_name)
        doc_count = count_result.get("count", 0)
        if doc_count > 0:
            print(
                f"  {C.BRIGHT_GREEN}{I.CHECK}{C.RESET} Index '{index_name}' exists ({doc_count:,} documents)"
            )
        else:
            print(f"  {C.BRIGHT_GREEN}{I.CHECK}{C.RESET} Index '{index_name}' exists (empty)")
    except typer.Exit:
        raise
    except Exception as e:
        print(f"  {C.BRIGHT_RED}{I.CROSS}{C.RESET} Failed to check index '{index_name}'")
        print(f"    {C.DIM}{e}{C.RESET}")
        raise typer.Exit(1)

    # Locate the dashboards file
    project_root = get_project_root()
    dashboards_file = project_root / "opensearch" / "clickstream-dashboards.ndjson"
    if not dashboards_file.exists():
        print(f"  {C.BRIGHT_RED}{I.CROSS}{C.RESET} Dashboards file not found")
        print(f"    Expected: {dashboards_file}")
        raise typer.Exit(1)

    # Build the Dashboards API URL
    if settings.opensearch.host == "localhost" or settings.opensearch.host == "127.0.0.1":
        # Local Docker: Dashboards on separate container, port 5601, no SSL
        dashboards_base_url = "http://localhost:5601"
    else:
        # Aiven: Dashboards API on same host, port 443
        dashboards_base_url = f"https://{settings.opensearch.host}:443"

    import_url = f"{dashboards_base_url}/api/saved_objects/_import?overwrite=true"

    # Import the dashboards
    try:
        with open(dashboards_file, "rb") as f:
            # OpenSearch Dashboards expects multipart form data
            files = {"file": ("clickstream-dashboards.ndjson", f, "application/ndjson")}
            headers = {
                "osd-xsrf": "true",  # Required by OpenSearch Dashboards
                "securitytenant": "global",  # Import to Global tenant
            }

            response = requests.post(
                import_url,
                files=files,
                headers=headers,
                auth=(settings.opensearch.user, settings.opensearch.password),
                verify=settings.opensearch.verify_certs,
                timeout=30,
            )

        if response.status_code != 200:
            print(f"  {C.BRIGHT_RED}{I.CROSS}{C.RESET} Failed to import dashboards")
            print(f"    Status: {response.status_code}")
            print(f"    Response: {response.text[:500]}")
            raise typer.Exit(1)

        # Parse the response to count imported objects
        result = response.json()
        success = result.get("success", False)
        success_count = result.get("successCount", 0)
        success_results = result.get("successResults", [])

        if not success and success_count == 0:
            # Import failed
            errors = result.get("errors", [])
            print(f"  {C.BRIGHT_RED}{I.CROSS}{C.RESET} Failed to import dashboards")
            for error in errors[:3]:
                error_type = error.get("error", {}).get("type", "unknown")
                obj_id = error.get("id", "unknown")
                print(f"    {error_type}: {obj_id}")
            raise typer.Exit(1)

        # Count objects by type
        imported_counts: dict[str, int] = {}
        for obj in success_results:
            obj_type = obj.get("type", "unknown")
            imported_counts[obj_type] = imported_counts.get(obj_type, 0) + 1

        # Report what was imported
        if "index-pattern" in imported_counts:
            print(f"  {C.BRIGHT_GREEN}{I.CHECK}{C.RESET} Imported index pattern: {index_name}*")

        viz_count = imported_counts.get("visualization", 0)
        if viz_count > 0:
            print(f"  {C.BRIGHT_GREEN}{I.CHECK}{C.RESET} Imported {viz_count} visualizations")

        dash_count = imported_counts.get("dashboard", 0)
        if dash_count > 0:
            print(f"  {C.BRIGHT_GREEN}{I.CHECK}{C.RESET} Imported {dash_count} dashboards")

    except requests.exceptions.Timeout:
        print(f"  {C.BRIGHT_RED}{I.CROSS}{C.RESET} Connection to OpenSearch Dashboards timed out")
        print(f"    URL: {C.DIM}{import_url}{C.RESET}")
        print(f"    Check that the service is running and accessible")
        raise typer.Exit(1)
    except requests.exceptions.ConnectionError:
        print(f"  {C.BRIGHT_RED}{I.CROSS}{C.RESET} Could not connect to OpenSearch Dashboards")
        print(f"    URL: {C.DIM}{import_url}{C.RESET}")
        print(f"    Check that the service is running and accessible")
        raise typer.Exit(1)
    except requests.exceptions.RequestException as e:
        print(f"  {C.BRIGHT_RED}{I.CROSS}{C.RESET} Failed to import dashboards")
        # Extract just the error type/message, not the full traceback
        error_msg = str(e).split(":")[-1].strip() if ":" in str(e) else str(e)
        print(f"    {C.DIM}{error_msg}{C.RESET}")
        raise typer.Exit(1)

    # Show the dashboards URL
    print()
    if settings.opensearch.host == "localhost" or settings.opensearch.host == "127.0.0.1":
        # Local Docker: Dashboards on port 5601
        dashboard_url = "http://localhost:5601"
    else:
        # Aiven: Show login page URL
        dashboard_url = f"https://{settings.opensearch.host}/app/login"
    print(f"  Open dashboards at {C.WHITE}{dashboard_url}{C.RESET}")
    print()
