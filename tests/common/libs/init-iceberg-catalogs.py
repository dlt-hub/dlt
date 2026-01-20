#!/usr/bin/env python3
"""
Initialize Iceberg catalogs with required namespaces for testing.

This script creates the namespaces needed by test_pyiceberg.py tests:
- test_explicit_ns: Used by test_catalog_from_explicit_config_parametrized
- test_yaml_ns: Used by test_catalog_from_yaml_parametrized
- test_env_ns: Used by test_catalog_from_env_vars_parametrized

Usage:
    # From Docker (catalog-init service)
    python init-iceberg-catalogs.py

    # From host (after docker-compose up)
    python init-iceberg-catalogs.py --host
"""

import os
import sys
import time

# Determine if running inside Docker or on host
# Inside Docker: use service names (rest, postgres, minio)
# On host: use localhost
IN_DOCKER = os.path.exists("/.dockerenv") or os.environ.get("IN_DOCKER") == "1"

REST_HOST = "rest" if IN_DOCKER else "localhost"
POSTGRES_HOST = "postgres" if IN_DOCKER else "localhost"
MINIO_HOST = "minio" if IN_DOCKER else "localhost"


def wait_for_service(check_fn, service_name, max_retries=30, delay=2):
    """Wait for a service to be ready."""
    for i in range(max_retries):
        try:
            check_fn()
            print(f"✓ {service_name} is ready")
            return True
        except Exception as e:
            print(f"  Waiting for {service_name}... ({i+1}/{max_retries}): {e}")
            time.sleep(delay)
    print(f"✗ {service_name} failed to start")
    return False


def init_rest_catalog():
    """Initialize REST catalog with required namespaces."""
    from pyiceberg.catalog import load_catalog

    print("\n=== Initializing REST Catalog ===")
    print(f"  REST host: {REST_HOST}, MinIO host: {MINIO_HOST}")

    catalog = load_catalog(
        "rest_init",
        type="rest",
        uri=f"http://{REST_HOST}:8181",
        warehouse="s3://warehouse/",
        **{
            "s3.endpoint": f"http://{MINIO_HOST}:9000",
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
        },
    )

    namespaces_to_create = ["test_explicit_ns", "test_yaml_ns", "test_env_ns"]

    for ns in namespaces_to_create:
        try:
            # Pass properties to ensure namespace is properly created
            catalog.create_namespace(ns, properties={"created_by": "init-iceberg-catalogs"})
            print(f"  ✓ Created namespace: {ns}")
        except Exception as e:
            # Namespace might already exist
            if "already exists" in str(e).lower():
                print(f"  - Namespace already exists: {ns}")
            else:
                print(f"  ✗ Failed to create namespace {ns}: {e}")
                raise


def init_postgres_catalog():
    """Initialize PostgreSQL SQL catalog with required namespaces.

    IMPORTANT: PyIceberg SQL catalog stores namespaces keyed by (catalog_name, namespace).
    We must create namespaces for EACH catalog_name that tests will use.
    """
    from pyiceberg.catalog import load_catalog
    import tempfile

    print("\n=== Initializing PostgreSQL Catalog ===")
    print(f"  PostgreSQL host: {POSTGRES_HOST}")

    # Use a temp directory for warehouse (tables won't be created, just namespace metadata)
    warehouse_dir = tempfile.mkdtemp(prefix="iceberg_warehouse_")

    # Catalog names used by the tests - must match test_pyiceberg.py
    catalog_names = [
        "explicit_config_catalog",  # test_catalog_from_explicit_config_parametrized
        "yaml_catalog",  # test_catalog_from_yaml_parametrized
        "default",  # test_catalog_from_env_vars_parametrized
    ]

    namespaces_to_create = ["test_explicit_ns", "test_yaml_ns", "test_env_ns"]

    for catalog_name in catalog_names:
        print(f"\n  Creating namespaces for catalog: {catalog_name}")
        catalog = load_catalog(
            catalog_name,
            type="sql",
            uri=f"postgresql+psycopg2://loader:loader@{POSTGRES_HOST}:5432/dlt_data",
            warehouse=warehouse_dir,
        )

        for ns in namespaces_to_create:
            try:
                # Must pass at least one property for SQL catalog to create an entry
                catalog.create_namespace(ns, properties={"created_by": "init-iceberg-catalogs"})
                print(f"    ✓ Created namespace: {ns}")
            except Exception as e:
                # Namespace might already exist
                if "already exists" in str(e).lower():
                    print(f"    - Namespace already exists: {ns}")
                else:
                    print(f"    ✗ Failed to create namespace {ns}: {e}")
                    raise

        # Verify
        existing = catalog.list_namespaces()
        print(f"    Namespaces: {existing}")


def check_rest_catalog():
    """Check if REST catalog is reachable."""
    import requests

    response = requests.get(f"http://{REST_HOST}:8181/v1/config", timeout=5)
    response.raise_for_status()


def check_postgres():
    """Check if PostgreSQL is reachable."""
    import psycopg2

    conn = psycopg2.connect(
        host=POSTGRES_HOST, port=5432, user="loader", password="loader", database="dlt_data"
    )
    conn.close()


def main():
    global IN_DOCKER, REST_HOST, POSTGRES_HOST, MINIO_HOST

    # Allow override via command line
    if "--host" in sys.argv:
        IN_DOCKER = False
        REST_HOST = "localhost"
        POSTGRES_HOST = "localhost"
        MINIO_HOST = "localhost"

    print("Initializing Iceberg catalogs for testing...")
    print(f"Running {'inside Docker' if IN_DOCKER else 'on host'}")

    # Wait for services
    if not wait_for_service(check_rest_catalog, "REST Catalog"):
        sys.exit(1)

    if not wait_for_service(check_postgres, "PostgreSQL"):
        sys.exit(1)

    # Initialize catalogs
    try:
        init_rest_catalog()
        init_postgres_catalog()
        print("\n✓ All catalogs initialized successfully!")
    except Exception as e:
        print(f"\n✗ Initialization failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
