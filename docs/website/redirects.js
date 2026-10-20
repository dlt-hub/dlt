// Redirect rules shared between the Cloudflare worker (worker.ts) and the
// post-build verification script (scripts/verify-redirects.js).

/** @type {Array<{from: string, to: string}>} */
const REDIRECTS = [
  // basic root redirects
  {
    // NOTE: We only ever hit the root path on dev previews, so we can redirect to the devel version
    from: "/",
    to: "/docs/devel/intro",
  },
  {
    from: "/docs",
    to: "/docs/intro",
  },
  {
    from: "/docs/",
    to: "/docs/intro",
  },
  {
    from: "/docs/hub",
    to: "/docs/hub/getting-started/introduction",
  },

  // docs section redirects
  {
    from: "/docs/getting-started",
    to: "/docs/intro/",
  },
  {
    from: "/docs/dlt-ecosystem",
    to: "/docs/dlt-ecosystem/verified-sources/",
  },
  {
    from: "/docs/general-usage/credentials/config_providers",
    to: "/docs/general-usage/credentials/setup/",
  },
  {
    from: "/docs/general-usage/credentials/configuration",
    to: "/docs/general-usage/credentials/setup/",
  },
  {
    from: "/docs/general-usage/credentials/config_specs",
    to: "/docs/general-usage/credentials/complex_types/",
  },
  // tutorial redirects
  {
    from: "/docs/tutorial/intro",
    to: "/docs/tutorial/load-data-from-an-api/",
  },
  {
    from: "/docs/tutorial/grouping-resources",
    to: "/docs/tutorial/load-data-from-an-api/",
  },

  // reference + misc redirects
  {
    from: "/docs/telemetry",
    to: "/docs/reference/telemetry/",
  },
  {
    from: "/docs/walkthroughs",
    to: "/docs/intro/",
  },
  {
    from: "/docs/visualizations",
    to: "/docs/general-usage/dataset-access/dataset",
  },
  {
    from: "/docs/general-usage/dataset-access",
    to: "/docs/general-usage/dataset-access/dataset",
  },

  // top-404 redirects
  {
    from: "/docs/dlt-ecosystem/llm-tooling/cursor-restapi",
    to: "/docs/hub/ingestion/rest-api-source",
  },
  {
    from: "/docs/general-usage/filesystem",
    to: "/docs/dlt-ecosystem/verified-sources/filesystem/",
  },
  {
    from: "/docs/walkthroughs/",
    to: "/docs/intro/",
  },
  {
    from: "/docs/dlt-ecosystem/verified-sources/rest_api/reference",
    to: "/docs/dlt-ecosystem/verified-sources/rest_api",
  },

  // renamed / relocated pages
  {
    from: "/docs/walkthroughs/load-data-from-an-api",
    to: "/docs/tutorial/load-data-from-an-api",
  },
  {
    from: "/docs/walkthroughs/create-a-pipeline",
    to: "/docs/tutorial/load-data-from-an-api",
  },
  {
    from: "/docs/dlt-ecosystem/table-formats/delta",
    to: "/docs/dlt-ecosystem/destinations/delta-iceberg",
  },
  {
    from: "/docs/dlt-ecosystem/file-formats/parquet",
    to: "/docs/dlt-ecosystem/file-formats#parquet",
  },
  {
    from: "/docs/dlt-ecosystem/file-formats/jsonl",
    to: "/docs/dlt-ecosystem/file-formats#jsonl",
  },
  {
    from: "/docs/dlt-ecosystem/file-formats/csv",
    to: "/docs/dlt-ecosystem/file-formats#csv",
  },
  {
    from: "/docs/dlt-ecosystem/file-formats/insert-format",
    to: "/docs/dlt-ecosystem/file-formats#sql-insert",
  },
  {
    from: "/docs/running-in-production/alerting",
    to: "/docs/running-in-production/running#using-slack-to-send-messages",
  },
  {
    from: "/docs/running-in-production/monitoring",
    to: "/docs/running-in-production/running",
  },
  {
    from: "/docs/running-in-production/tracing",
    to: "/docs/running-in-production/running",
  },
  {
    from: "/docs/general-usage/dataset-access/sql-client",
    to: "/docs/dlt-ecosystem/transformations/sql",
  },
  {
    from: "/docs/general-usage/dataset-access/ibis-backend",
    to: "/docs/dlt-ecosystem/transformations/python#using-ibis",
  },
  {
    from: "/docs/dlt-ecosystem/table-formats/iceberg",
    to: "/docs/dlt-ecosystem/destinations/delta-iceberg",
  },
  {
    from: "/docs/load-data-from-an-api",
    to: "/docs/tutorial/load-data-from-an-api",
  },
  {
    from: "/docs/destinations/snowflake",
    to: "/docs/dlt-ecosystem/destinations/snowflake",
  },
  {
    from: "/docs/destinations/duckdb",
    to: "/docs/dlt-ecosystem/destinations/duckdb",
  },
  {
    from: "/docs/pipelines/salesforce",
    to: "/docs/dlt-ecosystem/verified-sources/salesforce",
  },
  {
    from: "/docs/dlt-ecosystem/verified-sources/stripe_analytics",
    to: "/docs/dlt-ecosystem/verified-sources/stripe",
  },
  {
    from: "/docs/dlt-ecosystem/transformations/pandas",
    to: "/docs/dlt-ecosystem/transformations/python",
  },
  {
    from: "/docs/getting-started/build-a-data-pipeline",
    to: "/docs/tutorial/load-data-from-an-api",
  },
  {
    from: "/docs/build-a-pipeline-tutorial",
    to: "/docs/tutorial/load-data-from-an-api",
  },

  // api_reference paths gained a /dlt/ prefix
  {
    from: "/docs/api_reference/extract/resource",
    to: "/docs/api_reference/dlt/extract/resource",
  },
  {
    from: "/docs/api_reference/common/configuration/specs/base_configuration",
    to: "/docs/api_reference/dlt/common/configuration/specs/base_configuration",
  },
  {
    from: "/docs/hub/reference",
    to: "/docs/hub/getting-started/introduction",
  },
  {
    from: "/docs/hub/intro",
    to: "/docs/hub/getting-started/introduction",
  },
  {
    from: "/docs/general-usage/connectors",
    to: "/docs/dlt-ecosystem/verified-sources/",
  },
  {
    from: "/docs/api_reference/pipeline/configuration",
    to: "/docs/general-usage/credentials/",
  },
  {
    from: "/docs/walkthroughs/grouping-resources",
    to: "/docs/general-usage/source",
  },
  {
    from: "/docs/general-usage/dlt",
    to: "/docs/intro",
  },

  // hub restructure (2026-05-20): legacy hub paths → new category folders
  {
    from: "/docs/hub/introduction",
    to: "/docs/hub/getting-started/introduction",
  },
  {
    from: "/docs/hub/oss-and-dlthub",
    to: "/docs/hub/getting-started/oss-and-dlthub",
  },
  {
    from: "/docs/hub/getting-started/runtime-tutorial",
    to: "/docs/hub/pipeline-operations/deployments",
  },
  {
    from: "/docs/hub/getting-started/platform-tutorial",
    to: "/docs/hub/pipeline-operations/deployments",
  },
  {
    from: "/docs/hub/workspace/overview",
    to: "/docs/hub/getting-started/installation",
  },
  {
    from: "/docs/hub/ingestion/workspace",
    to: "/docs/hub/getting-started/installation",
  },
  {
    from: "/docs/hub/workspace/init",
    to: "/docs/hub/ingestion/init",
  },
  {
    from: "/docs/hub/workspace/dashboard",
    to: "/docs/hub/ingestion/dashboard",
  },
  {
    from: "/docs/hub/ecosystem/ms-sql",
    to: "/docs/hub/ingestion/ms-sql",
  },
  {
    from: "/docs/hub/ecosystem/delta",
    to: "/docs/hub/ingestion/delta",
  },
  {
    from: "/docs/hub/ecosystem/iceberg",
    to: "/docs/hub/ingestion/iceberg",
  },
  {
    from: "/docs/hub/ecosystem/snowflake_plus",
    to: "/docs/hub/ingestion/snowflake-plus",
  },
  {
    from: "/docs/hub/features/transformations",
    to: "/docs/hub/transformations",
  },
  {
    from: "/docs/hub/features/transformations/dbt-transformations",
    to: "/docs/hub/transformations/dbt-transformations",
  },
  {
    from: "/docs/hub/core-concepts/profiles-dlthub",
    to: "/docs/hub/pipeline-operations/profiles",
  },
  {
    from: "/docs/hub/runtime/overview",
    to: "/docs/hub/pipeline-operations/overview",
  },
  {
    from: "/docs/hub/runtime/workspace-setup",
    to: "/docs/hub/pipeline-operations/workspace-setup",
  },
  {
    from: "/docs/hub/runtime/deploying",
    to: "/docs/hub/pipeline-operations/deployments",
  },
  {
    from: "/docs/hub/runtime/triggers",
    to: "/docs/hub/pipeline-operations/triggers",
  },
  {
    from: "/docs/hub/runtime/job-configuration",
    to: "/docs/hub/pipeline-operations/job-configuration",
  },
  {
    from: "/docs/hub/runtime/monitor-and-debug",
    to: "/docs/hub/pipeline-operations/monitoring",
  },
  {
    from: "/docs/hub/runtime/regions",
    to: "/docs/hub/platform-capabilities/regions",
  },
  {
    from: "/docs/hub/runtime/users-and-roles",
    to: "/docs/hub/platform-capabilities/users-and-roles",
  },
  {
    from: "/docs/hub/runtime/settings",
    to: "/docs/hub/platform-capabilities/settings",
  },
  {
    from: "/docs/hub/features/quality/data-quality",
    to: "/docs/hub/data-quality",
  },
  {
    from: "/docs/hub/features/quality/advanced",
    to: "/docs/hub/data-quality/advanced",
  },
  {
    from: "/docs/hub/core-concepts/datasets",
    to: "/docs/hub/data-discovery/datasets",
  },
  {
    from: "/docs/dlt-ecosystem/llm-tooling/llm-native-workflow",
    to: "/docs/hub/ingestion/rest-api-source",
  },
  {
    from: "/docs/dlt-ecosystem/llm-tooling/explore-and-transform",
    to: "/docs/hub/transformations/explore-and-transform",
  },
  // Removed pages that external sites still link to (public backlink crawl,
  // Sept 2026: rest-client alone is linked from ~100 GitHub repos, many via
  // the DataTalksClub zoomcamp). Without these they answer 404.
  {
    from: "/docs/general-usage/http/rest-client",
    to: "/docs/dlt-ecosystem/verified-sources/rest_api/advanced#restclient",
  },
  {
    from: "/docs/general-usage/http/requests",
    to: "/docs/dlt-ecosystem/verified-sources/rest_api/advanced#requests-wrapper",
  },
  {
    from: "/docs/general-usage/http/overview",
    to: "/docs/dlt-ecosystem/verified-sources/rest_api/advanced#restclient",
  },
  {
    from: "/docs/general-usage/dashboard",
    to: "/docs/hub/ingestion/dashboard",
  },
  {
    // the reference section landing page - the API reference has its own entry below
    from: "/docs/reference",
    to: "/docs/reference/command-line-interface",
  },
  {
    from: "/docs/api_reference",
    to: "/docs/api_reference/dlt/__init__",
  },
  {
    from: "/docs/api_reference/dlt",
    to: "/docs/api_reference/dlt/__init__",
  },
  {
    from: "/docs/api_reference/sources/sql_database/__init__",
    to: "/docs/api_reference/dlt/sources/sql_database/__init__",
  },
  {
    from: "/docs/api_reference/helpers/dbt/runner",
    to: "/docs/api_reference/dlt/helpers/dbt/runner",
  },
  {
    from: "/docs/walkthroughs/add-a-verified-source",
    to: "/docs/dlt-ecosystem/verified-sources",
  },
  {
    from: "/docs/dlt-ecosystem/verified-sources/filesystem/basic",
    to: "/docs/dlt-ecosystem/verified-sources/filesystem",
  },
  {
    from: "/docs/performance",
    to: "/docs/reference/performance",
  },
  {
    from: "/docs/general-usage/performance",
    to: "/docs/reference/performance",
  },
  {
    from: "/docs/reference/explainers/airflow-gcp-cloud-composer",
    to: "/docs/walkthroughs/deploy-a-pipeline/deploy-with-airflow-composer",
  },
  {
    from: "/docs/walkthroughs/deploy-a-pipeline/deploy-gcp-cloud-function-as-webhook",
    to: "/docs/walkthroughs/deploy-a-pipeline/deploy-with-google-cloud-functions",
  },
  {
    from: "/docs/general-usage/dataset-access/transformations",
    to: "/docs/dlt-ecosystem/transformations",
  },
  {
    from: "/docs/dlt-ecosystem/transformations/dbt/dbt_cloud",
    to: "/docs/dlt-ecosystem/transformations/dbt#dbt-cloud",
  },
  {
    from: "/docs/general-usage/dataset-access/",
    to: "/docs/general-usage/dataset-access/dataset",
  },
  {
    from: "/docs/hub/features/ai",
    to: "/docs/hub/ai-harness/introduction",
  },
  {
    from: "/docs/hub/features/mcp-server",
    to: "/docs/hub/ai-harness/introduction",
  },
  {
    from: "/docs/plus/features/ai",
    to: "/docs/hub/ai-harness/introduction",
  },
  {
    from: "/docs/guides/schema-management",
    to: "/docs/general-usage/schema-evolution",
  },
  {
    from: "/docs/dlt-ecosystem/visualizations/understanding-the-tables",
    to: "/docs/general-usage/destination-tables",
  },
  // pages that moved in earlier docs restructures and currently end on the
  // not-found page (backlink crawl, Sept 2026)
  {
    from: "/docs/general-usage",
    to: "/docs/general-usage/source",
  },
  {
    from: "/docs/general-usage/dataset-access/streamlit",
    to: "/docs/hub/cookbook/build-streamlit-dashboard",
  },
  {
    from: "/docs/dlt-ecosystem/verified-sources/filesystem/advanced",
    to: "/docs/dlt-ecosystem/verified-sources/filesystem",
  },
  {
    from: "/docs/running-in-production",
    to: "/docs/running-in-production/running",
  },
  {
    from: "/docs/hub/features/data-access",
    to: "/docs/general-usage/dataset-access/dataset",
  },
  {
    from: "/docs/hub/features/quality/tests",
    to: "/docs/hub/data-quality",
  },
  {
    from: "/docs/hub/features/transformations/setup",
    to: "/docs/hub/transformations",
  },
  {
    from: "/docs/hub/features/project/overview",
    to: "/docs/hub/pipeline-operations/overview",
  },
  {
    from: "/docs/hub/features/project/source-configuration",
    to: "/docs/hub/pipeline-operations/overview",
  },
  {
    from: "/docs/hub/features/project/python-api",
    to: "/docs/hub/pipeline-operations/overview",
  },
  {
    from: "/docs/hub/getting-started/tutorial",
    to: "/docs/hub/getting-started/introduction",
  },
  {
    from: "/docs/hub/getting-started/advanced_tutorial",
    to: "/docs/hub/getting-started/introduction",
  },
  {
    from: "/docs/hub/core-concepts/profiles",
    to: "/docs/hub/pipeline-operations/profiles",
  },
  {
    from: "/docs/hub/production/pipeline-runner",
    to: "/docs/hub/pipeline-operations/deployments",
  },
  {
    from: "/docs/hub/production/observability",
    to: "/docs/hub/pipeline-operations/monitoring",
  },
  {
    from: "/docs/hub/getting-started/playground-workspace",
    to: "/docs/hub/pipeline-operations/playground-workspace",
  },
];

module.exports = REDIRECTS;
