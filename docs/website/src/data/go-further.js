/**
 * "Go further" links shown at the end of selected docs pages.
 *
 * Keys are doc permalinks without the version segment, so "/docs/devel/intro" and
 * "/docs/intro" both match "/docs/intro". A key ending in "/" is a prefix rule and
 * matches every page below it; exact keys win over prefixes, and the longest prefix wins.
 * Pages without a match render nothing.
 *
 * Links point at dlthub.com, which is a separate app on the same domain, so they must be
 * absolute URLs. Check each one returns 200 before adding it.
 */

const LINKS = {
  scd2Guide: {
    label: "Slowly changing dimensions (SCD2) with dlt",
    href: "https://dlthub.com/blog/scd2-and-incremental-loading",
    description: "What SCD2 is, when to use it, and how to set it up in dlt.",
  },
  scd2NestedJson: {
    label: "SCD2 on nested JSON: queries and cost",
    href: "https://dlthub.com/blog/scd2-nested-json-data-cost-optimization",
    description: "How nested data affects SCD2 queries and costs, with BigQuery numbers.",
  },
  practiceApis: {
    label: "Free APIs to practice on",
    href: "https://dlthub.com/blog/practice-api-sources",
    description: "Public APIs you can use to try out REST API pipelines.",
  },
  context: {
    label: "Source scaffolds for 10,000+ APIs",
    href: "https://dlthub.com/context",
    description: "Start a pipeline from a ready-made scaffold instead of from scratch.",
  },
  dlthub: {
    label: "dltHub platform",
    href: "https://dlthub.com/products/dlthub",
    description: "Deploy, monitor and scale dlt pipelines.",
  },
  motherduckDemo: {
    label: "dlt, dbt and MotherDuck in practice",
    href: "https://dlthub.com/blog/dlt-motherduck-demo",
    description: "A small, customizable data stack with dlt, dbt, DuckDB and MotherDuck.",
  },
  snowflake: {
    label: "dlt and Snowflake",
    href: "https://dlthub.com/partners/snowflake",
    description: "How teams load production data into Snowflake with dlt.",
  },
  databricks: {
    label: "dlt and Databricks",
    href: "https://dlthub.com/partners/databricks",
    description: "Custom sources, Unity Catalog, Delta and Iceberg with dlt.",
  },
  dbtSemanticLayer: {
    label: "dlt and dbt for semantic modelling",
    href: "https://dlthub.com/blog/dlt-dbt-semantic-layer",
    description: "An end-to-end pipeline from dlt to dbt semantic models.",
  },
  transformations: {
    label: "dltHub Transformations",
    href: "https://dlthub.com/products/transformations",
    description: "Model the data your pipelines load, starting from its schema.",
  },
  schemaEvolutionGuide: {
    label: "Schema evolution in data pipelines",
    href: "https://dlthub.com/blog/schema-evolution-guide",
    description: "Common failure modes when source schemas change, and how dlt handles them.",
  },
  dataQuality: {
    label: "Data quality in dltHub",
    href: "https://dlthub.com/products/data-quality",
    description: "Expectations, schema contracts and PII redaction on load.",
  },
  benchmark: {
    label: "dltHub throughput benchmark",
    href: "https://dlthub.com/blog/benchmark-dlthub",
    description: "Measured GB per hour for Parquet, SQL, JSON and REST sources.",
  },
  sqlBenchmark: {
    label: "SQL pipeline benchmark",
    href: "https://dlthub.com/blog/sql-benchmark-saas",
    description: "How data pipeline tools compare on SQL database loads.",
  },
  managedInfrastructure: {
    label: "Managed infrastructure",
    href: "https://dlthub.com/products/managed-infrastructure",
    description: "Scheduling, observability and alerting for every pipeline you run.",
  },
  airflow: {
    label: "Running dlt with Airflow",
    href: "https://dlthub.com/blog/dlt-with-airflow",
    description: "Four ways to run dlt in Airflow and which setup scales best.",
  },
  dagster: {
    label: "Orchestrating dlt with Dagster",
    href: "https://dlthub.com/blog/dlt-dagster",
    description: "A step-by-step Dagster project that runs a dlt pipeline.",
  },
  prefect: {
    label: "Resilient pipelines with dlt and Prefect",
    href: "https://dlthub.com/blog/dlt-prefect",
    description: "Loading Slack data into BigQuery and handling failures with Prefect.",
  },
  blog: {
    label: "dltHub blog",
    href: "https://dlthub.com/blog",
    description: "Guides, benchmarks and case studies from the dlt team.",
  },
  pricing: {
    label: "dltHub pricing",
    href: "https://dlthub.com/pricing",
    description: "Plans and what each one includes.",
  },
};

const restApi = [LINKS.practiceApis, LINKS.context];
const schema = [LINKS.schemaEvolutionGuide, LINKS.dataQuality];
const credentials = [LINKS.managedInfrastructure];

export const GO_FURTHER = {
  "/docs/intro": [LINKS.dlthub, LINKS.context, LINKS.blog],
  "/docs/general-usage/incremental-loading": [LINKS.scd2Guide, LINKS.scd2NestedJson],
  "/docs/general-usage/schema-contracts": schema,
  "/docs/general-usage/schema-evolution": schema,
  "/docs/general-usage/credentials/setup": credentials,
  "/docs/walkthroughs/add_credentials": credentials,
  "/docs/dlt-ecosystem/verified-sources": [LINKS.context, LINKS.dlthub],
  "/docs/dlt-ecosystem/verified-sources/rest_api": restApi,
  "/docs/dlt-ecosystem/verified-sources/rest_api/basic": restApi,
  "/docs/dlt-ecosystem/verified-sources/rest_api/advanced": restApi,
  "/docs/dlt-ecosystem/destinations/motherduck": [LINKS.motherduckDemo],
  "/docs/dlt-ecosystem/destinations/snowflake": [LINKS.snowflake],
  "/docs/dlt-ecosystem/destinations/databricks": [LINKS.databricks],
  "/docs/dlt-ecosystem/transformations/dbt": [LINKS.dbtSemanticLayer, LINKS.transformations],
  "/docs/reference/performance": [LINKS.benchmark, LINKS.sqlBenchmark],
  "docs/dlt-ecosystem/verified-sources/sql_database": LINKS.sqlBenchmark,
  "/docs/walkthroughs/deploy-a-pipeline/deploy-with-airflow-composer": [LINKS.airflow],
  "/docs/walkthroughs/deploy-a-pipeline/deploy-with-dagster": [LINKS.dagster],
  "/docs/walkthroughs/deploy-a-pipeline/deploy-with-prefect": [LINKS.prefect],
  "/docs/hub/": [LINKS.dlthub, LINKS.pricing],
};

/** Returns the links for a version-less docs path, or undefined. */
export function getGoFurtherLinks(path) {
  const normalized = path.length > 1 ? path.replace(/\/+$/, "") : path;
  if (GO_FURTHER[normalized]) {
    return GO_FURTHER[normalized];
  }
  const prefix = Object.keys(GO_FURTHER)
    .filter((key) => key.endsWith("/") && normalized.startsWith(key))
    .sort((a, b) => b.length - a.length)[0];
  return prefix ? GO_FURTHER[prefix] : undefined;
}
