/**
 * Creating a sidebar enables you to:
 - create an ordered group of docs
 - render a sidebar for each doc of that group
 - provide next/previous navigation

 The sidebars can be generated from the filesystem, or explicitly defined here.

 Create as many sidebars as you want.
 */

// @ts-check
const fs = require('fs');
const path = require('path');


function *walkSync(dir) {
  const files = fs.readdirSync(dir, { withFileTypes: true });
  for (const file of files) {
    if (file.isDirectory()) {
      yield* walkSync(path.join(dir, file.name));
    } else {
      yield path.join(dir, file.name);
    }
  }
}

/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  docsSidebar: [
    {
      type: 'category',
      label: 'Getting started',
      items: [
        { type: 'doc', id: 'intro', label: 'dlt' },
        'reference/installation',
        "dlt-ecosystem/llm-tooling/llm-native-workflow",
        "dlt-ecosystem/llm-tooling/explore-and-transform",
        'tutorial/rest-api',
        'tutorial/sql-database',
        'tutorial/filesystem',
        'tutorial/load-data-from-an-api',
      ]
    },
    {
      type: 'category',
      label: 'Release highlights',
      items: [
        { type: 'doc', id: 'release-notes/1.21.2', label: '1.21.2' },
        { type: 'doc', id: 'release-notes/1.19', label: '1.19' },
        { type: 'doc', id: 'release-notes/1.18', label: '1.18' },
        { type: 'doc', id: 'release-notes/1.17', label: '1.17' },
        { type: 'doc', id: 'release-notes/1.16', label: '1.16' },
        { type: 'doc', id: 'release-notes/1.15', label: '1.15' },
        { type: 'doc', id: 'release-notes/1.13-1.14', label: '1.13-1.14' },
        { type: 'doc', id: 'release-notes/1.12.1', label: '1.12.1' },
      ]
    },
    {
      type: 'category',
      label: 'Core concepts',
      items: [
        {
          type: 'doc',
          id: 'reference/explainers/how-dlt-works',
          label: 'Overview',
        },
        'general-usage/glossary',
        'general-usage/source',
        'general-usage/resource',
        'general-usage/pipeline',
        'general-usage/destination',
        'general-usage/state',
        {
          type: 'category',
          label: 'Schema',
          link: {
            type: 'doc',
            id: 'general-usage/schema',
          },
          items: [
            'general-usage/schema-contracts',
            'general-usage/schema-evolution',
            'general-usage/naming-convention',
          ]
        },
      ]
    },
    {
      type: 'category',
      label: 'Sources',
      items: [
        {
          type: 'link',
          label: '10k+ AI Context assets',
          description: 'Build a custom dlt REST API source with your agent',
          href: 'https://dlthub.com/workspace',
        },
        {
          type: 'category',
          label: 'REST API',
          description:'Load data from any REST API',
           link: {
            type: 'doc',
            id: 'dlt-ecosystem/verified-sources/rest_api/index',
          },
          items: [
            {
              type: 'doc',
              id: 'dlt-ecosystem/verified-sources/rest_api/basic',
              label: 'Basic',
            },
            {
              type: 'doc',
              id: 'dlt-ecosystem/verified-sources/rest_api/advanced',
              label: 'Advanced',
            },
            'dlt-ecosystem/verified-sources/openapi-generator',
          ]
        },
        {
          type: 'category',
          label: 'SQL database via SQLAlchemy',
          description: 'PostgreSQL, MySQL, MS SQL, BigQuery, Redshift, and more',
          link: {
            type: 'doc',
            id: 'dlt-ecosystem/verified-sources/sql_database/index',
           },
          items: [
            'dlt-ecosystem/verified-sources/sql_database/setup',
            'dlt-ecosystem/verified-sources/sql_database/configuration',
            'dlt-ecosystem/verified-sources/sql_database/usage',
            'dlt-ecosystem/verified-sources/sql_database/troubleshooting',
            'dlt-ecosystem/verified-sources/sql_database/advanced',
            'walkthroughs/add-incremental-configuration',
          ]
        },
        {
          type: 'doc',
          id: 'dlt-ecosystem/verified-sources/filesystem/index',
          label: 'Object store & filesystem',
        },
        'dlt-ecosystem/verified-sources/arrow-pandas',
        {
          type: 'category',
          label: 'Verified sources',
          description: 'Verified sources maintained by dltHub and the community',
          link: {
            type: 'doc',
            id: 'dlt-ecosystem/verified-sources/rest_api/index',
          },
          items: [
            'dlt-ecosystem/verified-sources/airtable',
            'dlt-ecosystem/verified-sources/amazon_kinesis',
            'dlt-ecosystem/verified-sources/asana',
            'dlt-ecosystem/verified-sources/chess',
            'dlt-ecosystem/verified-sources/facebook_ads',
            'dlt-ecosystem/verified-sources/freshdesk',
            'dlt-ecosystem/verified-sources/github',
            'dlt-ecosystem/verified-sources/google_ads',
            'dlt-ecosystem/verified-sources/google_analytics',
            'dlt-ecosystem/verified-sources/google_sheets',
            'dlt-ecosystem/verified-sources/hubspot',
            'dlt-ecosystem/verified-sources/inbox',
            'dlt-ecosystem/verified-sources/jira',
            'dlt-ecosystem/verified-sources/kafka',
            'dlt-ecosystem/verified-sources/matomo',
            'dlt-ecosystem/verified-sources/mongodb',
            'dlt-ecosystem/verified-sources/mux',
            'dlt-ecosystem/verified-sources/notion',
            'dlt-ecosystem/verified-sources/personio',
            'dlt-ecosystem/verified-sources/pg_replication',
            'dlt-ecosystem/verified-sources/pipedrive',
            'dlt-ecosystem/verified-sources/salesforce',
            'dlt-ecosystem/verified-sources/scrapy',
            'dlt-ecosystem/verified-sources/shopify',
            'dlt-ecosystem/verified-sources/slack',
            'dlt-ecosystem/verified-sources/strapi',
            'dlt-ecosystem/verified-sources/stripe',
            'dlt-ecosystem/verified-sources/workable',
            'dlt-ecosystem/verified-sources/zendesk',
          ]
        },
      ]
    },
    {
      type: 'category',
      label: 'Destinations',
      items: [
        'dlt-ecosystem/destinations/filesystem',
        'dlt-ecosystem/destinations/sqlalchemy',
        'dlt-ecosystem/destinations/athena',
        'dlt-ecosystem/destinations/bigquery',
        'dlt-ecosystem/destinations/clickhouse',
        'dlt-ecosystem/destinations/databricks',
        'dlt-ecosystem/destinations/delta-iceberg',
        'dlt-ecosystem/destinations/dremio',
        'dlt-ecosystem/destinations/duckdb',
        'dlt-ecosystem/destinations/ducklake',
        'dlt-ecosystem/destinations/huggingface',
        'dlt-ecosystem/destinations/iceberg',
        'dlt-ecosystem/destinations/lance',
        'dlt-ecosystem/destinations/lancedb',
        'dlt-ecosystem/destinations/fabric',
        'dlt-ecosystem/destinations/mssql',
        'dlt-ecosystem/destinations/motherduck',
        'dlt-ecosystem/destinations/postgres',
        'dlt-ecosystem/destinations/redshift',
        'dlt-ecosystem/destinations/snowflake',
        'dlt-ecosystem/destinations/synapse',
        'dlt-ecosystem/destinations/qdrant',
        'dlt-ecosystem/destinations/weaviate',
        'dlt-ecosystem/destinations/destination',
        'dlt-ecosystem/destinations/community-destinations',
      ]
    },
    {
      type: 'category',
      label: 'Using dlt',
      items: [
        'walkthroughs/create-a-pipeline',
        'walkthroughs/run-a-pipeline',
        'general-usage/dashboard',
        /*{
          type: "category",
          label: "Build with LLMs",
          link: {
            type: 'generated-index',
            title: 'Build with LLMs',
            description: 'Learn to build dlt pipelines with LLMs',
            slug: 'dlt-ecosystem/llm-tooling',
          },
          items: [
            "dlt-ecosystem/llm-tooling/llm-native-workflow",
          ]
        },*/
        {
          type: 'category',
          label: 'Configure pipelines and credentials',
           link: {
            type: 'doc',
            id: 'general-usage/credentials/index',
          },
          items: [
            'general-usage/credentials/setup',
            'general-usage/credentials/advanced',
            'general-usage/credentials/vaults',
            'general-usage/credentials/complex_types',
            // Unsure item
            'walkthroughs/add_credentials'
          ]
        },
        'walkthroughs/adjust-a-schema',
        {
          type: 'category',
          label: 'Access loaded data',
           link: {
            type: 'doc',
            id: 'general-usage/dataset-access/index',
          },
          items: [
            'general-usage/dataset-access/marimo',
            'general-usage/dataset-access/dataset',
            'general-usage/dataset-access/ibis-backend',
            'general-usage/dataset-access/sql-client',
            'general-usage/dataset-access/view-dlt-schema',
            'general-usage/destination-tables',
          ]
        },
        'general-usage/data-quality-lifecycle',
      ]
    },
    {
      type: 'category',
      label: 'Incremental loading',
      items: [
        'general-usage/full-loading',
        'general-usage/merge-loading',
        'general-usage/incremental-loading',
        'general-usage/incremental/cursor',
        'general-usage/incremental/lag',
        'general-usage/incremental/advanced-state',
        'general-usage/incremental/troubleshooting',
      ]
    },
    {
      type: 'category',
      label: 'Transformations',
      items: [
        {
          type: 'category',
          label: 'Extract, Transform, Load (ETL)',
          items: [
            { type: 'doc', id: 'dlt-ecosystem/transformations/add-map', label: 'Add column' },
            { type: 'doc', id: 'general-usage/customising-pipelines/renaming_columns', label: 'Rename column' },
            { type: 'doc', id: 'general-usage/customising-pipelines/removing_columns', label: 'Remove column' },
            { type: 'doc', id: 'general-usage/customising-pipelines/pseudonymizing_columns', label: 'Pseudonymize values' },
          ]
        },
        {
          type: 'category',
          label: 'Extract, Load, Transform (ELT)',
          items: [
            { type: 'doc', id: 'dlt-ecosystem/transformations/python', label: 'Python' },
            { type: 'doc', id: 'dlt-ecosystem/transformations/sql', label: 'SQL' },
            { type: 'doc', id: 'dlt-ecosystem/transformations/dbt/dbt', label: 'dbt' },
            { type: 'link', href: 'https://sqlmesh.readthedocs.io/en/stable/integrations/dlt/', label: 'SQLMesh' },
          ]
        },
      ]
    },
    {
      type: 'category',
      label: 'Deploy',
      items: [
        {
          type: "category",
          label: "On Snowflake",
          link: {
            type: 'generated-index',
            title: 'On Snowflake',
            description: 'How to run dlt in Snowflake.',
            slug: 'run-in-snowflake',
            keywords: ['Snowflake'],
          },
          items: [
            {
              id: "walkthroughs/run-in-snowflake/run-in-snowflake",
              type: 'doc',
              label: 'Run dlt',
            },
            {
              id: "walkthroughs/run-in-snowflake/database-connector-app",
              type: 'doc',
              label: 'Snowflake Native App',
            },
          ]
        },
        {
          type: 'category',
          label: 'Orchestrators',
          link: {
            type: 'generated-index',
            title: 'Deploy a pipeline',
            description: 'Deploy dlt pipelines with different methods.',
            slug: 'walkthroughs/deploy-a-pipeline',
          },
          items: [
            {
              id: 'walkthroughs/deploy-a-pipeline/deploy-with-github-actions',
              type: 'doc',
              label: 'GitHub Actions',
            },
            { type: 'doc', id: 'walkthroughs/deploy-a-pipeline/deploy-with-airflow-composer', label: 'Airflow' },
            { type: 'doc', id: 'walkthroughs/deploy-a-pipeline/deploy-with-google-cloud-functions', label: 'Google Cloud Functions' },
            { type: 'doc', id: 'walkthroughs/deploy-a-pipeline/deploy-with-google-cloud-run', label: 'Google Cloud Run' },
            { type: 'doc', id: 'walkthroughs/deploy-a-pipeline/deploy-with-kestra', label: 'Kestra' },
            { type: 'doc', id: 'walkthroughs/deploy-a-pipeline/deploy-with-dagster', label: 'Dagster' },
            { type: 'doc', id: 'walkthroughs/deploy-a-pipeline/deploy-with-prefect', label: 'Prefect' },
            { type: 'doc', id: 'walkthroughs/deploy-a-pipeline/deploy-with-modal', label: 'Modal' },
            { type: 'doc', id: 'walkthroughs/deploy-a-pipeline/deploy-with-orchestra', label: 'Orchestra' },
          ]
        },
      ]
    },
    {
      type: 'category',
      label: 'Performance',
      items: [
        { type: 'doc', id: 'reference/performance', label: 'Performance' },
      ],
    },
    {
      type: 'category',
      label: 'Reference',
      items: [
        'reference/command-line-interface',
        'reference/telemetry',
        'dlt-ecosystem/staging',
        {
          type: 'category',
          label: 'File formats',
          link: {
            type: 'generated-index',
            title: 'File formats',
            description: 'Overview of our loader file formats',
            slug: 'dlt-ecosystem/file-formats',
            keywords: ['destination', 'file formats'],
          },
          items: [
            'dlt-ecosystem/file-formats/jsonl',
            'dlt-ecosystem/file-formats/parquet',
            'dlt-ecosystem/file-formats/csv',
            'dlt-ecosystem/file-formats/insert-format',
          ]
        },
        {
          type: 'category',
          label: 'Table formats',
          link: {
            type: 'generated-index',
            title: 'Table formats',
            slug: 'dlt-ecosystem/table-formats',
            keywords: ['destination, table formats'],
          },
          items: [
            'dlt-ecosystem/table-formats/delta',
            'dlt-ecosystem/table-formats/iceberg',
          ]
        },
        'walkthroughs/create-new-destination',
        'general-usage/dataset-access/data-quality-dashboard',
        'reference/frequently-asked-questions',
      ],
    },
  ],
  hubSidebar: [
    {
      type: 'category',
      label: 'Getting started',
      items: [
        'hub/intro',
        'hub/getting-started/installation',
        'hub/getting-started/runtime-tutorial',
      ]
    },
        {
      type: 'category',
      label: 'AI Workbench',
      items: [
        'dlt-ecosystem/llm-tooling/llm-native-workflow',
      ]
    },
    {
      type: 'category',
      label: 'Ingestion',
      items: [
        'hub/workspace/init',
        { type: 'ref', id: 'general-usage/dashboard' },
        'hub/ecosystem/ms-sql',
        'hub/ecosystem/delta',
        'hub/ecosystem/iceberg',
        'hub/ecosystem/snowflake_plus',
      ]
    },
    {
      type: 'category',
      label: 'Transformations',
      items: [
        'hub/features/transformations/index',
        'hub/features/transformations/dbt-transformations',
      ]
    },
    {
      type: 'category',
      label: 'Pipeline operations',
      items: [
        'hub/runtime/overview',
        'hub/core-concepts/profiles-dlthub',
      ]
    },
    {
      type: 'category',
      label: 'Data quality & governance',
      items: [
        'hub/features/quality/data-quality',
        'hub/features/quality/advanced',
        'hub/features/quality/tests',
      ]
    },
    {
      type: 'category',
      label: 'Data discovery & serving',
      items: [
        'hub/core-concepts/datasets',
        { type: 'ref', id: 'general-usage/dataset-access/marimo' },
      ]
    },
    'hub/command-line-interface',
    'hub/EULA',
  ],
  cookbookSidebar: [
    {
      type: 'category',
      label: 'Cookbook',
      link: {
        type: 'doc',
        id: 'examples/index',
      },
      items: [
        'walkthroughs/dispatch-to-multiple-tables',
        'walkthroughs/share-a-dataset',
      ],
    },
  ],
  educationSidebar: [
    {
      type: 'category',
      label: 'Education',
      link: {
        type: 'doc',
        id: 'tutorial/education',
      },
      items: [
        'tutorial/fundamentals-course',
        'tutorial/advanced-course',
      ]
    },
  ]
};

 // insert examples
for (const item of sidebars.cookbookSidebar) {
    if (item.label === 'Cookbook') {
      for (let examplePath of walkSync("./docs_processed/examples")) {
        examplePath = examplePath.replace(/\\/g, "/");
        examplePath = examplePath.replace("docs_processed/", "");
        examplePath = examplePath.replace(".mdx", "");
        examplePath = examplePath.replace(".md", "");
        item.items.push(examplePath);
    }
  }
}


 // inject api reference if it exists
if (fs.existsSync('./docs_processed/api_reference/sidebar.json')) {
  for (const item of sidebars.docsSidebar) {
    if (item.label === 'Reference') {
      item.items.splice(0,0,require("./docs_processed/api_reference/sidebar.json"));
    }
  }
}

module.exports = sidebars;


/*
blog:
'build-a-pipeline-tutorial',
'walkthroughs/zendesk-weaviate',
{
  type: 'category',
  label: 'Data enrichments',
  items: [
    'general-usage/data-enrichments/user_agent_device_data_enrichment',
    'general-usage/data-enrichments/currency_conversion_data_enrichment',
    'general-usage/data-enrichments/url-parser-data-enrichment'
  ]
},
*/
