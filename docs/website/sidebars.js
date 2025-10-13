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
      link: {
        type: 'doc',
        id: 'intro',
      },
      items: [
        'reference/installation',
        'tutorial/rest-api',
        'tutorial/sql-database',
        'tutorial/filesystem',
        'tutorial/load-data-from-an-api',
        'tutorial/playground',
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
    },
    {
      type: 'category',
      label: 'Release highlights',
      link: {
        type: 'generated-index',
        title: 'Release highlights',
        slug: '/release-highlights',
        keywords: ['release notes, release highlights'],
      },
      items: [
        'release-notes/1.12.1',
        'release-notes/1.13-1.14',
        'release-notes/1.15'
      ]
    },
    {
      type: 'category',
      label: 'Core concepts',
      items: [
        'reference/explainers/how-dlt-works',
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
        'general-usage/glossary'
      ]
    },
    {
      type: 'category',
      label: 'Sources',
      link: {
        type: 'doc',
        id: 'dlt-ecosystem/verified-sources/index',
      },
      items: [
        {
          type: 'category',
          label: 'REST APIs',
          description:'Load data from any REST API',
           link: {
            type: 'doc',
            id: 'dlt-ecosystem/verified-sources/rest_api/index',
          },
          items: [
            'dlt-ecosystem/verified-sources/rest_api/basic',
            'dlt-ecosystem/verified-sources/rest_api/advanced',
            {
              type: 'category',
              label: 'REST API helpers',
              link: {
                type: 'doc',
                id: 'general-usage/http/overview',
              },
              items: [
                'general-usage/http/rest-client',
                'general-usage/http/requests',
              ]
            },
          ]
        },
        {
          type: 'category',
          label: '30+ SQL databases',
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
          type: 'category',
          label: 'Cloud storage and filesystem',
          description: 'AWS S3, Google Cloud Storage, Azure, SFTP, local file system',
            link: {
            type: 'doc',
            id: 'dlt-ecosystem/verified-sources/filesystem/index',
          },
          items: [
            'dlt-ecosystem/verified-sources/filesystem/basic',
            'dlt-ecosystem/verified-sources/filesystem/advanced',
          ]
        },
        'dlt-ecosystem/verified-sources/airtable',
        'dlt-ecosystem/verified-sources/amazon_kinesis',
        'dlt-ecosystem/verified-sources/arrow-pandas',
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
        'dlt-ecosystem/verified-sources/openapi-generator',
        'dlt-ecosystem/verified-sources/salesforce',
        'dlt-ecosystem/verified-sources/scrapy',
        'dlt-ecosystem/verified-sources/shopify',
        'dlt-ecosystem/verified-sources/slack',
        'dlt-ecosystem/verified-sources/strapi',
        'dlt-ecosystem/verified-sources/stripe',
        'dlt-ecosystem/verified-sources/workable',
        'dlt-ecosystem/verified-sources/zendesk',
        'walkthroughs/add-a-verified-source',
      ]
    },
    {
      type: 'category',
      label: 'Destinations',
      link: {
        type: 'doc',
        id: 'dlt-ecosystem/destinations/index',
      },
      items: [
        'dlt-ecosystem/destinations/bigquery',
        'dlt-ecosystem/destinations/databricks',
        'dlt-ecosystem/destinations/duckdb',
        'dlt-ecosystem/destinations/mssql',
        'dlt-ecosystem/destinations/synapse',
        'dlt-ecosystem/destinations/clickhouse',
        'dlt-ecosystem/destinations/filesystem',
        'dlt-ecosystem/destinations/delta-iceberg',
        'dlt-ecosystem/destinations/iceberg',
        'dlt-ecosystem/destinations/postgres',
        'dlt-ecosystem/destinations/redshift',
        'dlt-ecosystem/destinations/snowflake',
        'dlt-ecosystem/destinations/athena',
        'dlt-ecosystem/destinations/sqlalchemy',
        'dlt-ecosystem/destinations/weaviate',
        'dlt-ecosystem/destinations/lancedb',
        'dlt-ecosystem/destinations/qdrant',
        'dlt-ecosystem/destinations/dremio',
        'dlt-ecosystem/destinations/destination',
        'dlt-ecosystem/destinations/motherduck',
        'dlt-ecosystem/destinations/ducklake',
        'walkthroughs/create-new-destination'
      ]
    },
    {
      type: 'category',
      label: 'Using dlt',
      link: {
        type: 'generated-index',
        title: 'Using dlt',
        slug: 'general-usage',
        keywords: ['concepts', 'usage'],
      },
      items: [
        'walkthroughs/create-a-pipeline',
        'walkthroughs/run-a-pipeline',
        {
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
        },
        {
          type: 'category',
          label: 'Load data incrementally',
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
        'general-usage/dashboard',
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
            'general-usage/dataset-access/streamlit',
          ]
        },
        {
          type: 'category',
          label: 'Transform data',
          link: {
            type: 'doc',
            id: 'dlt-ecosystem/transformations/index',
          },
          items: [
            {
              type: 'category',
              label: 'Transform data with dbt',
              items: [
                'dlt-ecosystem/transformations/dbt/dbt',
                'dlt-ecosystem/transformations/dbt/dbt_cloud',
              ]
            },
            'dlt-ecosystem/transformations/python',
            'dlt-ecosystem/transformations/sql',
            {
              type: 'category',
              label: 'Transform before load',
              items: [
                'dlt-ecosystem/transformations/add-map',
                'general-usage/customising-pipelines/renaming_columns',
                'general-usage/customising-pipelines/pseudonymizing_columns',
                'general-usage/customising-pipelines/removing_columns',
              ]
            }
          ]
        },
      ]
    },
    {
      type: 'category',
      label: 'Deploying dlt',
      items: [
        'walkthroughs/share-a-dataset',
        {
          type: 'category',
          label: 'Deploy a pipeline',
          link: {
            type: 'generated-index',
            title: 'Deploy a pipeline',
            description: 'Deploy dlt pipelines with different methods.',
            slug: 'walkthroughs/deploy-a-pipeline',
          },
          items: [
            'walkthroughs/deploy-a-pipeline/deploy-with-github-actions',
            'walkthroughs/deploy-a-pipeline/deploy-with-airflow-composer',
            'reference/explainers/airflow-gcp-cloud-composer',
            'walkthroughs/deploy-a-pipeline/deploy-with-google-cloud-functions',
            'walkthroughs/deploy-a-pipeline/deploy-with-google-cloud-run',
            'walkthroughs/deploy-a-pipeline/deploy-gcp-cloud-function-as-webhook',
            'walkthroughs/deploy-a-pipeline/deploy-with-kestra',
            'walkthroughs/deploy-a-pipeline/deploy-with-dagster',
            'walkthroughs/deploy-a-pipeline/deploy-with-prefect',
            'walkthroughs/deploy-a-pipeline/deploy-with-modal',
            'walkthroughs/deploy-a-pipeline/deploy-with-orchestra',
          ]
        },
        {
          type: 'category',
          label: 'Run in production',
          link: {
            type: 'generated-index',
            title: 'Run in production',
            description: 'How to run dlt in production.',
            slug: 'running-in-production',
            keywords: ['production'],
          },
          items: [
            'running-in-production/running',
            'running-in-production/monitoring',
            'running-in-production/alerting',
            'running-in-production/tracing',
          ],
        },
      ]
    },
    {
      type: 'category',
      label: 'Optimizing dlt',
      items: [
        'reference/performance',
      ],
    },
    {
      type: 'category',
      label: 'Code examples',
      link: {
        type: 'generated-index',
        title: 'Code examples',
        description: 'A list of comprehensive code examples that teach you how to solve real world problem.',
        slug: 'examples',
        keywords: ['examples'],
      },
      items: [
        'walkthroughs/dispatch-to-multiple-tables',
      ],
    },
    {
      type: 'category',
      label: 'Reference',
      link: {
        type: 'generated-index',
        title: 'Reference',
        description: 'Learn more about the dlt, CLI, and the telemetry.',
        slug: 'reference',
        keywords: ['reference'],
      },
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
        'general-usage/dataset-access/data-quality-dashboard',
        'reference/frequently-asked-questions',
      ],
    },
    /*
    {
      type: 'category',
      label: 'How-to guides',
      link: {
        type: 'generated-index',
        title: 'How-to guides',
        description: 'In this section you will find step-by-step instructions for the common tasks.',
        slug: 'walkthroughs',
        keywords: ['how-to'],
      },
      items: [
        {
          type: 'category',
          label: 'Data enrichments',
          items: [
            'general-usage/data-enrichments/user_agent_device_data_enrichment',
            'general-usage/data-enrichments/currency_conversion_data_enrichment',
            'general-usage/data-enrichments/url-parser-data-enrichment'
          ]
        }
      ]
    }
    */
  ],
  hubSidebar: [
    {
      type: 'category',
      label: 'Getting started',
      items: [
        'hub/intro',
        'hub/getting-started/installation',
        'dlt-ecosystem/llm-tooling/llm-native-workflow',
      ]
    },
    {
      type: 'category',
      label: 'Workspace',
      link: {
        type: 'doc',
        id: 'hub/workspace/index',
      },
      items: [
        'hub/workspace/index',
        {
          type: 'category',
          label: 'Create pipeline',
          items: [
          'hub/workspace/init-and-verified-sources',
          'hub/ecosystem/ms-sql',
          ]
        },
        {
          type: 'category',
          label: 'Ensure data quality',
          items: [
            'general-usage/dashboard',
            'hub/features/mcp-server',
            'hub/features/quality/data-quality',
          ]
        },
        {
          type: 'category',
          label: 'Create reports and transformations',
          items: [
            'general-usage/dataset-access/marimo',
            'general-usage/dataset-access/dataset',
            'hub/features/transformations/index',
            'hub/features/transformations/dbt-transformations',
          ]
        },
        {
          type: 'category',
          label: 'Deploy workspace',
          items: [
            'hub/core-concepts/profiles',
          {
            type: 'category',
            label: 'Deploy a pipeline',
            link: {
              type: 'generated-index',
              title: 'Deploy a pipeline',
              description: 'Deploy dlt pipelines with different methods',
              slug: 'hub/walkthroughs/deploy-a-pipeline',
            },
            items: [
              'walkthroughs/deploy-a-pipeline/deploy-with-github-actions',
              'walkthroughs/deploy-a-pipeline/deploy-with-airflow-composer',
              'reference/explainers/airflow-gcp-cloud-composer',
              'walkthroughs/deploy-a-pipeline/deploy-with-google-cloud-functions',
              'walkthroughs/deploy-a-pipeline/deploy-with-google-cloud-run',
              'walkthroughs/deploy-a-pipeline/deploy-gcp-cloud-function-as-webhook',
              'walkthroughs/deploy-a-pipeline/deploy-with-kestra',
              'walkthroughs/deploy-a-pipeline/deploy-with-dagster',
              'walkthroughs/deploy-a-pipeline/deploy-with-prefect',
              'walkthroughs/deploy-a-pipeline/deploy-with-modal',
              'walkthroughs/deploy-a-pipeline/deploy-with-orchestra',
            ]
          },
          'hub/production/pipeline-runner',
          ]
        },
      ]
    },
    {
      type: 'category',
      label: 'Runtime',
      items: [
        {
          type: 'category',
          label: 'Maintain data workflows',
          items: [
            'hub/production/observability',
            'hub/production/prefect-integration',
          ]
        },
      ]
    },
    {
      type: 'category',
      label: 'Storage',
      items: [
        'hub/ecosystem/delta',
        'hub/ecosystem/iceberg',
        'hub/ecosystem/snowflake_plus',
      ]
    },
    {
              type: 'category',
              label: 'Project',
              link: {
                type: 'doc',
                id: 'hub/features/project/index',
              },
              items: [
                'hub/features/project/overview',
                'hub/features/project/source-configuration',
                'hub/features/project/python-api',
              ]
            },
    'hub/reference',
    'hub/EULA',
    ],
};

// insert examples
for (const item of sidebars.docsSidebar) {
  if (item.label === 'Code examples') {
    for (let examplePath of walkSync("./docs_processed/examples")) {
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
