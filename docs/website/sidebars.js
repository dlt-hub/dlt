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
  tutorialSidebar: [
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
          label: '30+ SQL Databases',
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
        'dlt-ecosystem/visualizations/exploring-the-data',
        {
          type: 'category',
          label: 'Transform the data',
          link: {
            type: 'generated-index',
            title: 'Transform the data',
            description: 'If you want to transform the data after loading, you can use one of the following methods: dbt, SQL, Pandas.',
            slug: 'dlt-ecosystem/transformations',
            keywords: ['transformations'],
          },
          items: [
            {
              type: 'category',
              label: 'Transforming data with dbt',
              items: [
                'dlt-ecosystem/transformations/dbt/dbt',
                'dlt-ecosystem/transformations/dbt/dbt_cloud',
              ]
            },
            'dlt-ecosystem/transformations/sql',
            'dlt-ecosystem/transformations/pandas',
            'general-usage/customising-pipelines/renaming_columns',
            'general-usage/customising-pipelines/pseudonymizing_columns',
            'general-usage/customising-pipelines/removing_columns'
          ]
        },
        {
          type: 'category',
          label: 'Configuration and secrets',
           link: {
            type: 'doc',
            id: 'general-usage/credentials/index',
          },
          items: [
            'general-usage/credentials/setup',
            'general-usage/credentials/advanced',
            'general-usage/credentials/complex_types',
            // Unsure item
            'walkthroughs/add_credentials'
          ]
        },
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
            'walkthroughs/adjust-a-schema',
          ]
        },
        {
          type: 'category',
          label: 'Loading Behavior',
          items: [
            'general-usage/incremental-loading',
            'walkthroughs/add-incremental-configuration',
            'general-usage/full-loading',
          ]
        }
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
            'walkthroughs/deploy-a-pipeline/deploy-gcp-cloud-function-as-webhook',
            'walkthroughs/deploy-a-pipeline/deploy-with-kestra',
            'walkthroughs/deploy-a-pipeline/deploy-with-dagster',
            'walkthroughs/deploy-a-pipeline/deploy-with-prefect',
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
    'reference/performance',
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
        // Unsure item
        'general-usage/destination-tables',
        'general-usage/naming-convention',
        'dlt-ecosystem/staging',
        {
          type: 'category',
          label: 'File formats',
          link: {
            type: 'generated-index',
            title: 'File formats',
            description: 'Overview of our loader file formats',
            slug: 'dlt-ecosystem/file-formats',
            keywords: ['destination'],
          },
          items: [
            'dlt-ecosystem/file-formats/jsonl',
            'dlt-ecosystem/file-formats/parquet',
            'dlt-ecosystem/file-formats/csv',
            'dlt-ecosystem/file-formats/insert-format',
          ]
        },
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
  ]
};

// insert examples
for (const item of sidebars.tutorialSidebar) {
  if (item.label === 'Code examples') {
    for (let examplePath of walkSync("./docs_processed/examples")) {
      examplePath = examplePath.replace("docs_processed/", "");
      examplePath = examplePath.replace(".md", "");
      item.items.push(examplePath);
    }
  }
}


// inject api reference if it exists
if (fs.existsSync('./docs_processed/api_reference/sidebar.json')) {
  for (const item of sidebars.tutorialSidebar) {
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