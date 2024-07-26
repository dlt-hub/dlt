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
    'intro',
    'getting-started',
    {
      type: 'category',
      label: 'Tutorial',
      link: {
        type: 'doc',
        id: 'tutorial/intro',
      },
      items: [
        'tutorial/load-data-from-an-api',
        'tutorial/grouping-resources',
      ]
    },
    {
      type: 'category',
      label: 'Integrations',
      link: {
        type: 'doc',
        id: 'dlt-ecosystem/index',
      },
      items: [
        {
          type: 'category',
          label: 'Sources',
          link: {
            type: 'doc',
            id: 'dlt-ecosystem/verified-sources/index',
          },
          items: [
            'dlt-ecosystem/verified-sources/airtable',
            'dlt-ecosystem/verified-sources/amazon_kinesis',
            'dlt-ecosystem/verified-sources/arrow-pandas',
            'dlt-ecosystem/verified-sources/asana',
            'dlt-ecosystem/verified-sources/chess',
            'dlt-ecosystem/verified-sources/facebook_ads',
            'dlt-ecosystem/verified-sources/filesystem',
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
            'dlt-ecosystem/verified-sources/rest_api',
            'dlt-ecosystem/verified-sources/openapi-generator',
            'dlt-ecosystem/verified-sources/salesforce',
            'dlt-ecosystem/verified-sources/scrapy',
            'dlt-ecosystem/verified-sources/shopify',
            'dlt-ecosystem/verified-sources/sql_database',
            'dlt-ecosystem/verified-sources/slack',
            'dlt-ecosystem/verified-sources/strapi',
            'dlt-ecosystem/verified-sources/stripe',
            'dlt-ecosystem/verified-sources/workable',
            'dlt-ecosystem/verified-sources/zendesk'
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
            'dlt-ecosystem/destinations/weaviate',
            'dlt-ecosystem/destinations/lancedb',
            'dlt-ecosystem/destinations/qdrant',
            'dlt-ecosystem/destinations/dremio',
            'dlt-ecosystem/destinations/destination',
            'dlt-ecosystem/destinations/motherduck'
          ]
        },
      ],
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
        'reference/explainers/how-dlt-works',
        'general-usage/resource',
        'general-usage/source',
        {
          type: 'category',
          label: 'Configuration and secrets',
           link: {
            type: 'doc',
            id: 'general-usage/credentials/index',
          },
          items: [
            'general-usage/credentials/setup',
            'general-usage/credentials/custom_sources',
            'general-usage/credentials/prebuilt_types',
          ]
        },
        'general-usage/pipeline',
        'general-usage/destination',
        'general-usage/destination-tables',
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
        'dlt-ecosystem/staging',
        'general-usage/state',
        'general-usage/incremental-loading',
        'general-usage/full-loading',
        'general-usage/schema',
        'general-usage/naming-convention',
        'general-usage/schema-contracts',
        'general-usage/schema-evolution',
        'build-a-pipeline-tutorial',
        'reference/performance',
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
      ],
    },
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
        'walkthroughs/create-a-pipeline',
        'walkthroughs/add-a-verified-source',
        'walkthroughs/add_credentials',
        'walkthroughs/run-a-pipeline',
        'walkthroughs/adjust-a-schema',
        'walkthroughs/share-a-dataset',
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
          ]
        },
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
          label: 'Customise pipelines',
          items: [
            'general-usage/customising-pipelines/renaming_columns',
            'general-usage/customising-pipelines/pseudonymizing_columns',
            'general-usage/customising-pipelines/removing_columns',
          ]
        },
        {
          type: 'category',
          label: 'Data enrichments',
          items: [
            'general-usage/data-enrichments/user_agent_device_data_enrichment',
            'general-usage/data-enrichments/currency_conversion_data_enrichment',
            'general-usage/data-enrichments/url-parser-data-enrichment'
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
        'walkthroughs/dispatch-to-multiple-tables',
        'walkthroughs/create-new-destination',
        'walkthroughs/zendesk-weaviate',
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
      ],
    },
    {
      type: 'category',
      label: 'Reference',
      link: {
        type: 'generated-index',
        title: 'Reference',
        description: 'The dlthub reference. Learn more about the dlt, CLI, and the telemetry.',
        slug: 'reference',
        keywords: ['reference'],
      },
      items: [
        'reference/installation',
        'reference/command-line-interface',
        'reference/telemetry',
        'reference/frequently-asked-questions',
        'general-usage/glossary',
      ],
    },
    // {
    //   "API Documentation": [
    //   require("./docs/api_reference/sidebar.json")
    // ],
    // }
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

// on the master branch link to devel and vice versa
if (process.env.IS_MASTER_BRANCH) {
  sidebars.tutorialSidebar.push(    {
    type: 'link',
    label: 'Switch to Devel Docs',
    href: 'https://dlthub.com/devel/intro',
    className: 'learn-more-link',
  })
} else {
  sidebars.tutorialSidebar.push(    {
    type: 'link',
    label: 'Switch to Stable Docs',
    href: 'https://dlthub.com/docs/intro',
    className: 'learn-more-link',
  })
}

module.exports = sidebars;
