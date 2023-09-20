/**
 * Creating a sidebar enables you to:
 - create an ordered group of docs
 - render a sidebar for each doc of that group
 - provide next/previous navigation

 The sidebars can be generated from the filesystem, or explicitly defined here.

 Create as many sidebars as you want.
 */

// @ts-check

/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  tutorialSidebar: [
    'intro',
    'getting-started',
    'build-a-pipeline-tutorial',
    {
      type: 'category',
      label: 'dlt Ecosystem',
      link: {
        type: 'generated-index',
        title: 'dlt Ecosystem',
        description: 'dlt fits everywhere where the data flows. check out our curated data sources, destinations and unexpected places where dlt runs',
        slug: 'dlt-ecosystem',
        keywords: ['getting started'],
      },
      items: [
        {
          type: 'category',
          label: 'Verified Sources',
          link: {
            type: 'doc',
            id: 'dlt-ecosystem/verified-sources/index',
          },
          items: [
            'dlt-ecosystem/verified-sources/airtable',
            'dlt-ecosystem/verified-sources/asana',
            'dlt-ecosystem/verified-sources/chess',
            'dlt-ecosystem/verified-sources/facebook_ads',
            'dlt-ecosystem/verified-sources/github',
            'dlt-ecosystem/verified-sources/google_analytics',
            'dlt-ecosystem/verified-sources/google_sheets',
            'dlt-ecosystem/verified-sources/hubspot',
            'dlt-ecosystem/verified-sources/jira',
            'dlt-ecosystem/verified-sources/matomo',
            'dlt-ecosystem/verified-sources/mongodb',
            'dlt-ecosystem/verified-sources/mux',
            'dlt-ecosystem/verified-sources/notion',
            'dlt-ecosystem/verified-sources/pipedrive',
            'dlt-ecosystem/verified-sources/salesforce',
            'dlt-ecosystem/verified-sources/shopify',
            'dlt-ecosystem/verified-sources/sql_database',
            'dlt-ecosystem/verified-sources/strapi',
            'dlt-ecosystem/verified-sources/stripe',
            'dlt-ecosystem/verified-sources/workable',
            'dlt-ecosystem/verified-sources/zendesk'
          ]
        },
        {
          type: 'category',
          label: 'File formats',
          link: {
            type: 'generated-index',
            title: 'File formats',
            description: 'Overview over our loader file formats',
            slug: 'dlt-ecosystem/file-formats',
            keywords: ['destination'],
          },
          items: [
            'dlt-ecosystem/file-formats/jsonl',
            'dlt-ecosystem/file-formats/parquet',
            'dlt-ecosystem/file-formats/insert-format',
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
            'dlt-ecosystem/destinations/duckdb',
            'dlt-ecosystem/destinations/mssql',
            'dlt-ecosystem/destinations/filesystem',
            'dlt-ecosystem/destinations/postgres',
            'dlt-ecosystem/destinations/redshift',
            'dlt-ecosystem/destinations/snowflake',
            'dlt-ecosystem/destinations/athena',
            'dlt-ecosystem/destinations/motherduck',
            'dlt-ecosystem/destinations/weaviate',
          ]
        },
        'dlt-ecosystem/staging',
        {
          type: 'category',
          label: 'Deployments',
          link: {
            type: 'doc',
            id: 'dlt-ecosystem/deployments/index',
          },
          items: [
            'walkthroughs/deploy-a-pipeline/deploy-with-github-actions',
            'walkthroughs/deploy-a-pipeline/deploy-with-airflow-composer',
            'walkthroughs/deploy-a-pipeline/deploy-with-google-cloud-functions',
            'walkthroughs/deploy-a-pipeline/deploy-gcp-cloud-function-as-webhook',
          ]
        },
        {
          type: 'category',
          label: 'Transformations',
          link: {
            type: 'generated-index',
            title: 'Transformations',
            description: 'If you want to transform the data after loading, you can use one of the following methods: dbt, SQL, Pandas.',
            slug: 'dlt-ecosystem/transformations',
            keywords: ['transformations'],
          },
          items: [
            'dlt-ecosystem/transformations/dbt',
            'dlt-ecosystem/transformations/sql',
            'dlt-ecosystem/transformations/pandas',
          ]
        },
        {
          type: 'category',
          label: 'Visualizations',
          items: [
            'dlt-ecosystem/visualizations/exploring-the-data',
          ]
        },
      ],
    },
    {
      type: 'category',
      label: 'Walkthroughs',
      link: {
        type: 'generated-index',
        title: 'Walktroughs',
        description: 'Overview over our walkthroughs. Learn how to use and deploy dlt.',
        slug: 'walkthroughs',
        keywords: ['walkthrough'],
      },
      items: [
        'walkthroughs/create-a-pipeline',
        'walkthroughs/add-a-verified-source',
        'walkthroughs/run-a-pipeline',
        'walkthroughs/adjust-a-schema',
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
            'walkthroughs/deploy-a-pipeline/deploy-with-google-cloud-functions',
            'walkthroughs/deploy-a-pipeline/deploy-gcp-cloud-function-as-webhook',
          ]
        },
        'walkthroughs/create-new-destination',
      ],
    },
    {
      type: 'category',
      label: 'General Usage',
      link: {
        type: 'generated-index',
        title: 'General usage',
        slug: 'general-usage',
        keywords: ['general usage'],
      },
      items: [
        'general-usage/resource',
        'general-usage/source',
        'general-usage/pipeline',
        'general-usage/destination-tables',
        'general-usage/state',
        'general-usage/incremental-loading',
        'general-usage/full-loading',
        'general-usage/credentials',
        'general-usage/schema',
        'general-usage/configuration',
        'general-usage/glossary',
        {
          type: 'category',
          label: 'Customising pipelines',
          items: [
            'general-usage/customising-pipelines/renaming_columns',
            'general-usage/customising-pipelines/pseudonymizing_columns',
          ]
        },
      ],
    },
    {
      type: 'category',
      label: 'Running in production',
      link: {
        type: 'generated-index',
        title: 'Running in production',
        description: 'Learn how to run dlt in production.',
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
        'reference/performance',
        'reference/telemetry',
        'reference/explainers/how-dlt-works',
        'reference/explainers/airflow-gcp-cloud-composer',
      ],
    },
  ]
};

module.exports = sidebars;
