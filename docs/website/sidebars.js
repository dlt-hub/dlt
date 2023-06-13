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
    {
      type: 'category',
      label: 'Getting Started',
      link: {
        type: 'generated-index',
        title: 'Getting Started',
        description: 'Learn how to get started with using dlt',
        slug: 'getting-started',
        keywords: ['getting started'],
      },
      items: [
        {
          type: 'category',
          label: 'Build a data pipeline',
          items: [
            'getting-started/build-a-data-pipeline/renaming_columns',
            'getting-started/build-a-data-pipeline/pseudonymizing_columns',
          ]
        },
        {
          type: 'category',
          label: 'Build a data platform',
          items: [
            'getting-started/build-a-data-platform/where_does_dlt_fit',
            'getting-started/build-a-data-platform/building_data_warehouse',
            ,
          ]
        },
      ],
    },
    {
      type: 'category',
      label: 'dlt Ecosystem',
      link: {
        type: 'generated-index',
        title: 'dlt Ecosystem',
        description: 'An overview of different aspects of the dlt ecosystem',
        slug: 'dlt-ecosystem',
        keywords: ['getting started'],
      },
      items: [
        {
          type: 'category',
          label: 'Destinations',
          items: [
            'dlt-ecosystem/destinations/bigquery',
            'dlt-ecosystem/destinations/duckdb',
            'dlt-ecosystem/destinations/postgres',
            'dlt-ecosystem/destinations/redshift',
          ]
        },
        {
          type: 'category',
          label: 'Transformations',
          items: [
            'dlt-ecosystem/transformations/transforming-the-data',
            ,
          ]
        },
        {
          type: 'category',
          label: 'Visualizations',
          items: [
            'dlt-ecosystem/visualizations/exploring-the-data',
            'dlt-ecosystem/visualizations/understanding-the-tables'
          ]
        },
        {
          type: 'category',
          label: 'Pipelines',
          items: [
            'dlt-ecosystem/pipelines/asana',
            'dlt-ecosystem/pipelines/chess',
            'dlt-ecosystem/pipelines/github',
            'dlt-ecosystem/pipelines/google_analytics',
            'dlt-ecosystem/pipelines/google_sheets',
            'dlt-ecosystem/pipelines/hubspot',
            'dlt-ecosystem/pipelines/matomo',
            'dlt-ecosystem/pipelines/pipedrive',
            'dlt-ecosystem/pipelines/shopify',
            'dlt-ecosystem/pipelines/strapi',
            'dlt-ecosystem/pipelines/zendesk',
          ]
        },
      ],
    },
    {
      type: 'category',
      label: 'User Guides',
      link: {
        type: 'generated-index',
        title: 'User Guides',
        slug: 'user-guides',
        keywords: ['user guides'],
      },
      items: [
        'user-guides/analytics-engineer',
        'user-guides/data-beginner',
        'user-guides/data-engineer',
        'user-guides/engineering-manager',
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
        'walkthroughs/deploy-a-pipeline'
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
        'general-usage/glossary',
        'general-usage/import-dlt',
        'general-usage/resource',
        'general-usage/source',
        'general-usage/pipeline',
        'general-usage/state',
        'general-usage/incremental-loading',
        'general-usage/credentials',
        'general-usage/schema',
        'general-usage/schema-evolution',
        'general-usage/configuration',
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
        {
          type: 'category',
          label: 'Orchestrators',
          items: [
            'running-in-production/orchestrators/choosing-an-orchestrator',
            'running-in-production/orchestrators/airflow-gcp-cloud-composer',
          ]
        },
        'running-in-production/deploying',
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
        'reference/how-dlt-works',
        'reference/command-line-interface',
        'reference/telemetry',
      ],
    },
  ]
};

module.exports = sidebars;
