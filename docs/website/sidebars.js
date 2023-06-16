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
        'getting-started/try-in-colab',
        'getting-started/build-a-data-pipeline',
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
          label: 'Verified Sources',
          link: {
            type: 'generated-index',
            title: 'Verified Sources',
            description: 'Overview over our verified sources. A source is a location that holds data with certain structure. Organized into one or more resources. We have verified reference implementations for the sources listed below.',
            slug: 'dlt-ecosystem/verified-sources',
            keywords: ['verified source'],
          },
          items: [
            'dlt-ecosystem/verified-sources/asana',
            'dlt-ecosystem/verified-sources/chess',
            'dlt-ecosystem/verified-sources/facebook_ads',
            'dlt-ecosystem/verified-sources/github',
            'dlt-ecosystem/verified-sources/google_analytics',
            'dlt-ecosystem/verified-sources/google_sheets',
            'dlt-ecosystem/verified-sources/hubspot',
            'dlt-ecosystem/verified-sources/jira',
            'dlt-ecosystem/verified-sources/matomo',
            'dlt-ecosystem/verified-sources/mux',
            'dlt-ecosystem/verified-sources/notion',
            'dlt-ecosystem/verified-sources/pipedrive',
            'dlt-ecosystem/verified-sources/salesforce',
            'dlt-ecosystem/verified-sources/shopify',
            'dlt-ecosystem/verified-sources/sql_database',
            'dlt-ecosystem/verified-sources/strapi',
            'dlt-ecosystem/verified-sources/stripe',
            'dlt-ecosystem/verified-sources/workable',
            'dlt-ecosystem/verified-sources/zendesk',
          ]
        },
        {
          type: 'category',
          label: 'Destinations',
          link: {
            type: 'generated-index',
            title: 'Destinations',
            description: 'Overview over our destinations. A destiantion is the data store where data from the source is loaded. Learn how to use them in your pipelines.',
            slug: 'dlt-ecosystem/destinations',
            keywords: ['destination'],
          },
          items: [
            'dlt-ecosystem/destinations/bigquery',
            'dlt-ecosystem/destinations/duckdb',
            'dlt-ecosystem/destinations/postgres',
            'dlt-ecosystem/destinations/redshift',
          ]
        },
        {
          type: 'category',
          label: 'Deployments',
          link: {
            type: 'generated-index',
            title: 'Deployments',
            description: 'Overview over where you can deploy dlt.',
            slug: 'dlt-ecosystem/deployments',
            keywords: ['deployments'],
          },
          items: [
            'dlt-ecosystem/deployments/where-can-dlt-run',
            'dlt-ecosystem/deployments/github-actions',
            'dlt-ecosystem/deployments/running-in-cloud-functions',
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
            'walkthroughs/deploy-a-pipeline/deploy-with-google-cloud-functions'
          ]
        },

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
        'general-usage/configuration',
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
        'reference/command-line-interface',
        'reference/performance',
        'reference/explainers/how-dlt-works',
        'reference/explainers/schema-evolution',
        'reference/telemetry',
      ],
    },
  ]
};

module.exports = sidebars;
