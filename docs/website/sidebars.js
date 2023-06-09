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
    'installation',
    'getting-started',
    'how-dlt-works',
    {
      type: 'category',
      label: 'Verified Sources',
      className: 'project',
      link: {
        type: 'generated-index',
        title: 'Verified Sources',
        description: 'Overview over our verified sources. A source is a location that holds data with certain structure. Organized into one or more resources. We have verified reference implementations for the sources listed below.',
        slug: 'verified-sources',
        keywords: ['verified source'],
      },
      items: [
        'verified-sources/asana',
        'verified-sources/chess',
        'verified-sources/github',
        'verified-sources/google_analytics',
        'verified-sources/google_sheets',
        'verified-sources/hubspot',
        'verified-sources/matomo',
        'verified-sources/pipedrive',
        'verified-sources/shopify',
        'verified-sources/strapi',
        'verified-sources/stripe',
        'verified-sources/zendesk',
      ],
    },
    {
      type: 'category',
      label: 'Destinations',
      link: {
        type: 'generated-index',
        title: 'Destinations',
        description: 'Overview over our destinations. A destiantion is the data store where data from the source is loaded. Learn how to use them in your pipelines.',
        slug: 'destinations',
        keywords: ['destination'],
      },
      items: [
        'destinations/bigquery',
        'destinations/duckdb',
        'destinations/postgres',
        'destinations/redshift',
      ],
    },
    {
      type: 'category',
      label: 'User Guides',
      link: {
        type: 'generated-index',
        title: 'User Guides',
        slug: 'user-guides',
        keywords: ['user guide'],
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
      label: 'Customizations',
      link: {
        type: 'generated-index',
        title: 'Customizations',
        description: 'Learn how to customize the default behaviour of dlt to your needs.',
        slug: 'customizations',
        keywords: ['customization'],
      },
      items: [
        {
          type: 'category',
          label: 'Customizing pipelines',
          items: [
            'customizations/customizing-pipelines/renaming_columns',
            'customizations/customizing-pipelines/pseudonymizing_columns',
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
      label: 'Using loaded data',
      link: {
        type: 'generated-index',
        title: 'Using loaded data',
        description: 'Learn how to make use of the data loaded by dlt.',
        slug: 'using-loaded-data',
        keywords: ['loaded data'],
      },
      items: [
        'using-loaded-data/understanding-the-tables',
        'using-loaded-data/exploring-the-data',
        'using-loaded-data/transforming-the-data',
      ],
    },
    {
      type: 'category',
      label: 'Reference',
      link: {
        type: 'generated-index',
        title: 'Reference',
        description: 'The dlthub reference. Learn about the CLI and the telemetry.',
        slug: 'reference',
        keywords: ['reference'],
      },
      items: [
        'reference/command-line-interface',
        'reference/telemetry',
      ],
    },
  ],
};

module.exports = sidebars;
