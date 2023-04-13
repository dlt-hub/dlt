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
      label: 'Pipelines',
      items: [
        'pipelines/chess',
        'pipelines/github',
        'pipelines/google_sheets',
        'pipelines/hubspot',
        'pipelines/pipedrive',
        'pipelines/strapi',
        'pipelines/zendesk',
      ],
    },
    {
      type: 'category',
      label: 'Destinations',
      items: [
        'destinations/bigquery',
        'destinations/duckdb',
        'destinations/postgres',
        'destinations/redshift',
      ],
    },
    {
      type: 'category',
      label: 'Walkthroughs',
      items: [
        'walkthroughs/create-a-pipeline',
        'walkthroughs/add-a-pipeline',
        'walkthroughs/run-a-pipeline',
        'walkthroughs/adjust-a-schema',
        'walkthroughs/deploy-a-pipeline'
      ],
    },
    {
      type: 'category',
      label: 'General Usage',
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
      ],
    },
    {
      type: 'category',
      label: 'Running in production',
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
      items: [
        'using-loaded-data/viewing-the-tables',
        'using-loaded-data/exploring-the-data',
        'using-loaded-data/transforming-the-data',
      ],
    },
    {
      type: 'category',
      label: 'Reference',
      items: [
        'reference/command-line-interface',
        'reference/telemetry',
      ],
    },
  ],
};

module.exports = sidebars;
