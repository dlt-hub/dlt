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
    'architecture',
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
      label: 'Pipelines',
      items: [
        'pipelines/chess',
        'pipelines/github',
        'pipelines/google_sheets',
        'pipelines/pipedrive',
        'pipelines/strapi',
      ],
    },
    'destinations',
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
        'running-in-production/scheduling',
        'running-in-production/running',
        'running-in-production/monitoring',
        'running-in-production/alerting',
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
      label: 'Customization',
      items: [
        'customization/incremental-loading',
        'customization/credentials',
        'customization/configuration',
        'customization/project-structure',
        'customization/advanced-pipelines',
      ],
    },
    {
      type: 'category',
      label: 'Concepts',
      items: [
        'concepts/import-dlt',
        'concepts/resource',
        'concepts/source',
        'concepts/pipeline',
        'concepts/schema',
        'concepts/state',
      ],
    },
    'glossary',
    'command-line-interface',
    {
      type: 'category',
      label: 'Reference',
      items: [
        'reference/telemetry',
        'reference/tracing',
      ],
    },
  ],
};

module.exports = sidebars;
