// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

const lightCodeTheme = require('prism-react-renderer/themes/github');
const darkCodeTheme = require('prism-react-renderer/themes/dracula');

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'dlt Docs',
  tagline: 'data load tool',
  url: 'https://dlthub.com',
  baseUrl: '/docs',
  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',
  favicon: 'img/favicon.ico',
  staticDirectories: ['public', 'static'],

  // GitHub pages deployment config.
  // If you aren't using GitHub pages, you don't need these.
  organizationName: 'dltHub', // Usually your GitHub org/user name.
  projectName: 'dlt', // Usually your repo name.

  // Even if you don't use internalization, you can use this field to set useful
  // metadata like html lang. For example, if your site is Chinese, you may want
  // to replace "en" with "zh-Hans".
  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      '@docusaurus/preset-classic',
      ({
        docs: {
          routeBasePath: '/',
          sidebarPath: require.resolve('./sidebars.js'),
          editUrl: 'https://github.com/dlt-hub/dlt/tree/devel/docs/website',
        },
        blog: {
          showReadingTime: true
        },
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
        gtag: {
          trackingID: 'G-7F1SE12JLR',
        },
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      navbar: {
        title: '',
        logo: {
          alt: 'dlt Docs Logo',
          src: 'img/dlt-logo.svg',
          href: 'https://dlthub.com'
        },
        items: [
          {
            type: 'doc',
            docId: 'intro',
            position: 'left',
            label: 'Docs',
          },
          {to: 'blog', label: 'Blog', position: 'left'},
          {
            href: 'https://join.slack.com/t/dlthub-community/shared_invite/zt-1slox199h-HAE7EQoXmstkP_bTqal65g',
            label: 'Slack',
            position: 'right',
          },
          {
            href: 'https://github.com/dlt-hub/dlt',
            label: 'GitHub',
            position: 'right',
          },
        ],
      },
      footer: {
        style: 'dark',
        links: [
          {
            title: 'Docs',
            items: [
              {
                label: 'Docs',
                to: '/intro',
              },
              {
                label: 'Blog',
                to: '/blog',
              }
            ],
          },
          {
            title: 'Community',
            items: [
              {
                label: 'Slack',
                href: 'https://join.slack.com/t/dlthub-community/shared_invite/zt-1slox199h-HAE7EQoXmstkP_bTqal65g',
              },
              {
                label: 'Email',
                href: 'mailto:community@dlthub.com',
              },
            ],
          },
          {
            title: 'More',
            items: [
              {
                label: 'GitHub',
                href: 'https://github.com/dlt-hub/dlt',
              },
              {
                label: 'Twitter',
                href: 'https://twitter.com/dlthub',
              }
            ],
          },
        ],
        copyright: `Copyright © ${new Date().getFullYear()} dltHub, Inc.`,
      },
      prism: {
        theme: lightCodeTheme,
        darkTheme: darkCodeTheme,
      },
      metadata: [{ name: 'keywords', content: 'data loading, elt, etl, extract, load, transform, python, data engineering, data warehouse, data lake'}],
      algolia: {
      // The application ID provided by Algolia
      appId: 'FUTSIDO7MI',

      // Public API key: it is safe to commit it
      apiKey: '94b8ae9b02673db8232fc6fe712bc5a0',

      indexName: 'dlthub',

      // Optional: see doc section below
      contextualSearch: true,

      },
    }),
};

module.exports = config;
