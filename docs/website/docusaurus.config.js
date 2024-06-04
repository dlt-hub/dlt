// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion
require('dotenv').config()

const lightCodeTheme = require('prism-react-renderer/themes/dracula');
// const lightCodeTheme = require('prism-react-renderer/themes/github');
const darkCodeTheme = require('prism-react-renderer/themes/dracula');

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'dlt Docs',
  tagline: 'data load tool',
  url: 'https://dlthub.com',
  baseUrl: process.env.DOCUSAURUS_BASE_URL || '/docs',
  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'throw',
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
          path: 'docs_processed',
          include: ['**/*.md', '**/*.mdx'],
          exclude: [
            // '**/_*.{js,jsx,ts,tsx,md,mdx}',
            // '**/_*/**',
            '**/*.test.{js,jsx,ts,tsx}',
            '**/__tests__/**',
          ],
          sidebarPath: require.resolve('./sidebars.js'),
          editUrl: (params) => {
            return "https://github.com/dlt-hub/dlt/tree/devel/docs/website/docs/" + params.docPath;
          },
          versions: {
            current: {
              label: 'current',
            },
          },
          lastVersion: 'current',
          showLastUpdateAuthor: true,
          showLastUpdateTime: true,
        },
        blog: {
          showReadingTime: true
        },
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
        gtag: {
          trackingID: ['G-7F1SE12JLR', 'G-PRHSCL1CMK'],
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
          src: 'img/dlthub-logo.png',
          href: 'https://dlthub.com'
        },
        items: [
          { label: 'dlt ' + (process.env.IS_MASTER_BRANCH ? "stable ": "devel ") + (process.env.DOCUSAURUS_DLT_VERSION || "0.0.1"), position: 'left', href: 'https://github.com/dlt-hub/dlt', className: 'version-navbar'  },
          {
            type: 'doc',
            docId: 'intro',
            position: 'left',
            label: 'Docs',
          },
          { to: 'blog', label: 'Blog', position: 'left' },
          {
            href: 'https://dlthub.com/community',
            label: 'Join community',
            position: 'right',
            className: 'slack-navbar',
          },
          {
            href: 'https://github.com/dlt-hub/dlt',
            label: 'Star us',
            position: 'right',
            className: 'github-navbar',
            "aria-label": "GitHub repository",
          },
        ],
      },
      docs: {
        sidebar: {
          hideable: true,
        },
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
                className: 'footer-link'
              },
              {
                label: 'Blog',
                to: '/blog',
                className: 'footer-link'
              }
            ],
          },
          {
            title: 'Community',
            items: [
              {
                label: 'Slack',
                href: 'https://dlthub.com/community',
                className: 'footer-link'
              },
              {
                label: 'Email',
                href: 'mailto:community@dlthub.com',
                className: 'footer-link'
              },
            ],
          },
          {
            title: 'More',
            items: [
              {
                label: 'GitHub',
                href: 'https://github.com/dlt-hub/dlt',
                className: 'footer-link'
              },
              {
                label: 'Twitter',
                href: 'https://twitter.com/dlthub',
                className: 'footer-link'
              }
            ],
          },
        ],
        copyright: `Copyright © ${new Date().getFullYear()} dltHub, Inc.`,
      },
      prism: {
        theme: lightCodeTheme,
        darkTheme: darkCodeTheme,
        additionalLanguages: ['powershell', 'bash', 'python', 'toml', 'yaml', 'log'],
      },
      metadata: [{ name: 'keywords', content: 'data loading, elt, etl, extract, load, transform, python, data engineering, data warehouse, data lake' }],
      algolia: {
        // The application ID provided by Algolia
        appId: 'FUTSIDO7MI',

        // Public API key: it is safe to commit it
        apiKey: '94b8ae9b02673db8232fc6fe712bc5a0',

        indexName: 'dlthub',

        // Optional: see doc section below
        contextualSearch: true,
      },
      colorMode: {
        defaultMode:'dark',
        disableSwitch: false,
        respectPrefersColorScheme: true,
      },
    }),

  scripts: [
    {
      src: 'https://dlt-static.s3.eu-central-1.amazonaws.com/dhelp.js',
      async: true,
      defer: true,
    },
  ],
};

if (!process.env.IS_MASTER_BRANCH && config.themeConfig) {
  config.themeConfig.announcementBar = {
    id: 'devel docs',
    content:
      'This is the development version of the dlt docs. <a target="_blank" rel="noopener noreferrer" href="https://dlthub.com/docs/intro">Go to the stable docs.</a>',
    backgroundColor: '#4c4898',
    textColor: '#fff',
    isCloseable: false,
  }
}

module.exports = config;
