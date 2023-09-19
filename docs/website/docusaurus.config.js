// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

const lightCodeTheme = require('prism-react-renderer/themes/dracula');
// const lightCodeTheme = require('prism-react-renderer/themes/github');
const darkCodeTheme = require('prism-react-renderer/themes/dracula');

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'dlt Docs',
  tagline: 'data load tool',
  url: 'https://dlthub.com',
  baseUrl: '/docs',
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
          include: ['**/*.md', '**/*.mdx'],
          exclude: [
            // '**/_*.{js,jsx,ts,tsx,md,mdx}',
            // '**/_*/**',
            '**/*.test.{js,jsx,ts,tsx}',
            '**/__tests__/**',
          ],
          sidebarPath: require.resolve('./sidebars.js'),
          editUrl: 'https://github.com/dlt-hub/dlt/tree/devel/docs/website',
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
          {
            type: 'doc',
            docId: 'intro',
            position: 'left',
            label: 'Docs',
          },
          { to: 'blog', label: 'Blog', position: 'left' },
          {
            href:'https://colab.research.google.com/drive/1NfSB1DpwbbHX9_t5vlalBTf13utwpMGx?usp=sharing',
            label: 'Colab demo',
            position:'right',
          className: 'colab-demo',
          },
          {
            href: 'https://join.slack.com/t/dlthub-community/shared_invite/zt-1slox199h-HAE7EQoXmstkP_bTqal65g',
            label: '.',
            position: 'right',
            className: 'slack-navbar',
          },
          {
            href: 'https://github.com/dlt-hub/dlt',
            label: '.',
            position: 'right',
            className: 'github-navbar',
            "aria-label": "GitHub repository",
          },
        ],
      },
      announcementBar: {
        content:
          '⭐️ If you like data load tool (dlt), give it a star on <a target="_blank" rel="noopener noreferrer" href="https://github.com/dlt-hub/dlt">GitHub</a>! ⭐️',
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
                href: 'https://join.slack.com/t/dlthub-community/shared_invite/zt-1slox199h-HAE7EQoXmstkP_bTqal65g',
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

module.exports = config;
