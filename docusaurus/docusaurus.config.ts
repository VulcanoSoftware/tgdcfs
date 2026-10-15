import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

const config: Config = {
  title: 'TGDCFS',
  tagline: 'Telegram & Discord as WebDAV and SFTP Storage Backends',
  favicon: 'img/favicon.ico',

  url: 'https://tgdcfs.vercel.app',
  baseUrl: '/',

  organizationName: 'Xyvran',
  projectName: 'tgdcfs',

  onBrokenLinks: 'throw',

  markdown: {
    hooks: {
      onBrokenMarkdownLinks: 'warn',
    },
  },

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      'classic',
      {
        docs: {
          sidebarPath: './sidebars.ts',
          editUrl: 'https://github.com/Xyvran/tgdcfs/tree/master/docusaurus/',
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    image: 'img/docusaurus-social-card.jpg',
    colorMode: {
      defaultMode: 'dark',
      respectPrefersColorScheme: true,
    },
    navbar: {
      title: 'TGDCFS Documentation',
      logo: {
        alt: 'TGDCFS Logo',
        src: 'img/logo.svg',
      },
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'usersSidebar',
          position: 'left',
          label: '👤 Users',
        },
        {
          type: 'docSidebar',
          sidebarId: 'techUsersSidebar',
          position: 'left',
          label: '🛠️ Technical Users',
        },
        {
          type: 'docSidebar',
          sidebarId: 'developersSidebar',
          position: 'left',
          label: '💻 Developers',
        },
        {
          href: 'https://github.com/Xyvran/tgdcfs',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Documentation',
          items: [
            {
              label: 'User Guide',
              to: '/docs/users/',
            },
            {
              label: 'Technical User Guide',
              to: '/docs/technical-users/',
            },
            {
              label: 'Developer Guide',
              to: '/docs/developers/',
            },
          ],
        },
        {
          title: 'Project Links',
          items: [
            {
              label: 'GitHub Repository',
              href: 'https://github.com/Xyvran/tgdcfs',
            },
            {
              label: 'Config Generator',
              href: 'https://xyvran.github.io/tgdcfs/config-generator/',
            },
            {
              label: 'Getting Started Guide',
              href: 'https://xyvran.github.io/tgdcfs/getting-started/',
            },
          ],
        },
        {
          title: 'Community & License',
          items: [
            {
              label: 'MIT License',
              href: 'https://github.com/Xyvran/tgdcfs/blob/master/LICENSE',
            },
            {
              label: 'Docker Hub',
              href: 'https://hub.docker.com/r/xyvran/tgdcfs',
            },
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} TGDCFS Project. Built with Docusaurus.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ['bash', 'yaml', 'json', 'python', 'docker', 'ini', 'markup'],
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
