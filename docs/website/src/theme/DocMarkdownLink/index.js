/**
 * Renders a "View Markdown" badge link and a <link rel="alternate"> head tag
 * pointing to the .md version of the current doc page.
 *
 * Used in DocItem/Layout (regular doc pages). NOT used on generated-index
 * pages since those have no source .md files.
 */
import React from 'react';
import clsx from 'clsx';
import Head from '@docusaurus/Head';
import {useLocation} from '@docusaurus/router';

// Pages under these paths have no .md files
const HIDE_MD_PATTERNS = ['/api_reference/'];

export default function DocMarkdownLink({className}) {
  const location = useLocation();
  const mdUrl = `${location.pathname}.md`;
  const show = !HIDE_MD_PATTERNS.some((p) => location.pathname.includes(p));

  if (!show) {
    return null;
  }

  return (
    <>
      <Head>
        <link rel="alternate" type="text/markdown" href={mdUrl} />
      </Head>
      {' '}
      <a
        href={mdUrl}
        className={clsx(className, 'badge badge--secondary')}
        target="_blank"
        rel="noopener noreferrer"
        style={{textDecoration: 'none'}}>
        View Markdown
      </a>
    </>
  );
}
