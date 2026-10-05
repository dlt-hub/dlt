/**
 * Swizzled DocItem/Layout: adds DocMarkdownLink next to DocVersionBadge.
 */
import React from 'react';
import clsx from 'clsx';
import {useWindowSize} from '@docusaurus/theme-common';
import {
  useDoc,
  useDocsVersion,
} from '@docusaurus/plugin-content-docs/client';
import {getSearchSection} from '../../SearchBar/sections';
import DocItemPaginator from '@theme/DocItem/Paginator';
import DocVersionBanner from '@theme/DocVersionBanner';
import DocVersionBadge from '@theme/DocVersionBadge';
import DocMarkdownLink from '@theme/DocMarkdownLink';
import DocItemFooter from '@theme/DocItem/Footer';
import DocItemTOCMobile from '@theme/DocItem/TOC/Mobile';
import DocItemTOCDesktop from '@theme/DocItem/TOC/Desktop';
import DocItemContent from '@theme/DocItem/Content';
import DocBreadcrumbs from '@theme/DocBreadcrumbs';
import ContentVisibility from '@theme/ContentVisibility';

import styles from './styles.module.css';

function useDocTOC() {
  const {frontMatter, toc} = useDoc();
  const windowSize = useWindowSize();

  const hidden = frontMatter.hide_table_of_contents;
  const canRender = !hidden && toc.length > 0;

  const mobile = canRender ? <DocItemTOCMobile /> : undefined;
  const desktop =
    canRender && (windowSize === 'desktop' || windowSize === 'ssr') ? (
      <DocItemTOCDesktop />
    ) : undefined;

  return {hidden, mobile, desktop};
}

// Pagefind attributes: only the latest version is indexed (no devel/old
// duplicates) and only the doc content (no navbar, sidebar, footer).
function SearchContent({children}) {
  const {metadata} = useDoc();
  const version = useDocsVersion();
  if (!version.isLast) {
    return children;
  }
  const {section, weight} = getSearchSection(metadata.id);
  return (
    <div data-pagefind-body="" data-pagefind-weight={String(weight)}>
      <meta data-pagefind-filter="section[content]" content={section} />
      {children}
    </div>
  );
}

export default function DocItemLayout({children}) {
  const docTOC = useDocTOC();
  const {metadata} = useDoc();
  return (
    <div className="row">
      <div className={clsx('col', !docTOC.hidden && styles.docItemCol)}>
        <ContentVisibility metadata={metadata} />
        <DocVersionBanner />
        <div className={styles.docItemContainer}>
          <article>
            <DocBreadcrumbs />
            <DocVersionBadge />
            <DocMarkdownLink />
            {docTOC.mobile}
            <SearchContent>
              <DocItemContent>{children}</DocItemContent>
            </SearchContent>
            <DocItemFooter />
          </article>
          <DocItemPaginator />
        </div>
      </div>
      {docTOC.desktop && <div className="col col--3">{docTOC.desktop}</div>}
    </div>
  );
}
