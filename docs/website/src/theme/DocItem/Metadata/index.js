import Head from "@docusaurus/Head";
import { useDoc } from "@docusaurus/plugin-content-docs/client";
import { PageMetadata } from "@docusaurus/theme-common";
import React from "react";

// dltHub pages are titled as their own product instead of the site-wide "dlt Docs"
const HUB_TITLE_SUFFIX = "dltHub Docs";

export default function DocItemMetadata() {
  const { metadata, frontMatter, assets } = useDoc();
  // the doc id is relative to the version, so this also covers /docs/devel/hub/...
  const isHubPage = metadata.id.startsWith("hub/");
  const hubTitle = `${metadata.title} | ${HUB_TITLE_SUFFIX}`;

  return (
    <>
      <PageMetadata
        // hub pages set their own title below, so the default suffix is never rendered
        title={isHubPage ? undefined : metadata.title}
        description={metadata.description}
        keywords={frontMatter.keywords}
        image={assets.image ?? frontMatter.image}
      />
      {isHubPage && (
        <Head>
          <title>{hubTitle}</title>
          <meta property="og:title" content={hubTitle} />
        </Head>
      )}
    </>
  );
}
