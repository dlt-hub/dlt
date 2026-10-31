/**
 * Wrapped DocItem/Footer: renders the original footer, then a short "Go further" list of
 * dlthub.com pages for docs that have an entry in src/data/go-further.js.
 */
import React from "react";
import Footer from "@theme-original/DocItem/Footer";
import { useDoc, useDocsVersion, useVersions } from "@docusaurus/plugin-content-docs/client";
import { getGoFurtherLinks } from "@site/src/data/go-further";
import styles from "./styles.module.css";

// "/docs/devel/intro" and "/docs/intro" both become "/docs/intro".
function useVersionlessPath() {
  const { metadata } = useDoc();
  const { pluginId, version } = useDocsVersion();
  const versionPath = (useVersions(pluginId).find((v) => v.name === version)?.path ?? "/docs").replace(/\/+$/, "");
  const rest = metadata.permalink.slice(versionPath.length);
  return `/docs${rest.startsWith("/") ? rest : `/${rest}`}`;
}

function GoFurther({ links }) {
  return (
    <nav className={styles.goFurther} aria-labelledby="go-further-heading">
      <h2 id="go-further-heading" className={styles.heading}>
        Go further
      </h2>
      <ul className={styles.list}>
        {links.map((link) => (
          <li key={link.href} className={styles.item}>
            {/* Plain <a>: these pages live on the dlthub.com app, not in the docs router. */}
            <a className={styles.link} href={link.href}>
              {link.label}
            </a>
            {link.description && <span className={styles.description}>{link.description}</span>}
          </li>
        ))}
      </ul>
    </nav>
  );
}

export default function FooterWrapper(props) {
  const links = getGoFurtherLinks(useVersionlessPath());
  return (
    <>
      <Footer {...props} />
      {links?.length > 0 && <GoFurther links={links} />}
    </>
  );
}
