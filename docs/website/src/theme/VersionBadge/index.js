import React from "react";
import clsx from "clsx";
import { usePluginData } from "@docusaurus/useGlobalData";
import { getBadge, parseConstraints } from "../../../plugins/versioning/badge";
import styles from "./styles.module.css";

export function Badge({ badge }) {
  if (!badge) return null;
  return (
    <span className={clsx(styles.badge, styles[badge.kind])} title={badge.title}>
      {badge.label}
    </span>
  );
}

/** Badge of a whole page, resolved at build time by plugins/versioning/index.js. */
export function PageBadge({ docId }) {
  const { badges } = usePluginData("versioning");
  return <Badge badge={badges[docId]} />;
}

/**
 * Section badge. Not written by hand: plugins/versioning/directive.js turns the markdown directives
 * `<!-- since: [package ]1.32.0 -->` below a heading into this, `since` being `package=version,...`.
 * Renders nothing once the feature is older than the "new" window.
 */
export function Since({ since }) {
  const { released, requires } = usePluginData("versioning");
  return <Badge badge={getBadge({ since: parseConstraints(since) }, released, { requires })} />;
}
