import React from "react";
import clsx from "clsx";
import { usePluginData } from "@docusaurus/useGlobalData";
import { getBadge } from "../../versioning/badge";
import styles from "./VersionBadge.module.css";

export function Badge({ badge }) {
  if (!badge) return null;
  return (
    <span className={clsx(styles.badge, styles[badge.kind])} title={`${badge.package} ${badge.label.toLowerCase()}`}>
      {badge.label}
    </span>
  );
}

/** Badge of a whole page, resolved at build time by plugins/versioning.js. */
export function PageBadge({ docId }) {
  const { badges } = usePluginData("versioning");
  return <Badge badge={badges[docId]} />;
}

/**
 * Section badge. Not written by hand: versioning/remark-since.js turns the markdown directive
 * `<!-- since: [package ]1.32.0 -->` below a heading into this. Renders nothing once the feature is older than the "new" window.
 */
export function Since({ version, package: pkg }) {
  const { released } = usePluginData("versioning");
  return <Badge badge={getBadge({ since: version, package: pkg }, released)} />;
}
