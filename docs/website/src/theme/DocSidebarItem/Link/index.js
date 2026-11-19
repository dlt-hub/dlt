/**
 * Swizzled DocSidebarItem/Link: tags sidebar entries of new/preview pages, and of pages with new/preview sections ("Updated").
 * The badge text is drawn by CSS from the `sidebar-badge-*` class (see custom.css),
 * so the original link and its label stay untouched.
 */
import React from "react";
import clsx from "clsx";
import Link from "@theme-original/DocSidebarItem/Link";
import { usePluginData } from "@docusaurus/useGlobalData";

export default function DocSidebarItemLinkWrapper(props) {
  const { badges, updated } = usePluginData("versioning");
  const badge = badges[props.item.docId] || updated[props.item.docId];
  if (!badge) return <Link {...props} />;
  const item = { ...props.item, className: clsx(props.item.className, `sidebar-badge-${badge.kind}`) };
  return <Link {...props} item={item} />;
}
