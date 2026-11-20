/**
 * Swizzled DocSidebarItem/Category: marks a category that has badged pages anywhere below it, so a reader
 * can follow the badges from the top of the sidebar down to the page. When every page below it carries the
 * same page badge (all new, or all preview) the category gets that badge, otherwise it is "updated".
 */
import React from "react";
import clsx from "clsx";
import Category from "@theme-original/DocSidebarItem/Category";
import { usePluginData } from "@docusaurus/useGlobalData";

/** @returns {{leaves: number, kinds: string[]}} number of pages below, and the badge kind of each badged one */
function collect(item, { badges, updated }) {
  if (item.type === "link") {
    const badge = badges[item.docId] || updated[item.docId];
    return { leaves: 1, kinds: badge ? [badge.kind] : [] };
  }
  if (item.type !== "category") return { leaves: 0, kinds: [] };
  return item.items.reduce(
    (total, child) => {
      const part = collect(child, { badges, updated });
      return { leaves: total.leaves + part.leaves, kinds: [...total.kinds, ...part.kinds] };
    },
    { leaves: 0, kinds: [] }
  );
}

export default function DocSidebarItemCategoryWrapper(props) {
  const { leaves, kinds } = collect(props.item, usePluginData("versioning"));
  if (!kinds.length) return <Category {...props} />;
  const same = kinds.length === leaves && kinds.every((kind) => kind === kinds[0]);
  const kind = same ? kinds[0] : "updated";
  const item = { ...props.item, className: clsx(props.item.className, `sidebar-category-${kind}`) };
  return <Category {...props} item={item} />;
}
