/**
 * Swizzled TOC (page outline): appends a "New" / "Preview" pill to the entries of badged sections and "Updated" to their ancestors.
 * Section badges come from plugins/versioning.js; the TOC renders entry values as HTML.
 */
import React from "react";
import OriginalTOC from "@theme-original/TOC";
import { useDoc } from "@docusaurus/plugin-content-docs/client";
import { usePluginData } from "@docusaurus/useGlobalData";

function useDocId() {
  try {
    return useDoc().metadata.id;
  } catch {
    return null; // not a doc page, e.g. a blog post
  }
}

export default function TOCWrapper({ toc, ...props }) {
  const { sections } = usePluginData("versioning");
  const docId = useDocId();
  const badges = (docId && sections[docId]) || {};
  // a badged heading is a leaf of the outline: its ancestor headings (the nearest lower level above it,
  // repeatedly) are "updated", unless they carry a badge themselves
  const updated = new Set();
  toc.forEach((item, i) => {
    if (!badges[item.id]) return;
    let level = item.level;
    for (let j = i - 1; j >= 0; j--) {
      if (toc[j].level < level) {
        updated.add(toc[j].id);
        level = toc[j].level;
      }
    }
  });
  const pill = (kind, label, title) => `<span class="toc-badge toc-badge--${kind}" title="${title}">${label}</span>`;
  const marked = toc.map((item) => {
    const badge = badges[item.id];
    if (badge) return { ...item, value: item.value + pill(badge.kind, badge.kind === "preview" ? "Preview" : "New", badge.title) };
    if (updated.has(item.id)) return { ...item, value: item.value + pill("updated", "Updated", "has new or preview sections") };
    return item;
  });
  return <OriginalTOC toc={marked} {...props} />;
}
