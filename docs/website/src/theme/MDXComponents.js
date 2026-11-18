import React from "react";
// Import the original mapper
import MDXComponents from "@theme-original/MDXComponents";
import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";
import { Since } from "../components/VersionBadge";

export default {
  // Re-use the default mapping
  ...MDXComponents,
  Tabs,
  TabItem,
  Since,
  // Map the "<Highlight>" tag to our Highlight component
  // `Highlight` will receive all props that were passed to `<Highlight>` in MDX
};
