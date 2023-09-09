import React from 'react';
// Import the original mapper
import MDXComponents from '@theme-original/MDXComponents';
import {DltSnippet} from './DltSnippet';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

export default {
  // Re-use the default mapping
  ...MDXComponents,
  DltSnippet: DltSnippet,
  Tabs: Tabs,
  TabItem: TabItem,
  // Map the "<Highlight>" tag to our Highlight component
  // `Highlight` will receive all props that were passed to `<Highlight>` in MDX
};