import React from 'react';
// Import the original mapper
import MDXComponents from '@theme-original/MDXComponents';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import {DLTExampleHeader} from './DLTExampleHeader';

export default {
  // Re-use the default mapping
  ...MDXComponents,
  Tabs,
  TabItem,
  DLTExampleHeader
  // Map the "<Highlight>" tag to our Highlight component
  // `Highlight` will receive all props that were passed to `<Highlight>` in MDX
};