import Admonition from "@theme/Admonition";
import Link from "@docusaurus/Link";

export function DltHubFeatureAdmonition() {
  return (
    <Admonition type="note" title={<span>dltHub Licensed Feature</span>}>
      <p>
        This feature requires <Link to="/docs/hub/getting-started/installation">installed <code>dlthub</code> package</Link> and a <Link to="/docs/hub/getting-started/installation#licensing">license</Link> that you can get from us or self-issue.
        <br/>
        <br/>
        <em><Link to="/docs/hub/EULA">Copyright © 2025 dltHub Inc. All rights reserved.</Link></em>
      </p>
    </Admonition>
  );
}
