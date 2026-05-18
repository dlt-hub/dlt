import Admonition from "@theme/Admonition";
import Link from "@docusaurus/Link";

export function DltHubFeatureAdmonition() {
  return (
    <Admonition type="note" title={<span>dltHub Feature</span>}>
      <p>
        This feature requires <Link to="/docs/hub/getting-started/installation">installed <code>dlthub</code> package</Link>. <Link to="https://dlthub.com/waiting-list">join the waiting list</Link> for official access.
        <br/>
        <br/>
        <em><Link to="/docs/hub/license">Copyright © 2026 dltHub Inc. All rights reserved.</Link></em>
      </p>
    </Admonition>
  );
}
