import Admonition from "@theme/Admonition";
import Link from "@docusaurus/Link";

export function DltHubFeatureAdmonition() {
  return (
    <Admonition type="note" title={<span>dltHub Licensed Feature</span>}>
      <p>
        This feature requires <Link to="/docs/hub/getting-started/installation">installed <code>dlthub</code> package</Link> and an active <Link to="/docs/hub/getting-started/installation#licensing">license</Link>. You can <Link to="/docs/hub/getting-started/installation#self-licensing">self-issue a trial</Link> or <Link to="https://info.dlthub.com/waiting-list">join the waiting list</Link> for official access.
        <br/>
        <br/>
        <em><Link to="/docs/hub/EULA">Copyright © 2025 dltHub Inc. All rights reserved.</Link></em>
      </p>
    </Admonition>
  );
}
