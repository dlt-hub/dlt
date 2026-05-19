import Admonition from "@theme/Admonition";
import Link from "@docusaurus/Link";
import { useActiveVersion } from "@docusaurus/plugin-content-docs/client";

export function DltHubFeatureAdmonition() {
  // resolve the license page in the active docs version; older snapshots may
  // still expose it as `hub/EULA` until they get regenerated
  const activeVersion = useActiveVersion(undefined);
  const findDoc = (id) => activeVersion?.docs?.find((d) => d.id === id);
  const licenseDoc = findDoc("hub/license") ?? findDoc("hub/EULA");
  const licensePath = licenseDoc?.path ?? "/docs/hub/license";
  return (
    <Admonition type="note" title={<span>dltHub Feature</span>}>
      <p>
        This feature requires <Link to="/docs/hub/getting-started/installation">installed <code>dlthub</code> package</Link>. <Link to="https://dlthub.com/waiting-list">join the waiting list</Link> for official access.
        <br/>
        <br/>
        <em><Link to={licensePath}>Copyright © 2026 dltHub Inc. All rights reserved.</Link></em>
      </p>
    </Admonition>
  );
}
