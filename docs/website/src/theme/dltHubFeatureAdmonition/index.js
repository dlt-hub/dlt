import Admonition from "@theme/Admonition";

export function dltHubFeatureAdmonition() {
  return (
    <Admonition type="note" title={<span style={{ textTransform: "lowercase" }}>dltHub Feature</span>}>
      <p>
        This page is for dltHub Feature, which requires a license. <a href="https://info.dlthub.com/waiting-list">Join our early access program</a> for a trial license.
      </p>
    </Admonition>
  );
}
