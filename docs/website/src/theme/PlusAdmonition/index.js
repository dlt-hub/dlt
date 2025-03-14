import Admonition from "@theme/Admonition";

export function PlusAdmonition() {
  return (
    <Admonition type="note" title={<span style={{ textTransform: "lowercase" }}>dlt+</span>}>
      <p>
        This page is for dlt+, which requires a license. <a href="https://info.dlthub.com/waiting-list">Join our early access program</a> for a trial license.
      </p>
    </Admonition>
  );
}
