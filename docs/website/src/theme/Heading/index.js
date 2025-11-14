import React from "react";
import Heading from "@theme-original/Heading";
import { useLocation } from "@docusaurus/router";
import { DltHubFeatureAdmonition } from "../DltHubFeatureAdmonition";

export default function HeadingWrapper(props) {
  const location = useLocation();
  const showHub = false; //location.pathname.includes("/hub/");
  const { as } = props;

  if (as === "h1" && showHub) {
    return (
      <>
        <Heading {...props} />
        <DltHubFeatureAdmonition />
      </>
    );
  }

  return (
    <>
      <Heading {...props} />
    </>
  );
}
