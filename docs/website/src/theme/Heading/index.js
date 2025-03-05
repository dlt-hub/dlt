import React from "react";
import Heading from "@theme-original/Heading";
import { useLocation } from "@docusaurus/router";
import { PlusAdmonition } from "../PlusAdmonition";

export default function HeadingWrapper(props) {
  const location = useLocation();
  const showPlus = location.pathname.includes("/plus/");
  const { as } = props;

  if (as === "h1" && showPlus) {
    return (
      <>
        <Heading {...props} />
        <PlusAdmonition />
      </>
    );
  }

  return (
    <>
      <Heading {...props} />
    </>
  );
}
