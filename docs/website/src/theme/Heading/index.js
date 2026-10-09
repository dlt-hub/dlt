import React, { useContext } from "react";
import Heading from "@theme-original/Heading";
import { useLocation } from "@docusaurus/router";
import { DltHubFeatureAdmonition } from "../DltHubFeatureAdmonition";
import { headingWeight, SearchWeightContext } from "../SearchBar/sections";

export default function HeadingWrapper(props) {
  const location = useLocation();
  const showHub = false; //location.pathname.includes("/hub/");
  const { as } = props;
  const searchWeight = useContext(SearchWeightContext);
  const headingProps =
    searchWeight === null
      ? props
      : { ...props, "data-pagefind-weight": String(headingWeight(searchWeight, as)) };

  if (as === "h1" && showHub) {
    return (
      <>
        <Heading {...headingProps} />
        <DltHubFeatureAdmonition />
      </>
    );
  }

  return (
    <>
      <Heading {...headingProps} />
    </>
  );
}
