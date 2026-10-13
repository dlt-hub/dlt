import prismIncludeLanguages from "@theme-original/prism-include-languages";

export default function includeLanguages(PrismObject) {
  prismIncludeLanguages(PrismObject);
  // prism's python grammar marks only definitions, so parameters, calls, attributes and types stay plain text
  PrismObject.languages.insertBefore("python", "builtin", {
    parameter: {
      pattern: /([(,]\s*)[A-Za-z_]\w*(?=\s*(?::|=(?!=)))/,
      lookbehind: true,
      alias: "attr-name",
    },
    "function-call": {
      pattern: /\b[A-Za-z_]\w*(?=\s*\()/,
      alias: "function",
    },
    attribute: {
      pattern: /(\.)[A-Za-z_]\w*/,
      lookbehind: true,
      alias: "property",
    },
    "type-name": {
      pattern: /\b(?!(?:True|False|None)\b)[A-Z][A-Z0-9_]*[a-z]\w*/,
      alias: "class-name",
    },
  });
}
