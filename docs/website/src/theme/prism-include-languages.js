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
  // argparse usage lines in the generated CLI references, e.g. `dlt schema [-h] [--format {json,yaml}] file`
  PrismObject.languages.cli = {
    command: {
      pattern: /^(?:dlt|dlthub)\b(?:[ \t]+[a-z][\w-]*)*/m,
      alias: "function-name",
    },
    // a subcommand that follows a positional, e.g. `drop` in `dlt pipeline [pipeline_name] drop [-h]`
    subcommand: {
      pattern: /\b[a-z][\w-]*(?=[ \t]+\[-h\])/,
      alias: "function-name",
    },
    option: {
      pattern: /(^|[\s[|])--?[A-Za-z][\w-]*/,
      lookbehind: true,
      alias: "property",
    },
    choices: {
      pattern: /\{[^{}]*\}/,
      inside: { punctuation: /[{},]/, constant: /[^{},]+/ },
    },
    argument: {
      pattern: /\b[A-Za-z_][\w-]*/,
      alias: "variable",
    },
    operator: /\.\.\./,
    punctuation: /[[\]|]/,
  };
}
