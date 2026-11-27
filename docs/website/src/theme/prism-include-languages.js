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
  // argparse usage lines in the generated CLI references, e.g. `dlt schema [-h] [--format {json,yaml}] file`,
  // and dlt / dlthub / pip / uv command invocations in the docs
  PrismObject.languages.cli = {
    comment: {
      pattern: /(^|\s)#.*/m,
      lookbehind: true,
    },
    string: /"(?:\\.|[^"\\])*"|'[^']*'/,
    command: [
      {
        pattern: /^(?:uv[ \t]+run[ \t]+)?(?:dlthub|dlt)(?![\w-])(?:[ \t]+[a-z][\w-]*)*/m,
        alias: "function-name",
      },
      // has no subcommands, the first word after it is the source name
      {
        pattern: /^dlt-init-openapi(?![\w-])/m,
        alias: "function-name",
      },
      // package managers: only the tool and its subcommands, so package names stay arguments
      {
        pattern:
          /^(?:uvx|uv|pip3?)(?![\w-])(?:[ \t]+(?:pip|tool|python))?(?:[ \t]+(?:install|uninstall|show|list|freeze|add|remove|run|sync|venv|lock|init))?(?![\w-])/m,
        alias: "function-name",
      },
    ],
    // a subcommand that follows a positional, e.g. `drop` in `dlt pipeline [pipeline_name] drop [-h]`
    subcommand: [
      {
        pattern: /\b[a-z][\w-]*(?=[ \t]+\[-h\])/,
        alias: "function-name",
      },
      // e.g. `show` in `dlt pipeline <pipeline_name> show`
      {
        pattern: /(<[\w -]+>[ \t]+)[a-z][\w-]*/,
        lookbehind: true,
        alias: "function-name",
      },
    ],
    placeholder: {
      pattern: /<[\w -]+>/,
      alias: "variable",
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
      pattern: /(?:\.{1,2}\/|~\/|\b)[A-Za-z_][\w.@/-]*/,
      alias: "variable",
    },
    number: /\b\d+(?:\.\d+)*\b/,
    operator: /\.\.\./,
    punctuation: /[[\]|]/,
  };
  // folder layouts, e.g. `├── .dlt/` with `# comments` or `← notes`
  PrismObject.languages.tree = {
    comment: {
      pattern: /(^|\s)(?:#|←).*/m,
      lookbehind: true,
    },
    "tree-line": /[│├└─┬┼]+|^[ \t]*[|`](?:-+)?/m,
    directory: {
      pattern: /[^\s│├└─|]+\/(?=\s|$)/m,
      alias: "function-name",
    },
    placeholder: {
      pattern: /<[\w -]+>/,
      alias: "variable",
    },
  };
  // REST API endpoint lines in the generated API references, e.g. `GET /api/v1/workspaces/{workspace_id}`
  PrismObject.languages.endpoint = {
    method: {
      pattern: /^[A-Z]+(?=[ \t])/m,
      alias: "keyword",
    },
    "path-parameter": {
      pattern: /\{[^{}\s]+\}/,
      alias: "attr-name",
      inside: { punctuation: /[{}]/ },
    },
    punctuation: /\//,
  };
}
