const fs = require("node:fs");

version_files = ["versions.json", "versioned_docs", "versioned_sidebars"];

for (const f of version_files) {
  fs.rmSync(f, { recursive: true, force: true });
}
