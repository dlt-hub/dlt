const fs = require('fs');

version_files = [
    "versions.json",
    "versioned_docs",
    "versioned_sidebars"
]

for (let f of version_files) {
    fs.rmSync(f, { recursive: true, force: true })
}