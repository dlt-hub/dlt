/**
 * Resolves the latest *released* version of each package, at build time.
 *
 * Source is picked in one place, `resolveReleased`:
 *   - env `DLT_DOCS_RELEASED_VERSIONS` set: a JSON file path, or inline `dlt=1.32.0,dlthub=0.5.0`
 *     (used to inspect the version badges locally, see website/README.md)
 *   - otherwise PyPI.
 * Add another source (git tag, private index) by adding a branch there.
 */
const fs = require("node:fs");
const { PACKAGES } = require("./badge");

const ENV_VAR = "DLT_DOCS_RELEASED_VERSIONS";

function fromOverride(value) {
  if (value.includes("=")) {
    return Object.fromEntries(value.split(",").map((pair) => pair.split("=").map((s) => s.trim())));
  }
  return JSON.parse(fs.readFileSync(value, "utf8"));
}

/** PyPI `info.version` is the latest non-prerelease, non-yanked release */
async function fromPyPI(pkg) {
  const response = await fetch(`https://pypi.org/pypi/${pkg}/json`);
  if (!response.ok) throw new Error(`PyPI returned ${response.status} for "${pkg}"`);
  return (await response.json()).info.version;
}

/**
 * @param {string[]} packages only these are resolved, so an override needs to list just the used ones
 * @returns {Promise<Record<string, string>>}
 */
async function resolveReleased(packages, env = process.env) {
  const unknown = packages.filter((p) => !PACKAGES.includes(p));
  if (unknown.length) throw new Error(`unknown packages: ${unknown.join(", ")}`);
  const override = env[ENV_VAR] ? fromOverride(env[ENV_VAR]) : null;
  const released = {};
  for (const pkg of packages) {
    if (override) {
      if (!override[pkg]) throw new Error(`${ENV_VAR} has no version for "${pkg}"`);
      released[pkg] = override[pkg];
    } else {
      try {
        released[pkg] = await fromPyPI(pkg);
      } catch (e) {
        throw new Error(`cannot resolve released version of "${pkg}" from PyPI (${e.message}); set ${ENV_VAR} to work offline`);
      }
    }
  }
  return released;
}

module.exports = { ENV_VAR, resolveReleased };
