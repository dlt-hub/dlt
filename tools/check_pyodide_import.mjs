// Imports the built dlt wheel inside a pyodide (emscripten) VM.
//
// Usage:
//
//     node tools/check_pyodide_import.mjs <dist-dir>
//
// Booting the VM and resolving the dependency tree both need the network, so any
// failure before dlt is installed is an environment problem and skips the check.
// Only an installed dlt that cannot be imported fails it.

import fs from "node:fs";
import path from "node:path";

// dists our markers exclude on emscripten; a missing module outside this set means
// micropip installed an incomplete tree rather than dlt reaching for a banned dep
const EXCLUDED_ON_EMSCRIPTEN = ["orjson", "win32api", "win_precise_time"];
const INSTALL_ATTEMPTS = 3;

const distDir = process.argv[2] ?? "dist";
const wheels = fs.existsSync(distDir)
  ? fs.readdirSync(distDir).filter((f) => f.startsWith("dlt-") && f.endsWith(".whl"))
  : [];
if (wheels.length !== 1) {
  console.error(`FAILED: expected exactly one dlt wheel in ${distDir}/, found ${wheels.length}`);
  process.exit(1);
}
const wheel = wheels[0];

let py;
try {
  const { loadPyodide } = await import("pyodide");
  py = await loadPyodide({ stdout: () => {}, stderr: () => {} });
  // micropip parses name and version out of the filename, so keep it verbatim
  py.FS.writeFile(`/${wheel}`, new Uint8Array(fs.readFileSync(path.join(distDir, wheel))));
  await py.loadPackage("micropip");
  await py.loadPackage("six");
} catch (e) {
  skip("could not start a pyodide VM", e);
}

let installed = false;
for (let attempt = 1; attempt <= INSTALL_ATTEMPTS && !installed; attempt++) {
  try {
    await py.pyimport("micropip").install(`emfs:/${wheel}`);
    installed = true;
  } catch (e) {
    if (attempt === INSTALL_ATTEMPTS) skip(`could not install ${wheel} under emscripten`, e);
    console.error(`micropip attempt ${attempt} failed, retrying`);
  }
}

try {
  py.runPython("import dlt; import dlt.destinations; from dlt.destinations import filesystem");
} catch (e) {
  const missing = /No module named '([^']+)'/.exec(String(e?.message ?? e))?.[1];
  const banned = missing && EXCLUDED_ON_EMSCRIPTEN.includes(missing.split(".")[0]);
  if (!banned) skip(`micropip left '${missing ?? "a dependency"}' unusable`, e);
  console.error("FAILED: dlt is installed under emscripten but does not import");
  console.error(`a module on the import path imports '${missing}', which is excluded here`);
  console.error(tail(e));
  process.exit(1);
}

const report = py.runPython(`
import sys, dlt
from dlt.common.json import json
f"dlt {dlt.__version__} imports on {sys.platform} using the {json._impl_name} json backend"
`);
console.log(report);

function tail(e, n = 12) {
  return String(e?.message ?? e)
    .split("\n")
    .map((l) => l.trim())
    .filter((l) => l && l.length < 200)
    .slice(-n)
    .join("\n");
}

function skip(summary, e) {
  console.error(`SKIPPED: ${summary}`);
  console.error(e);
  console.error("this is a pyodide/PyPI environment problem, not a dlt import problem");
  process.exit(1);
}
