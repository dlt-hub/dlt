// run: node --test versioning/
const test = require("node:test");
const assert = require("node:assert");
const { compareVersions, getBadge, validateMeta } = require("./badge");
const { resolveContext, resolveReleased } = require("./released");

const released = { dlt: "1.32.1", dlthub: "0.5.0" };

test("compares PEP 440 versions incl. pre-releases", () => {
  assert.equal(compareVersions("1.32.0", "1.32.0"), 0);
  assert.equal(compareVersions("1.32", "1.32.0"), 0);
  assert.equal(compareVersions("1.32.0a1", "1.32.0"), -1);
  assert.equal(compareVersions("1.32.0rc1", "1.32.0a5"), 1);
  assert.equal(compareVersions("1.32.0.dev2", "1.32.0a1"), -1);
  assert.equal(compareVersions("1.10.0", "1.9.9"), 1);
  assert.throws(() => compareVersions("banana", "1.0.0"));
});

test("badge transitions: preview -> new -> none", () => {
  const at = (since, rel) => getBadge({ since }, { dlt: rel })?.kind ?? null;
  assert.equal(at("1.33.0", "1.32.1"), "preview");
  assert.equal(at("1.32.0", "1.32.0"), "new");
  assert.equal(at("1.30.0", "1.32.1"), "new");
  assert.equal(at("1.29.0", "1.32.1"), null);
  assert.equal(at("1.33.0", "1.32.1"), "preview");
  // released version being a pre-release means `since` is not out yet
  assert.equal(at("1.32.0", "1.32.0rc1"), "preview");
  assert.equal(at("1.0.0", "2.0.0"), null);
});

test("label, status and no metadata", () => {
  assert.equal(getBadge({ since: "1.31.0" }, released).label, "New in 1.31.0");
  assert.equal(getBadge({ since: "0.5.0", package: "dlthub" }, released).label, "New in dlthub 0.5.0");
  assert.equal(getBadge({ status: "preview", since: "1.1.0" }, released).kind, "preview");
  assert.equal(getBadge({}, released), null);
});

test("badges are scoped per package", () => {
  assert.equal(getBadge({ since: "0.6.0", package: "dlthub" }, released).kind, "preview");
  assert.equal(getBadge({ since: "0.6.0" }, released), null); // dlt 1.32 is far past 0.6
  assert.throws(() => getBadge({ since: "1.0.0", package: "dlthub-client" }, released), /no released version/);
});

test("validates metadata", () => {
  assert.throws(() => validateMeta({ package: "nope" }, "f"), /unknown package/);
  assert.throws(() => validateMeta({ package: "dlthub-sdk" }, "f"), /ships inside "dlthub-client"/);
  assert.throws(() => validateMeta({ status: "beta" }, "f"), /unknown status/);
  assert.throws(() => validateMeta({ since: 1.3 }, "f"), /quoted string/);
  validateMeta({ since: "1.3.0", package: "dlthub", status: "preview" }, "f");
});

test("released override: inline, file, and missing package", async () => {
  const env = { DLT_DOCS_RELEASED_VERSIONS: "dlt=1.30.0, dlthub=0.4.0" };
  assert.deepEqual(await resolveReleased(["dlt", "dlthub"], env), { dlt: "1.30.0", dlthub: "0.4.0" });
  await assert.rejects(resolveReleased(["dlthub-client"], env), /no version/);
  await assert.rejects(resolveReleased(["pip"], env), /unknown packages/);
});

test("section directive is rewritten into a <Since> heading badge", () => {
  const { preprocess } = require("./directive");
  const out = (fileContent) => preprocess({ fileContent });
  assert.equal(out("## A b\n<!-- since: 1.32.0 -->\n\ntext"), '## A b<Since since="dlt=1.32.0" />\n\ntext');
  assert.equal(out("## A\n<!-- since: dlthub 0.5.0 -->"), '## A<Since since="dlthub=0.5.0" />');
  assert.equal(out("## A {#id}\n<!-- since: 1.2.0 -->"), '## A<Since since="dlt=1.2.0" /> {#id}');
  assert.equal(out("## A\n\ntext <!-- since: 1.2.0 -->"), "## A\n\ntext <!-- since: 1.2.0 -->");
});

test("stacked constraints: dictionary since, all must be released", () => {
  const rel = { dlt: "1.32.1", "dlthub-client": "0.4.0" };
  const at = (since, r = rel) => getBadge({ since }, r)?.kind ?? null;
  assert.equal(at({ "dlthub-client": "0.4.0", dlt: "1.32.0" }), "new");
  assert.equal(at({ "dlthub-client": "0.5.0", dlt: "1.32.0" }), "preview");
  assert.equal(at({ "dlthub-client": "0.4.0", dlt: "1.33.0" }), "preview");
  // new while any constraint is inside the window
  assert.equal(at({ "dlthub-client": "0.4.0", dlt: "1.1.0" }), "new");
  assert.equal(at({ "dlthub-client": "0.1.0", dlt: "1.1.0" }, { dlt: "1.32.1", "dlthub-client": "0.9.0" }), null);
  assert.equal(getBadge({ since: { "dlthub-client": "0.4.0", dlt: "1.32.0" } }, rel).label, "New in dlthub-client 0.4.0, 1.32.0");
});

test("implied lower bounds of a released version only matter while unreleased", () => {
  const requires = { "dlthub-client@0.4.0": { dlt: "1.33.0" } };
  const meta = { since: "0.4.0", package: "dlthub-client" };
  const badge = getBadge(meta, { dlt: "1.32.1", "dlthub-client": "0.4.0" }, { requires });
  assert.equal(badge.kind, "preview");
  assert.match(badge.title, /requires dlt >= 1\.33\.0/);
  const ok = getBadge(meta, { dlt: "1.33.0", "dlthub-client": "0.4.0" }, { requires });
  assert.equal(ok.kind, "new");
  assert.match(ok.title, /requires dlt >= 1\.33\.0/);
  // an explicit constraint that already covers the bound is not repeated
  const covered = getBadge({ since: { "dlthub-client": "0.4.0", dlt: "1.33.0" } }, { dlt: "1.33.0", "dlthub-client": "0.4.0" }, { requires });
  assert.doesNotMatch(covered.title, /requires/);
  // the window only counts explicit constraints
  assert.equal(getBadge(meta, { dlt: "1.40.0", "dlthub-client": "0.9.0" }, { requires }), null);
});

test("validates dictionary since", () => {
  validateMeta({ since: { dlt: "1.3.0", dlthub: "0.5.0" } }, "f");
  assert.throws(() => validateMeta({ since: { nope: "1.0.0" } }, "f"), /unknown package/);
  assert.throws(() => validateMeta({ since: { dlt: 1.3 } }, "f"), /quoted string/);
  assert.throws(() => validateMeta({ since: {} }, "f"), /empty/);
  assert.throws(() => validateMeta({ since: { dlt: "1.3.0" }, package: "dlt" }, "f"), /cannot be combined/);
});

test("stacked section comments become one <Since>", () => {
  const { preprocess } = require("./directive");
  const out = (fileContent) => preprocess({ fileContent });
  assert.equal(
    out("## A\n<!-- since: dlthub-client 0.4.0 -->\n<!-- since: 1.32.0 -->\n\ntext"),
    '## A<Since since="dlthub-client=0.4.0,dlt=1.32.0" />\n\ntext'
  );
  assert.throws(() => out("## A\n<!-- since: dlt 1.0.0 -->\n<!-- since: 1.1.0 -->"), /given twice/);
  assert.throws(() => out("## A\n<!-- since: banana -->"), /cannot parse/);
});

test("requirements override and PyPI-free context", async () => {
  const { fromRequiresOverride } = require("./released");
  assert.deepEqual(fromRequiresOverride("dlthub-client@0.4.0=dlt>=1.30.0; dlthub-client@0.5.0=dlt>=1.31.0"), {
    "dlthub-client@0.4.0": { dlt: "1.30.0" },
    "dlthub-client@0.5.0": { dlt: "1.31.0" },
  });
  assert.throws(() => fromRequiresOverride("dlthub-client@0.4.0=dlt<2"), /cannot parse/);
  const env = { DLT_DOCS_RELEASED_VERSIONS: "dlthub-client=0.4.0,dlt=1.32.0", DLT_DOCS_REQUIRES: "dlthub-client@0.4.0=dlt>=1.30.0" };
  const ctx = await resolveContext([{ "dlthub-client": "0.4.0" }], env);
  assert.deepEqual(ctx.requires, { "dlthub-client@0.4.0": { dlt: "1.30.0" } });
  assert.equal(ctx.released.dlt, "1.32.0");
  // offline without DLT_DOCS_REQUIRES: nothing implied, no network
  assert.deepEqual((await resolveContext([{ "dlthub-client": "0.4.0" }], { DLT_DOCS_RELEASED_VERSIONS: "dlthub-client=0.4.0" })).requires, {});
});
