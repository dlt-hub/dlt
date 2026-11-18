// run: node --test versioning/
const test = require("node:test");
const assert = require("node:assert");
const { compareVersions, getBadge, validateMeta } = require("./badge");
const { resolveReleased } = require("./released");

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
  assert.equal(getBadge({ status: "preview", since: "1.1.0" }, released).kind, "preview");
  assert.equal(getBadge({}, released), null);
});

test("badges are scoped per package", () => {
  assert.equal(getBadge({ since: "0.6.0", package: "dlthub" }, released).kind, "preview");
  assert.equal(getBadge({ since: "0.6.0" }, released), null); // dlt 1.32 is far past 0.6
  assert.throws(() => getBadge({ since: "1.0.0", package: "dlthub-sdk" }, released), /no released version/);
});

test("validates metadata", () => {
  assert.throws(() => validateMeta({ package: "nope" }, "f"), /unknown package/);
  assert.throws(() => validateMeta({ status: "beta" }, "f"), /unknown status/);
  assert.throws(() => validateMeta({ since: 1.3 }, "f"), /quoted string/);
  validateMeta({ since: "1.3.0", package: "dlthub", status: "preview" }, "f");
});

test("released override: inline, file, and missing package", async () => {
  const env = { DLT_DOCS_RELEASED_VERSIONS: "dlt=1.30.0, dlthub=0.4.0" };
  assert.deepEqual(await resolveReleased(["dlt", "dlthub"], env), { dlt: "1.30.0", dlthub: "0.4.0" });
  await assert.rejects(resolveReleased(["dlthub-sdk"], env), /no version/);
  await assert.rejects(resolveReleased(["pip"], env), /unknown packages/);
});

test("section directive is rewritten into a <Since> heading badge", () => {
  const { preprocess } = require("./directive");
  const out = (fileContent) => preprocess({ fileContent });
  assert.equal(out("## A b\n<!-- since: 1.32.0 -->\n\ntext"), '## A b<Since version="1.32.0" />\n\ntext');
  assert.equal(out("## A\n<!-- since: dlthub 0.5.0 -->"), '## A<Since version="0.5.0" package="dlthub" />');
  assert.equal(out("## A {#id}\n<!-- since: 1.2.0 -->"), '## A<Since version="1.2.0" /> {#id}');
  assert.equal(out("## A\n\ntext <!-- since: 1.2.0 -->"), "## A\n\ntext <!-- since: 1.2.0 -->");
});
