const proc = require('child_process')
const fs = require('fs');
const semver = require('semver')

// const
const REPO_DIR = ".dlt-repo"
const REPO_DOCS_DIR = REPO_DIR + "/docs/website"
const REPO_PREPROCESSED_FILES_DIR = REPO_DOCS_DIR + "/docs_processed"
const REPO_URL = "https://github.com/dlt-hub/dlt.git"
const VERSIONED_DOCS_FOLDER = "versioned_docs"
const VERSIONED_SIDEBARS_FOLDER = "versioned_sidebars"
const ENV_FILE = '.env'

// no doc versions below this version will be deployed
const MINIMUM_SEMVER_VERSION = "1.0.0"

// clear old repo version
fs.rmSync(REPO_DIR, { recursive: true, force: true })

// checkout fresh
console.log("Checking out dlt repo")
fs.rmSync(REPO_DIR, {force: true, recursive: true})
proc.execSync(`git clone ${REPO_URL} ${REPO_DIR}`)

// find tags
console.log("Discovering versions")
const tags = proc.execSync(`cd ${REPO_DIR} && git tag`).toString().trim().split("\n");
console.log(`Found ${tags.length} tags`)

// parse and filter invalid tags
let versions = tags.map(v => semver.valid(v)).filter(v => v != null)

// remove all tags below the min version and sort
min_version = semver.valid(MINIMUM_SEMVER_VERSION)
versions = semver.rsort(versions.filter(v => semver.gt(v, min_version)))

// remove prelease versions
versions.filter(v => semver.prerelease(v) == null)

console.log(`Found ${versions.length} elligible versions`)
console.log(versions)
if (versions.length < 2) {
    console.error("Sanity check failed, not enough elligble version tags found")
    process.exit(1)
}

// write last version into env file
const envFileContent = `DOCUSAURUS_DLT_VERSION=${versions[0]}`;
fs.writeFileSync(ENV_FILE, envFileContent, 'utf8');

// in the future the only other version to build is the minor version below the latest
const selectedVersions = ["master"];
// let lastVersion = versions[0];
// for (let ver of versions) {
//     console.log(semver.minor(ver))
//     console.log(semver.minor(lastVersion))
//     if ( semver.minor(ver) == (semver.minor(lastVersion) - 1)) {
//         selectedVersions.push(ver)
//     }
// }

console.log(`Will create docs versions for ${selectedVersions}`)

// create folders
fs.rmSync(VERSIONED_DOCS_FOLDER, { recursive: true, force: true })
fs.rmSync(VERSIONED_SIDEBARS_FOLDER, { recursive: true, force: true })
fs.rmSync("versions.json", { force: true })

fs.mkdirSync(VERSIONED_DOCS_FOLDER);
fs.mkdirSync(VERSIONED_SIDEBARS_FOLDER);

// check that checked out repo is on devel
console.log("Checking branch")
const branch = proc.execSync(`cd ${REPO_DIR} && git rev-parse --abbrev-ref HEAD`).toString().trim()

// sanity check
if (branch != "devel") {
    console.error("Could not check out devel branch")
    process.exit(1)
}

/**
 * Backfills sidebar keys that are absent from versioned sidebar snapshots.
 *
 * Docusaurus requires every sidebar referenced in navbar items to exist in every
 * versioned snapshot. When a new sidebar is introduced in `devel` after a version
 * was tagged, older snapshots won't have it, causing build failures. This function
 * patches each snapshot JSON file by inserting a fallback entry for each missing
 * sidebar ID, pointing back to the docs root so the navbar tab remains functional.
 *
 * Note: sidebars.js is intentionally NOT required here — it calls walkSync on
 * docs_processed/ at load time, which does not exist during update_versions.js
 * execution in CI.
 *
 * @param {string} versionedSidebarsFolder - Path to the versioned_sidebars directory.
 * @param {string[]} sidebarIds - Sidebar IDs to backfill if absent from a snapshot.
 */
function backfillVersionedSidebars(versionedSidebarsFolder, sidebarIds) {
    const files = fs.readdirSync(versionedSidebarsFolder).filter(f => f.endsWith('.json'));
    for (const file of files) {
        const filePath = `${versionedSidebarsFolder}/${file}`;
        const snapshot = JSON.parse(fs.readFileSync(filePath, 'utf8'));
        let patched = false;
        for (const id of sidebarIds) {
            if (!snapshot[id]) {
                snapshot[id] = [
                    {
                        type: 'category',
                        label: 'Cookbook',
                        link: {
                            type: 'doc',
                            id: 'examples/index',
                        },
                        items: [],
                    },
                    {
                        type: 'category',
                        label: 'Education',
                        link: {
                            type: 'doc',
                            id: 'tutorial/education',
                        },
                        items: [],
                    },
                ];
                patched = true;
            }
        }
        if (patched) {
            fs.writeFileSync(filePath, JSON.stringify(snapshot, null, 2), 'utf8');
            console.log(`Patched ${file}: added missing sidebar keys`);
        }
    }
}

selectedVersions.reverse()
for (const version of selectedVersions) {

    // checkout verison and verify we have the right tag
    console.log(`Generating version ${version}, switching to tag:`)
    proc.execSync(`cd ${REPO_DIR} && git checkout ${version}`)

    // const tag = proc.execSync(`cd ${REPO_DIR} && git describe --exact-match --tags`).toString().trim()
    // if (tag != version) {
    //     console.error(`Could not checkout version ${version}`)
    //     process.exit(1)
    // }

    // clear preprocessed docs in subrepo
    fs.rmSync(REPO_PREPROCESSED_FILES_DIR, { force: true, recursive: true})

    // build doc version, we also run preprocessing and markdown gen for each doc version
    console.log(`Building docs...`)
    proc.execSync(`cd ${REPO_DOCS_DIR} && cd .. && make preprocess-docs`)

    console.log(`Snapshotting version...`)
    proc.execSync(`cd ${REPO_DOCS_DIR} && npx docusaurus docs:version ${version}`)

    console.log(`Moving snapshot`)
    fs.cpSync(REPO_DOCS_DIR+"/"+VERSIONED_DOCS_FOLDER, VERSIONED_DOCS_FOLDER, {recursive: true})
    fs.cpSync(REPO_DOCS_DIR+"/"+VERSIONED_SIDEBARS_FOLDER, VERSIONED_SIDEBARS_FOLDER, {recursive: true})

    backfillVersionedSidebars(VERSIONED_SIDEBARS_FOLDER, ['cookbookSidebar', "educationSidebar"]);
}

fs.cpSync(REPO_DOCS_DIR+"/versions.json", "versions.json")
