const fs = require('fs');
const path = require('path');
const watch = require('node-watch');
const fetch = require('sync-fetch')

// constants
const MD_SOURCE_DIR = "docs/";
const MD_TARGET_DIR = "docs_processed/";

const MOVE_FILES_EXTENSION = [".md", ".mdx", ".py", ".png", ".jpg", ".jpeg"];
const DOCS_EXTENSIONS = [".md", ".mdx"];

// markers
const TUBA_MARKER = "@@@DLT_TUBA";
const DLT_MARKER = "@@@DLT";

// fetch tuba config. TODO: make this fail gracefully
const tubaConfig = fetch('https://dlthub.com/docs/pipelines/links.json', {
  headers: {
    Accept: 'application/vnd.citationstyles.csl+json'
  }
}).json();

// yield all files in dir
function *walkSync(dir) {
    const files = fs.readdirSync(dir, { withFileTypes: true });
    for (const file of files) {
      if (file.isDirectory()) {
        yield* walkSync(path.join(dir, file.name));
      } else {
        yield path.join(dir, file.name);
      }
    }
  }

// takes a line with a dlt marker and returns only the trimmed content
function extractMarkerContent(line, marker) {
  line = line.replace("-->", "");
  line = line.replace("<!--", "");
  line = line.replace(marker, "");
  line = line.trim();
  return line
}

function insertTubaLinks(lines) {
  const result = []
  for (let line of lines) {
    if (line.includes(TUBA_MARKER)) {
      const tubaTag = extractMarkerContent(line, TUBA_MARKER);
      const links = tubaConfig.filter((link) => link.tags.includes(tubaTag));
      if (links.length > 0) {
        result.push("## Additional Setup guides")
        for (const link of links) {
          result.push(`- [${link.title}](${link.public_url})`)
        }
      } else {
        logger.warn(`No tuba links found for tag ${tubaTag}`)
      }
    }
    result.push(line);
  }
  return result;
}

function removeRemainingMarkers(lines) {
  return lines.filter((line) => !line.includes(DLT_MARKER));
}


function preprocess_docs() {
    console.log("Updating Snippets");
    let processedFiles = 0;
    for (const fileName of walkSync(MD_SOURCE_DIR)) {
        if (!MOVE_FILES_EXTENSION.includes(path.extname(fileName))) {
            continue
        }

        // target name, where to copy the file
        const targetFileName = fileName.replace(MD_SOURCE_DIR, MD_TARGET_DIR);
        fs.mkdirSync(path.dirname(targetFileName), { recursive: true });
        
        // if not markdown file, just copy
        if (!DOCS_EXTENSIONS.includes(path.extname(fileName))) {
            fs.copyFileSync(fileName, targetFileName);
            continue
        }
  
        // copy docs to correct folder
        let lines = fs.readFileSync(fileName, 'utf8').split(/\r?\n/);

        // insert stuff
        lines = insertTubaLinks(lines);
        lines = removeRemainingMarkers(lines);

        fs.writeFileSync(targetFileName, lines.join("\n"));
        processedFiles += 1;
    }
    console.log(`Processed ${processedFiles} files.`);
}

preprocess_docs();

if (process.argv.includes("--watch")) {
  console.log(`Watching...`)
  let lastUpdate = Date.now();
  watch("../", { recursive: true, filter: /\.(py|toml|md)$/  }, function(evt, name) {
      // break update loop
      if (Date.now() - lastUpdate < 500) {
          return;
      }
      console.log('%s changed...', name);
      preprocess_docs();
      lastUpdate = Date.now();
  });
}