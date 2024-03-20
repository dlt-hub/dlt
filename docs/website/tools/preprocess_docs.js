const fs = require('fs');
const path = require('path');
const watch = require('node-watch');
const fetch = require('sync-fetch');
const { get } = require('http');
const dedent = require('dedent');

// constants
const MD_SOURCE_DIR = "docs/";
const MD_TARGET_DIR = "docs_processed/";

const MOVE_FILES_EXTENSION = [".md", ".mdx", ".py", ".png", ".jpg", ".jpeg"];
const DOCS_EXTENSIONS = [".md", ".mdx"];

const SNIPPETS_FILE_SUFFIX = "-snippets.py"

// examples settings
const EXAMPLES_SOURCE_DIR = "./docs/examples/";
const EXAMPLES_DESTINATION_DIR = "../examples/";
const EXAMPLES_MAIN_SNIPPET_NAME = "example";
const EXAMPLES_CODE_SUBDIR = "/code";

// markers
const DLT_MARKER = "@@@DLT";
const TUBA_MARKER = `${DLT_MARKER}_TUBA`;
const SNIPPET_MARKER = `${DLT_MARKER}_SNIPPET`;
const SNIPPET_START_MARKER = `${DLT_MARKER}_SNIPPET_START`;
const SNIPPET_END_MARKER = `${DLT_MARKER}_SNIPPET_END`;

/**
 * Fetch tuba config
 */
const tubaConfig = fetch('https://dlthub.com/docs/pipelines/links.json', {
  headers: {
    Accept: 'application/vnd.citationstyles.csl+json'
  }
}).json();


/**
 * Yield all files in docs dir
 */
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

/**
 * List directories in dir
 */
function *listDirsSync(dir) {
    const files = fs.readdirSync(dir, { withFileTypes: true });
    for (const file of files) {
      if (file.isDirectory()) {
        yield path.join(dir, file.name);
      }
    }
}


/**
 * Extract the snippet or tuba tag name from a line
 */
const extractMarkerContent = (tag, line) => {
  if (line && line.includes(tag)) {
      // strip out md start and end comments
      line = line.replace("-->", "");
      line = line.replace("<!--", "");
      const words = line.split(" ");
      const tagIndex = words.findIndex(w => w==tag);
      return words[tagIndex+1].trim();
  }
  return undefined;
}

/**
 * Runs through an array or lines from a file and builds a map of found snippets
 */
function buildSnippetMap(lines, fileName) {
  const snippetMap = {};
  for (let lineIndex in lines) {
      let line = lines[lineIndex];
      let snippetName;
      line.trimEnd();
      if (snippetName = extractMarkerContent(SNIPPET_START_MARKER, line)) {
          snippetMap[snippetName] = {
              start: parseInt(lineIndex),
          };
      }
      if (snippetName = extractMarkerContent(SNIPPET_END_MARKER, line)) {
          if (snippetName in snippetMap) {
              snippetMap[snippetName]["end"] = parseInt(lineIndex);
          } else {
              throw new Error(`Found end tag for snippet "${snippetName}" but start tag not found! File ${fileName}.`);
          }
      }
  }
  return snippetMap;
}

/** 
 * Get snippet from file
 */
function getSnippetFromFile(snippetsFileName, snippetName) {
  const lines = fs.readFileSync(snippetsFileName, 'utf8').split(/\r?\n/);
  const snippetMap = buildSnippetMap(lines, snippetsFileName);

  if (!(snippetName in snippetMap)) {
      return undefined;
  }

  let result = lines.slice((snippetMap[snippetName]["start"]+1), snippetMap[snippetName]["end"]);
  // dedent works on strings, not on string arrays, so this is very ineffective unfortunately...
  result = dedent(result.join("\n")).split(/\r?\n/);
  return result;
}

/**
 * Get snippet from file
 */
function getSnippet(fileName, snippetName) {

  // regular snippet
  const ext = path.extname(fileName);
  const snippetParts = snippetName.split("::"); 

  // regular snippet
  let snippetsFileName = fileName.slice(0, -ext.length) + SNIPPETS_FILE_SUFFIX;
  if (snippetParts.length > 1) {
      snippetsFileName = path. dirname(fileName) + "/" + snippetParts[0];
      snippetName = snippetParts[1];
  }
  const snippet = getSnippetFromFile(snippetsFileName, snippetName);
  if (!snippet) {
      throw new Error(`Could not find requested snippet "${snippetName}" requested in file ${fileName} in file ${snippetsFileName}.`);
  }

  const codeType = path.extname(snippetsFileName).replace(".", "");
  snippet.unshift(`\`\`\`${codeType}`);
  snippet.push("```");

  return snippet;
}

/**
 * Insert snippets into the markdown file
 */
function insertSnippets(fileName, lines) {
  const result = []
  for (let line of lines) {
    if (line.includes(SNIPPET_MARKER)) {
      const snippetName = extractMarkerContent(SNIPPET_MARKER, line);
      snippet = getSnippet(fileName, snippetName);
      result.push(...snippet);
    }
    result.push(line);
  }
  return result;
}


/**
 * Insert tuba links into the markdown file
 */
function insertTubaLinks(lines) {
  const result = []
  for (let line of lines) {
    if (line.includes(TUBA_MARKER)) {
      const tubaTag = extractMarkerContent(SNIPPET_MARKER, line);
      const links = tubaConfig.filter((link) => link.tags.includes(tubaTag));
      if (links.length > 0) {
        result.push("## Additional Setup guides")
        for (const link of links) {
          result.push(`- [${link.title}](${link.public_url})`)
        }
      } else {
        // we could warn here, but it is a bit too verbose
      }
    }
    result.push(line);
  }
  return result;
}

/** 
 * Remove all lines that contain a DLT_MARKER
 * TODO: we should probably warn here if we find a DLT_MARKER
 * that was not processed before
 */
function removeRemainingMarkers(lines) {
  return lines.filter((line) => !line.includes(DLT_MARKER));
}

/**
 * Preprocess all docs in the docs folder
 */
function preprocess_docs() {
    console.log("Processing docs...");
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
        lines = insertSnippets(fileName, lines);
        lines = insertTubaLinks(lines);
        lines = removeRemainingMarkers(lines);

        fs.writeFileSync(targetFileName, lines.join("\n"));
        processedFiles += 1;
    }
    console.log(`Processed ${processedFiles} files.`);
}


/**
 * Sync examples into examples folder
 */
function syncExamples() {
  for (const exampleDir of listDirsSync(EXAMPLES_SOURCE_DIR)) {
      const exampleName = exampleDir.split("/").slice(-1)[0];
      const exampleDestinationDir = EXAMPLES_DESTINATION_DIR + exampleName;

      // clear example destination dir
      fs.rmSync(exampleDestinationDir, { recursive: true, force: true });
      // create __init__.py
      fs.mkdirSync(exampleDestinationDir, { recursive: true });
      fs.writeFileSync(exampleDestinationDir + "/__init__.py", "");

      // walk all files of example and copy to example destination
      const exampleCodeDir = exampleDir + EXAMPLES_CODE_SUBDIR;
      for (const fileName of walkSync(exampleCodeDir)) {
          let lines = getSnippetFromFile(fileName, EXAMPLES_MAIN_SNIPPET_NAME);
          if (!lines) {
              continue;
          }
          lines = removeRemainingMarkers(lines);

          // write file
          const destinationFileName =  exampleDestinationDir + fileName.replace(exampleCodeDir, "").replace("-snippets", "");
          fs.mkdirSync(path.dirname(destinationFileName), { recursive: true });
          fs.writeFileSync(destinationFileName, lines.join("\n"));
      }
  }
}

syncExamples();
preprocess_docs();

/**
 * Watch for changes and preprocess the docs if --watch cli command flag is present
 */
if (process.argv.includes("--watch")) {
  console.log(`Watching...`)
  let lastUpdate = Date.now();
  watch("../", { recursive: true, filter: /\.(py|toml|md)$/  }, function(evt, name) {
      // break update loop
      if (Date.now() - lastUpdate < 500) {
          return;
      }
      console.log('%s changed...', name);
      syncExamples();
      preprocess_docs();
      lastUpdate = Date.now();
  });
}