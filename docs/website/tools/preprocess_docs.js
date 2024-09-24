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

const NUM_TUBA_LINKS = 10;

// examples settings
const EXAMPLES_DESTINATION_DIR = `./${MD_TARGET_DIR}examples/`;
const EXAMPLES_SOURCE_DIR = "../examples/";
const EXAMPLES_EXCLUSIONS = [".", "_", "archive", "local_cache"]

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
  let snippetCount = 0;
  for (let line of lines) {
    if (line.includes(SNIPPET_MARKER)) {
      const snippetName = extractMarkerContent(SNIPPET_MARKER, line);
      snippet = getSnippet(fileName, snippetName);
      result.push(...snippet);
      snippetCount+=1;
    }
    result.push(line);
  }
  return [snippetCount, result];
}


/**
 * Insert tuba links into the markdown file
 */
function insertTubaLinks(lines) {
  const result = []
  let tubaCount = 0;
  for (let line of lines) {
    if (line.includes(TUBA_MARKER)) {
      const tubaTag = extractMarkerContent(TUBA_MARKER, line);
      let links = tubaConfig.filter((link) => link.tags.includes(tubaTag));
      if (links.length > 0) {
        result.push("## Additional Setup guides")
        // shuffle links
        links = links.sort(() => 0.5 - Math.random());
        let count = 0;
        for (const link of links) {          
          result.push(`- [${link.title}](${link.public_url})`)
          count += 1;
          if (count >= NUM_TUBA_LINKS) {
            break;
          }
        }
      } else {
        // we could warn here, but it is a bit too verbose
      }
      tubaCount+=1;
    }
    result.push(line);
  }
  return [tubaCount, result];
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
    let insertedSnippets = 0;
    let processedTubaBlocks = 0;
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
        [snippetCount, lines] = insertSnippets(fileName, lines);
        insertedSnippets += snippetCount;
        [tubaCount, lines] = insertTubaLinks(lines);
        processedTubaBlocks += tubaCount;
        lines = removeRemainingMarkers(lines);

        fs.writeFileSync(targetFileName, lines.join("\n"));
        processedFiles += 1;
    }
    console.log(`Processed ${processedFiles} files.`);
    console.log(`Inserted ${insertedSnippets} snippets.`);
    console.log(`Processed ${processedTubaBlocks} tuba blocks.`);

}


function trimArray(lines) {
  if (lines.length == 0) {
    return lines;
  }
  while (!lines[0].trim()) {
    lines.shift();
  }
  while (!lines[lines.length-1].trim()) {
    lines.pop();
  }
  return lines;
}

/**
 * Sync examples into docs
 */
function syncExamples() {

  let count = 0;
  for (const exampleDir of listDirsSync(EXAMPLES_SOURCE_DIR)) {

    const exampleName = exampleDir.split("/").slice(-1)[0];

    // exclude some folders
    if (EXAMPLES_EXCLUSIONS.some(ex => exampleName.startsWith(ex))) {
      continue;
    }

    const exampleFile = `${EXAMPLES_SOURCE_DIR}${exampleName}/${exampleName}.py`;
    const targetFileName = `${EXAMPLES_DESTINATION_DIR}/${exampleName}.md`;
    const lines = fs.readFileSync(exampleFile, 'utf8').split(/\r?\n/);

    let commentCount = 0;
    let headerCount = 0;

    // separate file content
    const header = []
    const markdown = []
    const code = []

    for (const line of lines) {

      // find file docstring boundaries
      if (line.startsWith(`"""`)) {
        commentCount += 1
        if (commentCount > 2) {
          throw new Error();
         }
         continue;
      }

      // find header boundaries
      if (line.startsWith(`---`)) {
         headerCount += 1;
         if (headerCount > 2) {
          throw new Error();
         }
         continue;
      }

      if (headerCount == 1) {
        header.push(line);
      }
      else if (commentCount == 1) {
        markdown.push(line)
      }
      else if (commentCount == 2) {
        code.push(line);
      }

    }
    
    // if there is no header, do not generate a page
    if (headerCount == 0 ) {
      continue;
    }

    let output = [];

    output.push("---")
    output = output.concat(header);
    output.push("---")

    // add tip
    output.push(":::info")
    const url = `https://github.com/dlt-hub/dlt/tree/devel/docs/examples/${exampleName}`
    output.push(`The source code for this example can be found in our repository at: `)
    output.push(url);
    output.push(":::")

    output.push("## About this Example")
    output = output.concat(trimArray(markdown));

    output.push("### Full source code")
    output.push("```py");
    output = output.concat(trimArray(code));
    output.push("```");

    fs.mkdirSync(path.dirname(targetFileName), { recursive: true });
    fs.writeFileSync(targetFileName, output.join("\n"));
    
    count += 1;
  }
  console.log(`Synced ${count} examples`)
}

// strings to search for, this check could be better but it
// is a quick fix
const HTTP_LINK = "](https://dlthub.com/docs";
const ABS_LINK =  "](/"
const ABS_IMG_LINK =  "](/img"

/**
 * Inspect all md files an run some checks
 */
function checkDocs() {
  let foundError = false;
  for (const fileName of walkSync(MD_SOURCE_DIR)) {
    if (!DOCS_EXTENSIONS.includes(path.extname(fileName))) {
        continue
    }

    // here we simply check that there are no absolute or devel links in the markdown files
    let lines = fs.readFileSync(fileName, 'utf8').split(/\r?\n/);

    for (let [index, line] of lines.entries()) {

      const lineNo = index + 1;
      line = line.toLocaleLowerCase();

      if (line.includes(ABS_LINK) && !line.includes(ABS_IMG_LINK)) {
        foundError = true;
        console.error(`Found absolute md link in file ${fileName}, line ${lineNo}`)
      }
  
      if (line.includes(HTTP_LINK)) {
        foundError = true;
        console.error(`Found http md link referencing these docs in file ${fileName}, line ${lineNo}`)
      }
  
    }



  }

  if (foundError) {
    throw Error("Found one or more errors while checking docs.")
  }
  console.info("Found no errors in md files")
}


function processDocs() {
  fs.rmSync(MD_TARGET_DIR, {force: true, recursive: true})
  syncExamples();
  preprocess_docs();
  checkDocs();
}

processDocs()

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
      processDocs();
      lastUpdate = Date.now();
  });
}