const fs = require('fs');
const path = require('path');
const dedent = require('dedent');
var watch = require('node-watch');

const BASE_DIR = "./docs/";
const DOCS_EXTENSIONS = [".md", ".mdx"];
const SNIPPETS_FILE_SUFFIX = "-snippets.py"

const DLT_MARKER = "@@@DLT";
const START_MARKER = DLT_MARKER + "_SNIPPET_START";
const END_MARKER = DLT_MARKER + "_SNIPPET_END";

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

// extract the snippet name from a line
const extractSnippetName = (tag, line) => {
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

// filter out all lines that contain a DLT_MARKER
const filterDirectives = (lines) => {
    return lines.filter((line) => !line.includes(DLT_MARKER));
}

// run through a file and extract a map of snippets
function buildSnippetMap(lines, fileName) {
    const snippetMap = {};
    for (let lineIndex in lines) {
        let line = lines[lineIndex];
        let snippetName;
        line.trimEnd();
        if (snippetName = extractSnippetName(START_MARKER, line)) {
            snippetMap[snippetName] = {
                start: parseInt(lineIndex),
            };
        }
        if (snippetName = extractSnippetName(END_MARKER, line)) {
            if (snippetName in snippetMap) {
                snippetMap[snippetName]["end"] = parseInt(lineIndex); 
            } else {
                throw new Error(`Found end tag for snippet "${snippetName}" but start tag not found! File ${fileName}.`);
            }
        }
    }
    return snippetMap;
}

// get the right snippet for a file
function getSnippet(fileName, snippetName) {
    const ext = path.extname(fileName);
    const snippetParts = snippetName.split("::");
    let snippetsFileName = fileName.slice(0, -ext.length) + SNIPPETS_FILE_SUFFIX;
    if (snippetParts.length > 1) {
        snippetsFileName = path. dirname(fileName) + "/" + snippetParts[0];
        snippetName = snippetParts[1];
    }
    const lines = fs.readFileSync(snippetsFileName, 'utf8').split(/\r?\n/);
    const snippetMap = buildSnippetMap(lines, snippetsFileName);

    if (!(snippetName in snippetMap)) {
        throw new Error(`Could not find requested snippet "${snippetName}" requested in file ${fileName} in file ${snippetsFileName}.`);
    }

    let result = lines.slice((snippetMap[snippetName]["start"]+1), snippetMap[snippetName]["end"]);
    // dedent works on strings, not on string arrays, so this is very ineffective unfortunately...
    result = dedent(result.join("\n")).split(/\r?\n/);
    const codeType = path.extname(snippetsFileName).replace(".", "");
    result.unshift(`\`\`\`${codeType}`);
    result.push("```");
    return filterDirectives(result);
}

function insertSnippets(lines, fileName, onlyClear) {
    const result = [];
    let currentSnippet = undefined;
    let snippetsUpdated = false;
    for (let line of lines) {
        let snippetName;
        if (snippetName = extractSnippetName(END_MARKER, line)) {
            if (currentSnippet != snippetName) {
                throw new Error(`Found end tag for snippet "${snippetName}" but tag for snippet never opened! File ${fileName}.`);
            }
            if (!onlyClear) {
                const snippet = getSnippet(fileName, currentSnippet)
                result.push(...snippet)
            }
            snippetsUpdated = true;
            currentSnippet = undefined;
        }
        if (currentSnippet === undefined) {
            result.push(line);
        }
        if (snippetName = extractSnippetName(START_MARKER, line)) {
            if (currentSnippet) {
                throw new Error(`Found start tag for snippet "${snippetName}" but tag for snippet "${currentSnippet}" not closed yet! File ${fileName}.`);
            }
            currentSnippet = snippetName;
        }
    }
    return [result, snippetsUpdated];
}

// update the snippets
function updateSnippets(dir) {
    console.log("Updating Snippets");
    let processedFiles = 0;
    for (const fileName of walkSync(dir)) {
        if (!DOCS_EXTENSIONS.includes(path.extname(fileName))) {
            continue
        }
        const lines  = fs.readFileSync(fileName, 'utf8').split(/\r?\n/);
        const [updatedLines, snippetsUpdated] = insertSnippets(lines, fileName);
        if (snippetsUpdated) {
            processedFiles += 1;
            fs.writeFileSync(fileName, updatedLines.join("\n"));
        }
    }
    console.log(`Processed ${processedFiles} files.`);
}

updateSnippets(BASE_DIR);

if (process.argv.includes("--watch")) {
    console.log(`Watching ${BASE_DIR}`)
    let lastUpdate = Date.now();
    watch(BASE_DIR, { recursive: true, filter: /\.(py|toml|md)$/  }, function(evt, name) {
        // break update loop
        if (Date.now() - lastUpdate < 500) {
            return;
        }
        console.log('%s changed...', name);
        updateSnippets(BASE_DIR);
        lastUpdate = Date.now();
    });
}