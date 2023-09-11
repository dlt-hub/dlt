const fs = require('fs');
const path = require('path');
const dedent = require('dedent');
var watch = require('node-watch');

// docs snippets settings
const BASE_DIR = "./docs/";
const DOCS_EXTENSIONS = [".md", ".mdx"];
const SNIPPETS_FILE_SUFFIX = "-snippets.py"

// examples settings
const EXAMPLES_SOURCE_DIR = "./docs/examples/";
const EXAMPLES_DESTINATION_DIR = "../examples/";
const EXAMPLES_MAIN_SNIPPET_NAME = "example";
const EXAMPLES_CODE_SUBDIR = "/code";

// markers
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

function *listDirsSync(dir) {
    const files = fs.readdirSync(dir, { withFileTypes: true });
    for (const file of files) {
      if (file.isDirectory()) {
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

function getSnippetFromFile(snippetsFileName, snippetName) {
    const lines = fs.readFileSync(snippetsFileName, 'utf8').split(/\r?\n/);
    const snippetMap = buildSnippetMap(lines, snippetsFileName);

    if (!(snippetName in snippetMap)) {
        return undefined;
    }

    let result = lines.slice((snippetMap[snippetName]["start"]+1), snippetMap[snippetName]["end"]);
    // dedent works on strings, not on string arrays, so this is very ineffective unfortunately...
    result = dedent(result.join("\n")).split(/\r?\n/);
    return filterDirectives(result);
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
    const snippet = getSnippetFromFile(snippetsFileName, snippetName);
    if (!snippet) {
        throw new Error(`Could not find requested snippet "${snippetName}" requested in file ${fileName} in file ${snippetsFileName}.`);
    }

    const codeType = path.extname(snippetsFileName).replace(".", "");
    snippet.unshift(`\`\`\`${codeType}`);
    snippet.push("```");

    return snippet;
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

// update the snippets in the md files
function updateSnippets() {
    console.log("Updating Snippets");
    let processedFiles = 0;
    for (const fileName of walkSync(BASE_DIR)) {
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

// sync examples from the website folders to docs/examples
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
            const lines = getSnippetFromFile(fileName, EXAMPLES_MAIN_SNIPPET_NAME);
            if (!lines) {
                continue;
            }
    
            // write file
            const destinationFileName =  exampleDestinationDir + fileName.replace(exampleCodeDir, "").replace("-snippets", "");
            fs.mkdirSync(path.dirname(destinationFileName), { recursive: true });
            fs.writeFileSync(destinationFileName, lines.join("\n"));
        }

    }
}

updateSnippets();
syncExamples();

if (process.argv.includes("--watch")) {
    console.log(`Watching ${BASE_DIR}`)
    let lastUpdate = Date.now();
    watch(BASE_DIR, { recursive: true, filter: /\.(py|toml|md)$/  }, function(evt, name) {
        // break update loop
        if (Date.now() - lastUpdate < 500) {
            return;
        }
        console.log('%s changed...', name);
        updateSnippets();
        syncExamples();
        lastUpdate = Date.now();
    });
}