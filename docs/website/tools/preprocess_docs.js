const fs = require('fs');
const path = require('path');

// constants
const MD_SOURCE_DIR = "docs/";
const MD_TARGET_DIR = "docs_processed/";

const MOVE_FILES_EXTENSION = [".md", ".mdx", ".py", ".png", ".jpg", ".jpeg"];
const DOCS_EXTENSIONS = [".md", ".mdx"];

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
        const lines = fs.readFileSync(fileName, 'utf8').split(/\r?\n/);
        const updatedLines = lines;

        // insert something to see wether it works
        lines.splice(8, 0, "Generated")

        fs.writeFileSync(targetFileName, updatedLines.join("\n"));
    }
    console.log(`Processed ${processedFiles} files.`);
}

preprocess_docs();