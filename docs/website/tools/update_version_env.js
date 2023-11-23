// creates an .env file with the current version of the dlt for consumption in docusaurus.config.js

const { readFile, writeFile } = require('fs/promises')
const tom = require('toml');

const TOML_FILE = '../../pyproject.toml';
const ENV_FILE = '.env'
 
async function update_env() {  
    const fileContent = await readFile(TOML_FILE, 'utf8');
    const toml = tom.parse(fileContent);
    const version = toml['tool']['poetry']['version'];
    const envFileContent = `DOCUSAURUS_DLT_VERSION=${version}`;
    await writeFile(ENV_FILE, envFileContent, 'utf8');
}

update_env();