import CodeBlock from '@theme/CodeBlock';
import React from 'react';
import dedent from 'dedent';

const DLT_MARKER = "@@@DLT";
const START_MARKER = DLT_MARKER + "_SNIPPET_START";
const END_MARKER = DLT_MARKER + "_SNIPPET_END";

// extract the snippet name from a line
const extractSnippetName = (line) => {
    const words = line.split(" ");
    return words[words.length-1].trim();
}

// filter out all lines that contain a DLT_MARKER
const filterDirectives = (lines) => {
    return lines.filter((line) => !line.includes(DLT_MARKER));
}

// extract a snippet from a string
const extractSnippet = (input, snippet) => {

    let lines = input.split(/\r?\n/);

    const snippetMap = {};
    for (let lineIndex in lines) {
        let line = lines[lineIndex];
        line.trimEnd();
        if (line.includes(START_MARKER)) {
            const snippetName = extractSnippetName(line);
            snippetMap[snippetName] = {
                start: parseInt(lineIndex),
            };
        }
        if (line.includes(END_MARKER)) {
            const snippetName = extractSnippetName(line);
            if (snippetName in snippetMap) {
                snippetMap[snippetName]["end"] = parseInt(lineIndex); 
            }
        }
    }
    if (!(snippet in snippetMap) || !("end" in snippetMap[snippet])) {
        throw new Error(`Snippet ${snippet} not found!`);
    }

    lines = lines.slice((snippetMap[snippet]["start"]+1), snippetMap[snippet]["end"]);
    lines = filterDirectives(lines);
    return lines.join("\n");
}


export const DltSnippet = ({snippets, language, snippet}) => {
    let snippetString = extractSnippet(snippets, snippet);
    snippetString = dedent(snippetString);
    return (<CodeBlock children={snippetString} language={language}/>);
}; 