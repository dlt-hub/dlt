var watch = require('node-watch');
const { exec } = require('child_process');

console.log("Starting to watch for snippet changes")
watch('../snippets', { recursive: true }, function(evt, name) {
  console.log('%s changed, updating snippets.', name);
  exec('yarn snipsync', (err, stdout, stderr) => {
    if (err) {
      return;
    }});
});