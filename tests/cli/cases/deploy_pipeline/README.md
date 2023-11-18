# How this repo works
1. `dlt init <source> <destination>` clones this repo, it uses the version of the dlt as a git tag to clone
2. if the source is one of the variants ie. `chess` then `chess.py` is used as pipeline template, if not the `pipeline.py` will be used (after renaming to the <source>)
2. id `--generic` options is passed the `pipeline_generic.py` template is used
3. it modifies the script by importing the right destination and using it in the pipeline
4. it copies the .gitignore, the pipeline script created above, the README and requirements to the folder in which dlt was called
5. it will create the `secrets.toml` and `config.toml` for the source and destination in the script
6. it will add the right dlt extra to `requirements.txt`



# How to customize and deploy this pipeline?

TODO: write some starting instructions