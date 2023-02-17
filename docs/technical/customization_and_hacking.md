# Customization

Customizations allow the user to change `dlt` behaviour without modifying the source code (which we call `hacking` 😄) Most of the customizations require writing python on yaml snipppets.

⛔ not implemented, hard to add

☮️ not implemented, easy to add

# in schema file

## global settings
- default column hints, types
- column propagation
- max nesting
- choose type autodetectors
- ⛔ add custom type autodetectors - more powerful than you think


## table/column settings
- table and column hints
- include and exclude filters
- ⛔ last value as decorator for common cases (ie. jsonpath + max operator + automatic filtering of the results)

# source and resource creation
when you implement new source/resource

## source
- providing custom schema via file
- providing custom schema in the code + decorator
- providing the nesting level via decorator

## resource
- providing table schema via hints (that includes the column definitions and column hints)
- resources may be parametrized (generators!)
- transformers also may be prametrized! (tutorial in progress)
- yielding metadata with the data items
- yielding custom data (ie. panda frames) (yes but last lambda must convert it to )

## extraction
- [retry with the decorator](/docs/examples/chess/chess.py)
- [run resources and transformers in parallel threads](/docs/examples/chess/chess.py) and test named `test_evolve_schema`
- run async resources and transformers

# source and resource modifications
- resource selection

## modification of sources and resources after they are created
must be done before passing to `run` method.

- adding custom resources and transformers to the pipeline after it is created
- easy change the table name for a resource (currently the whole template must be changed)
- ☮️ adding stateles lambdas (row transformations) to the resources: map, filter, flat_map (reverse pivot)
- ☮️ adding stateful lambdas (row transformations with the write access to pipeline state)
- change the source name


# pipeline callbacks and hooks
those are not implemented
https://github.com/dlt-hub/dlt/issues/63

