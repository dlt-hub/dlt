The other way to prevent that (see also `multistep_pipelines_and_dependent_resources.md`)

```python
@dlt.source
def spotify():

    @dlt.resource
    def get_huge_doc(name):
        yield requests.get(...)

    # make songs and playlists to be dependent on get_huge_doc
    @dlt.resource(depends_on=huge_doc)
    def songs(huge_doc):
        yield huge_doc["songs"]

    @dlt.resource(depends_on=huge_doc)
    def playlists(huge_doc):
        yield huge_doc["playlists"]

    # as you can see the get_huge_doc is not even returned, nevertheless it will be evaluated (only once)
    # the huge doc will not be extracted and loaded
    return songs, playlists
```
