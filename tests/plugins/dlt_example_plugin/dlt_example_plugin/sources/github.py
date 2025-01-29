import dlt


@dlt.source
def github():
    @dlt.resource(
        table_name="issues__2",
        primary_key="id",
    )
    def load_issues():
        # return data with path separators
        yield [
            {
                "id": 100,
                "issue__id": 10,
            }
        ]

    return load_issues
