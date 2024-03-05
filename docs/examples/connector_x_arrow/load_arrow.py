import connectorx as cx

import dlt
from dlt.sources.credentials import ConnectionStringCredentials


def read_sql_x(
    conn_str: ConnectionStringCredentials = dlt.secrets.value,
    query: str = dlt.config.value,
):
    yield cx.read_sql(
        conn_str.to_native_representation(),
        query,
        return_type="arrow2",
        protocol="binary",
    )


def genome_resource():
    # create genome resource with merge on `upid` primary key
    genome = dlt.resource(
        name="genome",
        write_disposition="merge",
        primary_key="upid",
        standalone=True,
    )(read_sql_x)(
        "mysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam",  # type: ignore[arg-type]
        "SELECT * FROM genome ORDER BY created LIMIT 1000",
    )
    # add incremental on created at
    genome.apply_hints(incremental=dlt.sources.incremental("created"))
    return genome


if __name__ == "__main__":
    pipeline = dlt.pipeline(destination="duckdb")
    genome = genome_resource()

    print(pipeline.run(genome))
    print(pipeline.last_trace.last_normalize_info)
    # NOTE: run pipeline again to see that no more records got loaded thanks to incremental loading
