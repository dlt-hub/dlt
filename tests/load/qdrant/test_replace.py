from typing import Iterator, Optional

import pytest
from qdrant_client import QdrantClient

import dlt
from dlt.common.configuration.container import Container
from dlt.common.pipeline import PipelineContext
from dlt.destinations import qdrant


pytestmark = pytest.mark.essential


@pytest.fixture
def qdrant_db() -> Iterator[QdrantClient]:
    client = QdrantClient(":memory:")
    try:
        yield client
    finally:
        Container()[PipelineContext].deactivate()
        client.close()


@pytest.mark.parametrize("dataset_name", [None, "dataset"])
def test_replace_local_collections(qdrant_db: QdrantClient, dataset_name: Optional[str]) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="replace_local",
        destination=qdrant(credentials=qdrant_db),
        dataset_name=dataset_name,
    )
    prefix = f"{dataset_name}_" if dataset_name else ""

    for rows in [
        [{"id": 1, "tags": ["old", "obsolete"]}, {"id": 2, "tags": ["old"]}],
        [{"id": 3, "tags": ["new"]}],
    ]:
        info = pipeline.run(rows, table_name="items", write_disposition="replace")
        assert not info.has_failed_jobs
        points, _ = qdrant_db.scroll(prefix + "items")
        assert sorted(point.payload["id"] for point in points) == [row["id"] for row in rows]
        tags, _ = qdrant_db.scroll(prefix + "items__tags")
        assert sorted(point.payload["value"] for point in tags) == sorted(
            tag for row in rows for tag in row["tags"]
        )


@pytest.mark.parametrize("dataset_name", [None, "dataset"])
def test_truncate_missing_collection(qdrant_db: QdrantClient, dataset_name: Optional[str]) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="truncate_missing",
        destination=qdrant(credentials=qdrant_db),
        dataset_name=dataset_name,
    )
    with pipeline.destination_client() as client:
        client.initialize_storage()
        client.initialize_storage(truncate_tables=["missing"])
        assert not client._collection_exists("missing")
