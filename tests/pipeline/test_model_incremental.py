from __future__ import annotations

import pathlib
from typing import Any, Iterator, Literal

import pytest

import dlt
from dlt.extract.incremental.transform import ModelIncremental


EVENTS_LOAD_0 = [
    {"id": 1, "value": 1.0},
    {"id": 2, "value": 2.0},
    {"id": 3, "value": 3.0},
]
EVENTS_LOAD_1 = [
    {"id": 4, "value": 4.0},
    {"id": 5, "value": 5.0},
]


@pytest.fixture(scope="module")
def module_tmp_path(tmp_path_factory: pytest.TempPathFactory) -> pathlib.Path:
    return tmp_path_factory.mktemp("pytest-test_model_incremental")


@pytest.fixture(scope="module")
def incremental_pipeline(module_tmp_path: pathlib.Path) -> dlt.Pipeline:
    pipeline = dlt.pipeline(
        pipeline_name="model_incremental",
        pipelines_dir=str(module_tmp_path / "pipelines_dir"),
        destination=dlt.destinations.duckdb(str(module_tmp_path / "incremental.db")),
        dev_mode=True,
    )

    @dlt.resource(name="events", primary_key="id", write_disposition="append")
    def events(batch: int) -> Iterator[Any]:
        if batch == 0:
            yield EVENTS_LOAD_0
        else:
            yield EVENTS_LOAD_1

    pipeline.run(events(batch=0))
    pipeline.run(events(batch=1))
    return pipeline


def _model_transformer(
    *,
    cursor_path: str = "id",
    start_value: Any = 0,
    end_value: Any = None,
    last_value_func: Any = max,
    range_start: Literal["open", "closed"] = "open",
    range_end: Literal["open", "closed"] = "open",
) -> ModelIncremental:
    return ModelIncremental(
        resource_name="test",
        cursor_path=cursor_path,
        initial_value=start_value,
        start_value=start_value,
        end_value=end_value,
        last_value_func=last_value_func,
        primary_key=None,
        unique_hashes=set(),
        range_start=range_start,
        range_end=range_end,
    )


def _capture_stateful_relation(
    pipeline: dlt.Pipeline,
    *,
    resource_name: str,
    initial_value: int,
    range_start: Literal["open", "closed"] = "open",
) -> dlt.Relation:
    """Build an `.incremental()`-applied Relation against a bound stateful cursor.

    Stateful incrementals need an active pipeline to resolve `get_state()`, so
    we wrap the build in a no-op resource and `extract()` it just to bind.
    """
    dataset = pipeline.dataset()
    captured: dlt.Relation | None = None

    @dlt.resource(name=resource_name)
    def probe(
        cursor: dlt.sources.incremental[int] = dlt.sources.incremental(
            "id", initial_value=initial_value, range_start=range_start
        ),
    ) -> Iterator[Any]:
        nonlocal captured
        captured = dataset.table("events").incremental(cursor)
        yield from []

    pipeline.extract(probe())
    assert captured is not None
    return captured


def test_dispatches_modelincremental_for_relation(incremental_pipeline: dlt.Pipeline) -> None:
    dataset = incremental_pipeline.dataset()
    incremental: dlt.sources.incremental[int] = dlt.sources.incremental(
        "id", initial_value=0, end_value=10**12
    )
    incremental._cached_state = {
        "unique_hashes": [],
        "initial_value": 0,
        "last_value": 0,
        "start_value": 0,
    }
    relation = dataset.table("events")
    incremental_transform = incremental._get_transform(relation)
    assert isinstance(incremental_transform, ModelIncremental)
    assert incremental_transform.cursor_path == "id"


def test_advances_last_value_for_open_range(incremental_pipeline: dlt.Pipeline) -> None:
    relation = _capture_stateful_relation(
        incremental_pipeline, resource_name="probe_advance", initial_value=2
    )
    transformer = _model_transformer(start_value=2)
    out, start_out_of_range, end_out_of_range = transformer(relation)

    assert out is relation
    assert (start_out_of_range, end_out_of_range) == (False, False)
    assert transformer.last_value == 5


def test_no_advance_when_end_value_is_set(incremental_pipeline: dlt.Pipeline) -> None:
    dataset = incremental_pipeline.dataset()
    incremental = dlt.sources.incremental("id", initial_value=0, end_value=10**12)
    relation = dataset.table("events").incremental(incremental)

    transformer = _model_transformer(start_value=0, end_value=10**12, range_start="closed")
    transformer(relation)

    assert transformer.last_value == 0


@pytest.mark.parametrize(
    "range_start",
    [
        pytest.param("open", id="open-range-start"),
        pytest.param("closed", id="closed-range-start"),
    ],
)
def test_stateful_advances_state_across_range_modifiers(
    incremental_pipeline: dlt.Pipeline, range_start: Literal["open", "closed"]
) -> None:
    relation = _capture_stateful_relation(
        incremental_pipeline,
        resource_name=f"probe_range_{range_start}",
        initial_value=2,
        range_start=range_start,
    )
    transformer = _model_transformer(start_value=2, range_start=range_start)
    transformer(relation)

    assert transformer.last_value == 5


def test_auto_applies_on_bare_relation(incremental_pipeline: dlt.Pipeline) -> None:
    dataset = incremental_pipeline.dataset()
    yielded: dlt.Relation | None = None

    @dlt.resource(name="probe_auto_apply")
    def probe(
        cursor: dlt.sources.incremental[int] = dlt.sources.incremental(
            "id", initial_value=2, range_start="open"
        ),
    ) -> Iterator[Any]:
        nonlocal yielded
        yielded = dataset.table("events")
        yield yielded

    resource = probe()
    incremental_pipeline.extract(resource)

    assert yielded is not None
    assert yielded.is_incremental is False
    assert resource.state["incremental"]["id"]["last_value"] == 5


def test_does_not_clobber_last_value_on_empty_filter(incremental_pipeline: dlt.Pipeline) -> None:
    # initial_value above all data (max is 5) so the WHERE excludes everything
    relation = _capture_stateful_relation(
        incremental_pipeline, resource_name="probe_empty_filter", initial_value=10**9
    )

    transformer = _model_transformer(start_value=10**9)
    transformer(relation)

    assert transformer.last_value == 10**9


def test_multi_package_advances_state_e2e(tmp_path: pathlib.Path) -> None:
    """End-to-end: across multiple pipeline runs of a stateful `.incremental()`
    resource, state advances and each run only sees new data.
    """
    pipeline = dlt.pipeline(
        pipeline_name="multi_package_e2e",
        pipelines_dir=str(tmp_path / "pipelines_dir"),
        destination=dlt.destinations.duckdb(str(tmp_path / "multi_package.db")),
        dev_mode=True,
    )

    @dlt.resource(name="events", primary_key="id", write_disposition="append")
    def raw_events(batch: int) -> Iterator[Any]:
        if batch == 0:
            yield EVENTS_LOAD_0
        elif batch == 1:
            yield EVENTS_LOAD_1
        else:
            yield [{"id": 6, "value": 6.0}, {"id": 7, "value": 7.0}]

    pipeline.run(raw_events(batch=0))
    pipeline.run(raw_events(batch=1))

    captured_per_run: list[list[int]] = []

    @dlt.resource(name="downstream")
    def downstream(
        cursor: dlt.sources.incremental[int] = dlt.sources.incremental(
            "id", initial_value=0, range_start="open"
        ),
    ) -> Iterator[Any]:
        rel = pipeline.dataset().table("events").incremental(cursor)
        rows = rel.select("id", "value").fetchall()
        captured_per_run.append(sorted(int(r[0]) for r in rows))
        for row in rows:
            yield {"id": int(row[0]), "value": float(row[1])}

    # first downstream run: pulls everything strictly greater than 0 -> ids 1..5
    pipeline.run(downstream())
    assert captured_per_run[-1] == [1, 2, 3, 4, 5]
    state = pipeline.state["sources"]
    src_state = next(iter(state.values()))
    assert src_state["resources"]["downstream"]["incremental"]["id"]["last_value"] == 5

    # add new rows, run downstream again: only new rows are processed
    pipeline.run(raw_events(batch=2))
    pipeline.run(downstream())
    assert captured_per_run[-1] == [6, 7]
    src_state = next(iter(pipeline.state["sources"].values()))
    assert src_state["resources"]["downstream"]["incremental"]["id"]["last_value"] == 7
