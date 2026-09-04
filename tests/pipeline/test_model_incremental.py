from __future__ import annotations

import pathlib
from typing import Any, Iterator, Literal

import pytest

import dlt
from dlt.extract.incremental import Incremental
from dlt.extract.incremental.transform import ModelIncremental

from tests.extract.utils import bind_state


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
    initial_value: Any = None,
    end_value: Any = None,
    last_value_func: Any = max,
    range_start: Literal["open", "closed"] = "open",
    range_end: Literal["open", "closed"] = "open",
    primary_key: Any = None,
    boundary_consumed: bool = False,
) -> ModelIncremental:
    # initial_value defaults to start_value (a fresh, not yet advanced state)
    if initial_value is None:
        initial_value = start_value
    parent: dlt.sources.incremental[Any] = dlt.sources.incremental(
        cursor_path,
        initial_value=initial_value,
        end_value=end_value,
        primary_key=primary_key,
        last_value_func=last_value_func,
        range_start=range_start,
        range_end=range_end,
    )
    # the dedup marker for the row at last_value, as an eager unique cursor advance writes it
    bind_state(
        parent,
        start_value,
        initial_value=initial_value,
        start_value=start_value,
        unique_hashes=[parent.cursor_value_hash(start_value)] if boundary_consumed else (),
    )
    transformer = ModelIncremental(
        resource_name="test",
        cursor_path=cursor_path,
        initial_value=initial_value,
        start_value=start_value,
        end_value=end_value,
        last_value_func=last_value_func,
        primary_key=primary_key,
        unique_hashes=set(),
        range_start=range_start,
        range_end=range_end,
    )
    transformer._incremental = parent
    return transformer


def _capture_stateful_relation(
    pipeline: dlt.Pipeline,
    *,
    resource_name: str,
    initial_value: int,
    range_start: Literal["open", "closed"] = "open",
    range_end: Literal["open", "closed"] = "open",
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
            "id", initial_value=initial_value, range_start=range_start, range_end=range_end
        ),
    ) -> Iterator[Any]:
        nonlocal captured
        # advance=False: the follow-up _model_transformer is what advances/consumes
        captured = dataset.table("events").incremental(cursor, advance=False)
        yield from []

    pipeline.extract(probe())
    assert captured is not None
    return captured


def test_dispatches_modelincremental_for_relation(incremental_pipeline: dlt.Pipeline) -> None:
    dataset = incremental_pipeline.dataset()
    incremental: dlt.sources.incremental[int] = bind_state(
        dlt.sources.incremental("id", initial_value=0, end_value=10**12), 0
    )
    relation = dataset.table("events")
    incremental_transform = incremental._get_transform(relation)
    assert isinstance(incremental_transform, ModelIncremental)
    assert incremental_transform.cursor_path == "id"


def test_advances_to_end_value_when_set(incremental_pipeline: dlt.Pipeline) -> None:
    dataset = incremental_pipeline.dataset()
    incremental = dlt.sources.incremental("id", initial_value=0, end_value=10**12)
    relation = dataset.table("events").incremental(incremental)

    transformer = _model_transformer(start_value=0, end_value=10**12, range_start="closed")
    out, _, _ = transformer(relation)

    # advance=True always advances state; with end_value set, advances to end_value
    assert transformer.last_value == 10**12
    rows = sorted(int(r[0]) for r in out.select("id").fetchall())
    assert rows == [1, 2, 3, 4, 5]


@pytest.mark.parametrize(
    "range_start,range_end,expected_ids",
    [
        # open start never replays the boundary: the synthesized upper is coerced to
        # closed so the boundary row is not lost — same rows as open-closed
        pytest.param("open", "open", [3, 4, 5], id="open-open-coerced-eager"),
        pytest.param("open", "closed", [3, 4, 5], id="open-closed"),
        pytest.param("closed", "open", [2, 3, 4], id="closed-open"),
        pytest.param("closed", "closed", [2, 3, 4, 5], id="closed-closed"),
    ],
)
def test_stateful_advances_state_across_range_modifiers(
    incremental_pipeline: dlt.Pipeline,
    range_start: Literal["open", "closed"],
    range_end: Literal["open", "closed"],
    expected_ids: list[int],
) -> None:
    relation = _capture_stateful_relation(
        incremental_pipeline,
        resource_name=f"probe_range_{range_start}_{range_end}",
        initial_value=2,
        range_start=range_start,
        range_end=range_end,
    )
    transformer = _model_transformer(start_value=2, range_start=range_start, range_end=range_end)
    out, _, _ = transformer(relation)

    assert transformer.last_value == 5
    rows = sorted(int(r[0]) for r in out.select("id").fetchall())
    assert rows == expected_ids


@pytest.mark.parametrize(
    "primary_key,start_value,initial_value,expected_ids",
    [
        # fresh state: user's closed start keeps initial_value, end overridden to <= MAX
        pytest.param("id", 0, 0, [1, 2, 3, 4, 5], id="eager-boundary-first-window"),
        # advanced state: lower goes strict so the already-loaded boundary never replays
        pytest.param("id", 3, 0, [4, 5], id="strict-lower-advanced-window"),
        pytest.param(("id",), 0, 0, [1, 2, 3, 4, 5], id="tuple-pk-eager"),
        # composite or missing pk does not imply uniqueness: ranges unchanged (>= 0, < 5)
        pytest.param(("id", "value"), 0, 0, [1, 2, 3, 4], id="composite-pk-unchanged"),
        pytest.param(None, 0, 0, [1, 2, 3, 4], id="no-pk-unchanged"),
    ],
)
def test_unique_cursor_takes_boundary_eagerly(
    incremental_pipeline: dlt.Pipeline,
    primary_key: Any,
    start_value: int,
    initial_value: int,
    expected_ids: list[int],
) -> None:
    """A primary key equal to the cursor declares cursor values unique: the boundary
    value is complete once seen, so it loads eagerly (closed end) and an advanced
    lower bound goes strict (open start) instead of replaying."""
    relation = incremental_pipeline.dataset().table("events")
    transformer = _model_transformer(
        start_value=start_value,
        initial_value=initial_value,
        range_start="closed",
        range_end="open",
        primary_key=primary_key,
        # an advanced unique cursor has the boundary row's dedup hash recorded
        boundary_consumed=start_value != initial_value,
    )
    out, _, _ = transformer(relation)

    assert transformer.last_value == 5
    rows = sorted(int(r[0]) for r in out.select("id").fetchall())
    assert rows == expected_ids


@pytest.mark.parametrize(
    "incremental_kwargs,batches,expected_per_run,expected_last_value_per_run",
    [
        # boundary row defers one cycle, late arrival at the watermark (id 4, ts 200)
        # loads exactly once, run without new data loads nothing, 6 stays deferred
        pytest.param(
            {},
            [[(1, 100), (2, 100), (3, 200)], [(4, 200), (5, 300)], [], [(6, 400)]],
            [[1, 2], [3, 4], [], [5]],
            [200, 300, 300, 400],
            id="default-ranges-deferred-tiling",
        ),
        # primary key equal to the cursor: boundary loads eagerly and never replays
        pytest.param(
            {"primary_key": "ts"},
            [[(1, 100), (2, 150), (3, 200)], [(4, 250), (5, 300)], [], [(6, 400)]],
            [[1, 2, 3], [4, 5], [], [6]],
            [200, 300, 300, 400],
            id="unique-cursor-eager",
        ),
    ],
)
def test_multi_run_windows_tile_e2e(
    tmp_path: pathlib.Path,
    incremental_kwargs: dict[str, Any],
    batches: list[list[tuple[int, int]]],
    expected_per_run: list[list[int]],
    expected_last_value_per_run: list[int],
) -> None:
    """Across pipeline runs the stateful windows tile the cursor axis: every row loads
    exactly once with append and state advances through real pipeline state."""
    pipeline = dlt.pipeline(
        pipeline_name="multi_run_e2e",
        pipelines_dir=str(tmp_path / "pipelines_dir"),
        destination=dlt.destinations.duckdb(str(tmp_path / "multi_run.db")),
        dev_mode=True,
    )

    @dlt.resource(name="events", primary_key="id", write_disposition="append")
    def raw_events(rows: list[tuple[int, int]]) -> Iterator[Any]:
        yield [{"id": i, "ts": ts} for i, ts in rows]

    captured: list[list[int]] = []

    @dlt.resource(name="downstream")
    def downstream(
        cursor: dlt.sources.incremental[int] = dlt.sources.incremental(
            "ts", initial_value=0, on_cursor_value_missing="exclude", **incremental_kwargs
        ),
    ) -> Iterator[Any]:
        out = cursor(pipeline.dataset().table("events"))
        captured.append(sorted(int(r[0]) for r in out.select("id").fetchall()))  # type: ignore
        yield from []

    for batch, expected, expected_last_value in zip(
        batches, expected_per_run, expected_last_value_per_run
    ):
        if batch:
            pipeline.run(raw_events(batch))
        pipeline.run(downstream())
        assert captured[-1] == expected
        # the window end is persisted, so the next run tiles from it
        cursor_state = pipeline.state["sources"][pipeline.default_schema_name]["resources"][
            "downstream"
        ]["incremental"]["ts"]
        assert cursor_state["last_value"] == expected_last_value


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


def test_user_advance_skips_incremental_filter(incremental_pipeline: dlt.Pipeline) -> None:
    # user pre-advances → parent __call__ short-circuits, ModelIncremental never fires,
    # and the relation passes through unchanged on every subsequent yield
    dataset = incremental_pipeline.dataset()
    incremental: dlt.sources.incremental[int] = bind_state(
        dlt.sources.incremental("id", initial_value=0), 0
    )

    relation = dataset.table("events")
    incremental.advance(2)

    first = incremental(relation)
    assert first is relation
    assert relation.is_incremental is False

    # opt-out persists across yields (framework does not reset for user-driven advance)
    second = incremental(relation)
    assert second is relation


def test_aggregate_with_inner_and_outer(incremental_pipeline: dlt.Pipeline) -> None:
    """Inner cursor (`value`) filters at SQL; outer cursor (`id`) aggregates over the
    inner-filtered set and ANDs the upper bound onto the relation."""
    dataset = incremental_pipeline.dataset()
    # inner: value >= 3.0 → ids 3, 4, 5
    inner = dlt.sources.incremental("value", initial_value=3.0, range_start="closed")
    relation = dataset.table("events").incremental(inner)

    # outer: id > 0, range_end open (default)
    transformer = _model_transformer(cursor_path="id", start_value=0, range_start="open")
    out, _, _ = transformer(relation)

    # MAX(id) over inner-filtered rows
    assert transformer.last_value == 5
    # combined WHERE: value >= 3.0 (inner) AND id > 0 AND id <= 5 (open-open coerced eager)
    rows = sorted(int(r[0]) for r in out.select("id").fetchall())
    assert rows == [3, 4, 5]
