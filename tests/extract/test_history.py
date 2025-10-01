import pytest

import dlt
from dlt.extract.exceptions import InvalidHistoryAccess
from dlt.extract.history import EMPTY_HISTORY


def test_history():
    @dlt.resource(keep_history=True)
    def objects():
        yield object()
        yield object()

    @dlt.transformer(data_from=objects)
    def propagate(user):
        yield user

    @dlt.transformer(data_from=propagate)
    def check(item, history: dlt.History = None):
        assert item is history[objects]
        yield 1

    assert list(check())


def test_history_str_key():
    @dlt.resource(keep_history=True)
    def objects():
        yield object()
        yield object()

    @dlt.transformer(data_from=objects)
    def propagate(user):
        yield user

    @dlt.transformer(data_from=propagate)
    def check(item, history: dlt.History = None):
        assert item is history["objects"]
        yield 1

    assert list(check())


def test_history_no_keep():
    @dlt.resource()
    def objects():
        yield object()
        yield object()

    @dlt.transformer(data_from=objects)
    def propagate(user):
        yield user

    @dlt.transformer(data_from=propagate)
    def check(item, history: dlt.History = None):
        with pytest.raises(InvalidHistoryAccess) as e:
            assert item is history[objects]
        assert "objects" in str(e.value)
        assert history is EMPTY_HISTORY
        yield 1

    assert list(check())


def test_history_key_error():
    @dlt.resource(keep_history=True)
    def objects():
        yield object()
        yield object()

    @dlt.transformer(data_from=objects)
    def propagate(user):
        yield user

    @dlt.transformer(data_from=propagate)
    def check(item, history: dlt.History = None):
        with pytest.raises(InvalidHistoryAccess) as e:
            assert item is history["non_existing_key"]
        assert "non_existing_key" in str(e.value)
        assert history is not EMPTY_HISTORY
        yield 1

    assert list(check())


def test_history_no_history_param():
    @dlt.resource(keep_history=True)
    def objects():
        yield object()
        yield object()

    @dlt.transformer(data_from=objects)
    def propagate(user):
        yield user

    @dlt.transformer(data_from=propagate)
    def check(item):
        yield 1

    assert list(check())


def test_history_no_none_specified():
    @dlt.resource(keep_history=True)
    def objects():
        yield object()
        yield object()

    @dlt.transformer(data_from=objects)
    def propagate(user):
        yield user

    @dlt.transformer(data_from=propagate)
    def check(item, history):
        assert item is history[objects]
        yield 1

    with pytest.raises(TypeError) as e:
        assert list(check())  # type: ignore[call-arg]
    assert "check(): missing a required argument: 'history'" in str(e.value)


def test_history_with_meta():
    @dlt.resource(keep_history=True)
    def objects():
        yield object()
        yield object()

    @dlt.transformer(data_from=objects)
    def propagate(user):
        yield user

    @dlt.transformer(data_from=propagate)
    def check(item, meta=None, history: dlt.History = None):
        assert item is history[objects]
        assert meta is None
        yield 1

    assert list(check())


def test_history_branching():
    @dlt.resource(keep_history=True)
    def objects():
        yield "test"

    @dlt.transformer(data_from=objects, keep_history=True)
    def more_objects(user):
        yield from ["object1", "object2"]

    @dlt.transformer(data_from=more_objects)
    def check(item, history: dlt.History = None):
        assert item == history[more_objects]
        yield 1

    assert list(check())
