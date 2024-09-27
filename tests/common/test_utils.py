from copy import deepcopy
import itertools
import inspect
import binascii
import pytest
from typing import Any, Dict
from dlt.common.exceptions import PipelineException, TerminalValueError

from dlt.common.runners import Venv
from dlt.common.utils import (
    clone_dict_nested,
    graph_find_scc_nodes,
    flatten_list_of_str_or_dicts,
    digest128,
    graph_edges_to_nodes,
    group_dict_of_lists,
    map_nested_in_place,
    reveal_pseudo_secret,
    obfuscate_pseudo_secret,
    get_module_name,
    concat_strings_with_limit,
    increase_row_count,
    merge_row_counts,
    extend_list_deduplicated,
    get_exception_trace,
    get_exception_trace_chain,
    update_dict_nested,
)


def test_flatten_list_of_str_or_dicts() -> None:
    l_d = [{"a": "b"}, "c", 1, [2]]
    d_d = flatten_list_of_str_or_dicts(l_d)  # type: ignore[arg-type]
    assert d_d == {"a": "b", "c": None, "1": None, "[2]": None}
    # key clash
    l_d = [{"a": "b"}, "a"]
    with pytest.raises(KeyError):
        d_d = flatten_list_of_str_or_dicts(l_d)  # type: ignore[arg-type]


def test_digest128_length() -> None:
    assert len(digest128("hash it")) == 120 / 6


def test_map_dicts_in_place() -> None:
    _d = {"a": "1", "b": ["a", "b", ["a", "b"], {"a": "c"}], "c": {"d": "e", "e": ["a", 2]}}
    exp_d = {
        "a": "11",
        "b": ["aa", "bb", ["aa", "bb"], {"a": "cc"}],
        "c": {"d": "ee", "e": ["aa", 4]},
    }
    assert map_nested_in_place(lambda v: v * 2, _d) == exp_d
    # in place
    assert _d == exp_d

    _l = ["a", "b", ["a", "b"], {"a": "c"}]
    exp_l = ["aa", "bb", ["aa", "bb"], {"a": "cc"}]
    assert map_nested_in_place(lambda v: v * 2, _l) == exp_l
    assert _l == exp_l

    with pytest.raises(ValueError):
        map_nested_in_place(lambda v: v * 2, "a")


def test_pseudo_obfuscation() -> None:
    pseudo_key = b"test-key"
    value = "test test test"
    obfuscated = obfuscate_pseudo_secret(value, pseudo_key)
    assert value == reveal_pseudo_secret(obfuscated, pseudo_key)

    # make sure base64 decoding errors are raised
    with pytest.raises(binascii.Error):
        reveal_pseudo_secret("ABBYA", pseudo_key)


def test_get_module_name() -> None:
    m = inspect.getmodule(test_pseudo_obfuscation)
    assert get_module_name(m) == "test_utils"

    from tests.common.cases.modules.uniq_mod_121 import find_my_module

    m = inspect.getmodule(find_my_module)
    assert get_module_name(m) == "uniq_mod_121"

    # use exec to get __main__ exception
    mod_name = Venv.restore_current().run_script("tests/common/cases/modules/uniq_mod_121.py")
    assert mod_name.strip() == "uniq_mod_121"


def test_concat_strings_with_limit() -> None:
    assert list(concat_strings_with_limit([], " ", 15)) == []

    philosopher = ["Bertrand Russell"]
    assert list(concat_strings_with_limit(philosopher, ";\n", 15)) == ["Bertrand Russell"]

    # only two strings will be merged (22 chars total)
    philosophers = [
        "Bertrand Russell",
        "Ludwig Wittgenstein",
        "G.E. Moore",
        "J.L. Mackie",
        "Alfred Tarski",
    ]
    moore_merged = [
        "Bertrand Russell",
        "Ludwig Wittgenstein",
        "G.E. Moore J.L. Mackie",
        "Alfred Tarski",
    ]
    moore_merged_2 = [
        "Bertrand Russell",
        "Ludwig Wittgenstein",
        "G.E. Moore;\nJ.L. Mackie",
        "Alfred Tarski",
    ]
    assert list(concat_strings_with_limit(philosophers, " ", 22)) == moore_merged
    # none will be merged
    assert list(concat_strings_with_limit(philosophers, ";\n", 22)) == philosophers
    # again 1
    assert list(concat_strings_with_limit(philosophers, ";\n", 23)) == moore_merged_2
    # all merged
    assert list(concat_strings_with_limit(philosophers, ";\n", 1024)) == [";\n".join(philosophers)]
    # none will be merged, all below limit
    assert list(concat_strings_with_limit(philosophers, ";\n", 1)) == philosophers


def test_find_scc_nodes() -> None:
    edges = [("A", "B"), ("B", "C"), ("D", "E"), ("F", "G"), ("G", "H"), ("I", "I"), ("J", "J")]

    def _comp(s):
        return sorted([tuple(sorted(c)) for c in s])

    components = _comp([{"A", "B", "C"}, {"D", "E"}, {"F", "G", "H"}, {"I"}, {"J"}])
    scc1 = graph_find_scc_nodes(graph_edges_to_nodes(edges, directed=False))
    for perm_edges in itertools.permutations(edges):
        scc2 = graph_find_scc_nodes(graph_edges_to_nodes(perm_edges, directed=False))
        assert _comp(scc1) == _comp(scc2) == components
        scc1 = scc2

    # add edge that should not change the component
    edges.append(("A", "C"))
    scc2 = graph_find_scc_nodes(graph_edges_to_nodes(edges, directed=False))
    assert _comp(scc2) == components


def test_graph_edges_to_nodes() -> None:
    edges = [
        ("A", "B"),
        ("A", "C"),
        ("B", "C"),
        ("D", "E"),
        ("F", "G"),
        ("G", "H"),
        ("I", "I"),
        ("J", "J"),
    ]
    graph = {
        "A": {"B", "C"},
        "B": {"C"},
        "C": set(),
        "D": {"E"},
        "E": set(),
        "F": {"G"},
        "G": {"H"},
        "H": set(),
        "I": set(),
        "J": set(),
    }
    g1 = graph_edges_to_nodes(edges)

    for perm_edges in itertools.permutations(edges):
        g2 = graph_edges_to_nodes(perm_edges)
        # same graph no matter the order of edges
        assert g1 == g2 == graph
        g1 = g2

    # test a few edge cases
    assert graph_edges_to_nodes([]) == {}
    # ignores double edge
    assert graph_edges_to_nodes([("A", "B"), ("A", "B")]) == {"A": {"B"}, "B": set()}


def test_increase_row_counts() -> None:
    counts: Dict[str, int] = {}
    increase_row_count(counts, "table1", 1)
    increase_row_count(counts, "table2", 0)
    increase_row_count(counts, "table3", 10)

    assert counts == {"table1": 1, "table2": 0, "table3": 10}

    increase_row_count(counts, "table1", 2)
    increase_row_count(counts, "table2", 3)
    increase_row_count(counts, "table3", 4)

    assert counts == {"table1": 3, "table2": 3, "table3": 14}


def test_merge_row_counts() -> None:
    rc1 = {
        "table1": 3,
        "table2": 3,
    }

    merge_row_counts(
        rc1,
        {
            "table2": 5,
            "table3": 20,
        },
    )
    assert rc1 == {"table1": 3, "table2": 8, "table3": 20}
    merge_row_counts(rc1, {"table2": 5, "table3": 20, "table4": 2})
    assert rc1 == {"table1": 3, "table2": 13, "table3": 40, "table4": 2}


def test_extend_list_deduplicated() -> None:
    assert extend_list_deduplicated(["one", "two", "three"], ["four", "five", "six"]) == [
        "one",
        "two",
        "three",
        "four",
        "five",
        "six",
    ]
    assert extend_list_deduplicated(
        ["one", "two", "three", "six"], ["two", "four", "five", "six"]
    ) == ["one", "two", "three", "six", "four", "five"]
    assert extend_list_deduplicated(["one", "two", "three"], ["one", "two", "three"]) == [
        "one",
        "two",
        "three",
    ]
    assert extend_list_deduplicated([], ["one", "two", "three"]) == ["one", "two", "three"]


def test_exception_traces() -> None:
    from dlt.common.destination.exceptions import IdentifierTooLongException

    # bare exception without stack trace
    trace = get_exception_trace(Exception("Message"))
    assert trace["message"] == "Message"
    assert trace["exception_type"] == "Exception"
    assert "stack_trace" not in trace
    assert trace["is_terminal"] is False

    # dlt exception with traceback
    try:
        raise IdentifierTooLongException("postgres", "table", "too_long_table", 8)
    except Exception as exc:
        trace = get_exception_trace(exc)
    assert trace["exception_type"] == "dlt.common.destination.exceptions.IdentifierTooLongException"
    assert isinstance(trace["stack_trace"], list)
    assert trace["exception_attrs"] == {
        "destination_name": "postgres",
        "identifier_type": "table",
        "identifier_name": "too_long_table",
        "max_identifier_length": 8,
    }
    assert trace["is_terminal"] is True

    # dlt exception with additional props
    try:
        raise PipelineException("test_pipeline", "Message")
    except Exception as exc:
        trace = get_exception_trace(exc)
    assert trace["pipeline_name"] == "test_pipeline"


def test_exception_trace_chain() -> None:
    from dlt.common.destination.exceptions import IdentifierTooLongException

    try:
        raise TerminalValueError("Val")
    except Exception:
        try:
            raise IdentifierTooLongException("postgres", "table", "too_long_table", 8)
        except Exception as exc:
            try:
                # explicit cause
                raise PipelineException("test_pipeline", "Message") from exc
            except Exception as exc:
                traces = get_exception_trace_chain(exc)
    # outer exception first
    assert len(traces) == 3
    assert traces[0]["exception_type"] == "dlt.common.exceptions.PipelineException"
    assert (
        traces[1]["exception_type"]
        == "dlt.common.destination.exceptions.IdentifierTooLongException"
    )
    assert traces[2]["exception_type"] == "dlt.common.exceptions.TerminalValueError"


def test_nested_dict_merge() -> None:
    dict_1 = {"a": 1, "b": 2}
    dict_2 = {"a": 2, "c": 4}

    assert update_dict_nested(dict(dict_1), dict_2) == {"a": 2, "b": 2, "c": 4}
    assert update_dict_nested(dict(dict_2), dict_1) == {"a": 1, "b": 2, "c": 4}
    assert update_dict_nested(dict(dict_1), dict_2, copy_src_dicts=True) == {"a": 2, "b": 2, "c": 4}
    assert update_dict_nested(dict(dict_2), dict_1, copy_src_dicts=True) == {"a": 1, "b": 2, "c": 4}
    dict_1_update = update_dict_nested({}, dict_1)
    assert dict_1_update == dict_1
    assert dict_1_update is not dict_1
    dict_1_update = clone_dict_nested(dict_1)
    assert dict_1_update == dict_1
    assert dict_1_update is not dict_1

    dict_1_deep = {"a": 3, "b": dict_1}
    dict_1_deep_clone = update_dict_nested({}, dict_1_deep)
    assert dict_1_deep_clone == dict_1_deep
    # reference got copied
    assert dict_1_deep_clone["b"] is dict_1
    # update with copy
    dict_1_deep_clone = clone_dict_nested(dict_1_deep)
    assert dict_1_deep_clone == dict_1_deep
    # reference got copied
    assert dict_1_deep_clone["b"] is not dict_1

    # make sure that that Mappings that are not dicts are atomically copied
    from dlt.common.configuration.specs import ConnectionStringCredentials

    dsn = ConnectionStringCredentials("postgres://loader:loader@localhost:5432/dlt_data")
    dict_1_mappings: Dict[str, Any] = {
        "_tuple": (1, 2),
        "_config": {"key": "str", "_dsn": dsn, "_dict": dict_1_deep},
    }
    # make a clone
    dict_1_mappings_clone = clone_dict_nested(dict_1_mappings)
    # values are same
    assert dict_1_mappings == dict_1_mappings_clone
    # all objects and mappings are copied as reference
    assert dict_1_mappings["_tuple"] is dict_1_mappings_clone["_tuple"]
    assert dict_1_mappings["_config"]["_dsn"] is dict_1_mappings_clone["_config"]["_dsn"]
    # dicts are copied by value
    assert dict_1_mappings["_config"] is not dict_1_mappings_clone["_config"]
    assert dict_1_mappings["_config"]["_dict"] is not dict_1_mappings_clone["_config"]["_dict"]
    assert (
        dict_1_mappings["_config"]["_dict"]["b"]
        is not dict_1_mappings_clone["_config"]["_dict"]["b"]
    )

    # make a copy using references
    dict_1_mappings_clone = update_dict_nested({}, dict_1_mappings)
    assert dict_1_mappings["_config"] is dict_1_mappings_clone["_config"]
    assert dict_1_mappings["_config"]["_dict"] is dict_1_mappings_clone["_config"]["_dict"]
    assert (
        dict_1_mappings["_config"]["_dict"]["b"] is dict_1_mappings_clone["_config"]["_dict"]["b"]
    )

    # replace a few keys
    print(dict_1_mappings)
    # this should be non destructive for the dst
    deep_clone_dict_1_mappings = deepcopy(dict_1_mappings)
    mappings_update = update_dict_nested(
        dict_1_mappings, {"_config": {"_dsn": ConnectionStringCredentials(), "_dict": {"a": "X"}}}
    )
    # assert deep_clone_dict_1_mappings == dict_1_mappings
    # things overwritten
    assert dict_1_mappings["_config"]["_dsn"] is mappings_update["_config"]["_dsn"]
    # this one is empty
    assert mappings_update["_config"]["_dsn"].username is None
    assert dict_1_mappings["_config"]["_dsn"].username is None
    assert mappings_update["_config"]["_dict"]["a"] == "X"
    assert dict_1_mappings["_config"]["_dict"]["a"] == "X"

    # restore original values
    mappings_update = update_dict_nested(
        mappings_update, {"_config": {"_dsn": dsn, "_dict": {"a": 3}}}
    )
    assert mappings_update == deep_clone_dict_1_mappings


def test_group_dict_of_lists_one_element_each_list():
    input_dict = {"Frege": ["obj1"], "Gödel": ["obj2"], "Wittgenstein": ["obj3"]}
    result = group_dict_of_lists(input_dict)
    assert len(result) == 1
    assert result[0] == {"Frege": "obj1", "Gödel": "obj2", "Wittgenstein": "obj3"}


def test_group_dict_of_lists_equal_length_lists():
    input_dict = {
        "Frege": ["obj1", "obj2"],
        "Gödel": ["obj3", "obj4"],
        "Wittgenstein": ["obj5", "obj6"],
    }
    result = group_dict_of_lists(input_dict)
    assert len(result) == 2
    assert result[0] == {"Frege": "obj1", "Gödel": "obj3", "Wittgenstein": "obj5"}
    assert result[1] == {"Frege": "obj2", "Gödel": "obj4", "Wittgenstein": "obj6"}


def test_group_dict_of_lists_various_length_lists():
    input_dict = {
        "Frege": ["obj1", "obj2", "obj3"],
        "Gödel": ["obj4", "obj5"],
        "Wittgenstein": ["obj6"],
    }
    result = group_dict_of_lists(input_dict)
    assert len(result) == 3
    assert result[0] == {"Frege": "obj1", "Gödel": "obj4", "Wittgenstein": "obj6"}
    assert result[1] == {"Frege": "obj2", "Gödel": "obj5"}
    assert result[2] == {"Frege": "obj3"}

    # Check if the sizes of the decomposed dicts are decreasing
    sizes = [len(d) for d in result]
    assert sizes == sorted(sizes, reverse=True), "Sizes of decomposed dicts are not decreasing"
