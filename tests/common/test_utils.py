import itertools
import inspect
import binascii
import pytest
from typing import Dict

from dlt.common.runners import Venv
from dlt.common.utils import (
    graph_find_scc_nodes,
    flatten_list_of_str_or_dicts,
    digest128,
    graph_edges_to_nodes,
    map_nested_in_place,
    reveal_pseudo_secret,
    obfuscate_pseudo_secret,
    get_module_name,
    concat_strings_with_limit,
    increase_row_count,
    merge_row_counts,
    extend_list_deduplicated,
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
