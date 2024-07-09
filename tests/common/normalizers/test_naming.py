import pytest
import string
from typing import List, Type

from dlt.common.normalizers.naming import (
    NamingConvention,
    snake_case,
    direct,
    duck_case,
    sql_ci_v1,
    sql_cs_v1,
)
from dlt.common.typing import DictStrStr
from dlt.common.utils import uniq_id


ALL_NAMING_CONVENTIONS = {
    snake_case.NamingConvention,
    direct.NamingConvention,
    duck_case.NamingConvention,
    sql_ci_v1.NamingConvention,
    sql_cs_v1.NamingConvention,
}

ALL_UNDERSCORE_PATH_CONVENTIONS = ALL_NAMING_CONVENTIONS - {direct.NamingConvention}


LONG_PATH = "prospects_external_data__data365_member__member__feed_activities_created_post__items__comments__items__comments__items__author_details__educations"
DENSE_PATH = "__".join(string.ascii_lowercase)
LONG_IDENT = 10 * string.printable
IDENT_20_CHARS = "she played cello well"
RAW_IDENT = ".\n'played CELLO🚧_"
RAW_IDENT_W_SPACES = f"  {RAW_IDENT} \t\n"
RAW_IDENT_2 = '123."\rhello😄!'
RAW_IDENT_2_W_SPACES = f"\n  {RAW_IDENT_2} \t "
RAW_PATH = [RAW_IDENT, RAW_IDENT_2_W_SPACES, RAW_IDENT_2, RAW_IDENT_2_W_SPACES]
EMPTY_IDENT = "   \t\n "
RAW_PATH_WITH_EMPTY_IDENT = [
    RAW_IDENT,
    RAW_IDENT_2_W_SPACES,
    EMPTY_IDENT,
    RAW_IDENT_2,
    RAW_IDENT_2_W_SPACES,
]


def test_tag_collisions() -> None:
    tags: DictStrStr = {}
    # we should have ~1 collision for 1/prob tag generations
    # print(NamingConvention._compute_tag(uniq_id(32), collision_prob=0.001))
    generations = 100000
    collisions = 0
    for _ in range(0, generations):
        tag = NamingConvention._compute_tag(
            uniq_id(32), collision_prob=NamingConvention._DEFAULT_COLLISION_PROB
        )
        if tag in tags:
            collisions += 1
        else:
            tags[tag] = tag
    assert collisions / generations < 0.001


def test_tag_generation() -> None:
    # is content hash
    content = 20 * LONG_PATH
    content_tag = NamingConvention._compute_tag(
        content, collision_prob=NamingConvention._DEFAULT_COLLISION_PROB
    )
    # no randomness
    for _ in range(0, 20):
        tag = NamingConvention._compute_tag(
            content, collision_prob=NamingConvention._DEFAULT_COLLISION_PROB
        )
        assert content_tag == tag

    fixture = [
        ("3f17271231504b8cf65690bcdc379df8a3b8aabe12efe1ea82848ec5f497cb69", "gds0iw"),
        ("58e5c351b53ffe1233e0656a532a721ae1d2ac7af71b6cfec8ceb64c63b10721", "uyboiq"),
        ("e3f34629839cedcabba95354e48a78dc80b0cd35c02ddfbbf20196ba7f968866", "51wdcg"),
        ("f0f22b8e8c58389a6c21dbcc1e261ee0354704e24996a0ec541276f58d1f2f52", "bpm7ca"),
        ("0d0de95c7c12ceee919d28d22c970285d80a36dea4fe32dbdd667a888ae6d47f", "doqcuq"),
        ("4973509ea648ddfbaf6c50e1fef33c3b0a3d1c1a82dff543a8255e60b6572567", "cl7rpq"),
        ("877c89f0dcbd24b8c3f787624ddca09deb6a44e4a72f12527209d78e4d9ed247", "xrnycg"),
        ("064df58cd3a51c50dbf30e975e63961a501212ff8e8ca544ab396727f4b8a367", "kgiizq"),
        ("c8f7da1b5c44c1ca10da67c1514c4cf365e4d5912685b25a39206d5c8c1966a1", "dj9zqq"),
        ("222d42333592ea87823fd2e7868d59fb0aded20603f433319691918299513cb6", "futp4w"),
        ("757d64eb242a91b494ec9e2661a7946410d68144d33860d6f4154092d65d5009", "wetlpg"),
        ("3c7348d43478292b4c4e0689d41a536fc8ccabdbd9fb9d0dfbe757a83d34cebe", "avxagg"),
        ("6896fac1546c201d4dc91d2c51bdcd9c820fe92fd0555947e59fdc89ca6f045d", "wbaj3w"),
        ("b4def322a4487dd90fcc4abd2f1efde0cdce81d8e0a580fd1897203ab4ebcebe", "whojmw"),
        ("07d974124b92adafc90473a3968ceb5e8329d815e0e48260473d70a781adb8ae", "aiqcea"),
        ("c67183a762e379290652cc26a786b21eff347643b1cc9012138f460901ce5d53", "zfztpg"),
        ("430976db5adef67d0009aa3cd9a2daca106829b36a7232732c5d694e7197c6d1", "evr7rq"),
        ("c1c8c0ff6933fa4e23fab5605139124b2c6cda0150a412daaea274818ee46e35", "er0nxq"),
        ("0060c538b6ce02b8d8e2c85b4e2810c58b846f4096ed7ab871fc092c45ac09d9", "zh9xgg"),
        ("4d4b99ff5d2a3d5cd076782c9cd088cd85d5c789d7de6bdc19c1d206b687d485", "2vvr5a"),
    ]

    for content, expected_tag in fixture:
        tag = NamingConvention._compute_tag(
            content, collision_prob=NamingConvention._DEFAULT_COLLISION_PROB
        )
        assert len(tag) == 6
        assert tag == expected_tag
        # print(f"('{content}', '{tag}'),")


def test_tag_placement() -> None:
    # tags are placed in the middle of string and that must happen deterministically
    tag = "123456"
    # only tag fits here (6 chars)
    assert NamingConvention._trim_and_tag("DLTHUB", tag, 6) == tag
    assert NamingConvention._trim_and_tag("prefDLTHUBsuf", tag, 6) == tag
    # overflow to the right
    assert NamingConvention._trim_and_tag("DLT_HUB", tag, 7) == "D" + tag
    assert NamingConvention._trim_and_tag("prefDLT_HUBsuf", tag, 7) == "p" + tag
    # middle placement
    assert NamingConvention._trim_and_tag("DLT_HUB_", tag, 8) == "D" + tag + "_"
    assert NamingConvention._trim_and_tag("prefDLT_HUB_suf", tag, 8) == "p" + tag + "f"

    with pytest.raises(AssertionError):
        # tag too long
        NamingConvention._trim_and_tag("xDLTHUBx", tag, 5)

    with pytest.raises(AssertionError):
        # string does not require tagging
        NamingConvention._trim_and_tag("DLT", tag, 100)


def test_shorten_identifier() -> None:
    # no limit
    long_ident = 8 * LONG_PATH
    assert NamingConvention.shorten_identifier(long_ident, long_ident, None) == long_ident
    # within limit
    assert NamingConvention.shorten_identifier("012345678", "xxx012345678xxx", 10) == "012345678"
    assert (
        NamingConvention.shorten_identifier("0123456789", "xxx012345678xx?", 10) == "0123456789"
    )  # max_length
    # tag based on original string placed in the middle
    tag = NamingConvention._compute_tag(
        IDENT_20_CHARS, collision_prob=NamingConvention._DEFAULT_COLLISION_PROB
    )
    norm_ident = NamingConvention.shorten_identifier(IDENT_20_CHARS, IDENT_20_CHARS, 20)
    assert tag in norm_ident
    assert len(norm_ident) == 20
    assert norm_ident == "she plauanpualo well"
    # the tag must be based on raw string, not normalized string, one test case with spaces
    for raw_content in [uniq_id(), f" {uniq_id()} "]:
        tag = NamingConvention._compute_tag(
            raw_content, collision_prob=NamingConvention._DEFAULT_COLLISION_PROB
        )
        norm_ident = NamingConvention.shorten_identifier(IDENT_20_CHARS, raw_content, 20)
        assert tag in norm_ident
        assert len(norm_ident) == 20


@pytest.mark.parametrize("convention", ALL_NAMING_CONVENTIONS)
def test_normalize_with_shorten_identifier(convention: Type[NamingConvention]) -> None:
    naming = convention()
    # None/empty ident raises
    with pytest.raises(ValueError):
        naming.normalize_identifier(None)
    with pytest.raises(ValueError):
        naming.normalize_identifier(EMPTY_IDENT)

    # normalized string is different
    assert naming.normalize_identifier(RAW_IDENT) != RAW_IDENT
    # strip spaces
    assert naming.normalize_identifier(RAW_IDENT) == naming.normalize_identifier(RAW_IDENT_W_SPACES)

    # force to shorten
    naming = convention(len(RAW_IDENT) // 2)
    # tag expected on stripped RAW_IDENT
    tag = NamingConvention._compute_tag(
        RAW_IDENT, collision_prob=NamingConvention._DEFAULT_COLLISION_PROB
    )
    # spaces are stripped
    assert naming.normalize_identifier(RAW_IDENT) == naming.normalize_identifier(RAW_IDENT_W_SPACES)
    assert tag in naming.normalize_identifier(RAW_IDENT)


@pytest.mark.parametrize("convention", ALL_NAMING_CONVENTIONS)
def test_normalize_path_shorting(convention: Type[NamingConvention]) -> None:
    naming = convention()
    path = naming.make_path(*LONG_PATH.split("__"))

    # path left intact
    assert naming.normalize_path(path) == path

    # path shortened - a few typical cases
    naming = convention(64)
    norm_path = naming.normalize_path(path)
    assert_short_path(norm_path, naming)

    naming = convention(127)
    norm_path = naming.normalize_path(path)
    assert_short_path(norm_path, naming)

    # max length around the length of the path
    for max_length in range(len(path) // 2, int(len(path) * 1.1)):
        naming = convention(max_length)
        norm_path = naming.normalize_path(path)
        if max_length >= len(path):
            assert norm_path == path
        else:
            assert_short_path(norm_path, naming)

    # break dense path with tons of separators
    path = naming.make_path(*DENSE_PATH.split("__"))
    for max_length in range(len(path) // 2, int(len(path) * 1.1)):
        naming = convention(max_length)
        norm_path = naming.normalize_path(path)
        if max_length >= len(path):
            assert norm_path == path
        else:
            assert_short_path(norm_path, naming)

    # test path without separators
    naming = convention(16)
    norm_path = naming.normalize_path(LONG_IDENT)
    assert len(norm_path) == 16
    assert len(naming.break_path(norm_path)) == 1


@pytest.mark.parametrize("convention", ALL_NAMING_CONVENTIONS)
def test_normalize_path(convention: Type[NamingConvention]) -> None:
    naming = convention()
    raw_path_str = naming.make_path(*RAW_PATH)
    assert convention.PATH_SEPARATOR in raw_path_str
    # count separators
    norm_path_str = naming.normalize_path(raw_path_str)
    assert len(naming.break_path(norm_path_str)) == len(RAW_PATH)
    # double norm path does not change anything
    assert (
        naming.normalize_path(raw_path_str)
        == naming.normalize_path(norm_path_str)
        == naming.normalize_path(naming.normalize_path(norm_path_str))
    )
    # empty element in path is ignored
    assert naming.make_path(*RAW_PATH_WITH_EMPTY_IDENT) == raw_path_str
    assert naming.normalize_path(raw_path_str) == norm_path_str

    # preserve idents but shorten path
    naming = convention(len(RAW_IDENT) * 2)  # give enough max length
    # tag computed from raw path
    tag = NamingConvention._compute_tag(
        raw_path_str, collision_prob=NamingConvention._DEFAULT_COLLISION_PROB
    )
    tagged_raw_path_str = naming.normalize_path(raw_path_str)
    # contains tag
    assert tag in tagged_raw_path_str
    # idempotent
    assert (
        tagged_raw_path_str
        == naming.normalize_path(tagged_raw_path_str)
        == naming.normalize_path(naming.normalize_path(tagged_raw_path_str))
    )
    assert tagged_raw_path_str == naming.make_path(*naming.break_path(tagged_raw_path_str))

    # also cut idents
    naming = convention(len(RAW_IDENT) - 4)
    tagged_raw_path_str = naming.normalize_path(raw_path_str)
    # we expect the same tag because raw path is the same
    assert tag in tagged_raw_path_str


@pytest.mark.parametrize("convention", ALL_NAMING_CONVENTIONS)
def test_shorten_fragments(convention: Type[NamingConvention]) -> None:
    # max length around the length of the path
    naming = convention()
    # we assume raw path is normalized
    path = naming.make_path(*RAW_PATH)

    for max_length in range(len(path) // 2, int(len(path) * 1.1)):
        naming = convention(max_length)
        norm_path = naming.shorten_fragments(*RAW_PATH)
        if max_length >= len(path):
            assert norm_path == path
        else:
            assert len(norm_path) == naming.max_length
        # skips empty spaces
        assert naming.shorten_fragments(*RAW_PATH_WITH_EMPTY_IDENT) == norm_path


@pytest.mark.parametrize("convention", ALL_UNDERSCORE_PATH_CONVENTIONS)
def test_normalize_break_path(convention: Type[NamingConvention]) -> None:
    naming_unlimited = convention()
    assert naming_unlimited.break_path("A__B__C") == ["A", "B", "C"]
    # what if path has _a and _b which valid normalized idents
    assert naming_unlimited.break_path("_a___b__C___D") == ["_a", "_b", "C", "_D"]
    # skip empty identifiers from path
    assert naming_unlimited.break_path("_a_____b") == ["_a", "_b"]
    assert naming_unlimited.break_path("_a____b") == ["_a", "b"]
    assert naming_unlimited.break_path("_a__  \t\r__b") == ["_a", "b"]


@pytest.mark.parametrize("convention", ALL_UNDERSCORE_PATH_CONVENTIONS)
def test_normalize_make_path(convention: Type[NamingConvention]) -> None:
    naming_unlimited = convention()
    assert naming_unlimited.make_path("A", "B") == "A__B"
    assert naming_unlimited.make_path("_A", "_B") == "_A___B"
    assert naming_unlimited.make_path("_A", "", "_B") == "_A___B"
    assert naming_unlimited.make_path("_A", "\t\n ", "_B") == "_A___B"


def test_naming_convention_name() -> None:
    assert snake_case.NamingConvention.name() == "snake_case"
    assert direct.NamingConvention.name() == "direct"


def assert_short_path(norm_path: str, naming: NamingConvention) -> None:
    assert len(norm_path) == naming.max_length
    assert naming.normalize_path(norm_path) == norm_path
    assert all(len(ident) <= naming.max_length for ident in naming.break_path(norm_path))
