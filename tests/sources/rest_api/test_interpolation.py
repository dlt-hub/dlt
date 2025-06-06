import pytest

from dlt.sources.rest_api.config_setup import (
    expand_placeholders,
    _find_expressions,
    _expressions_to_resolved_params,
    ResolvedParam,
)


@pytest.mark.parametrize(
    "obj, placeholders, expected, preserve_value_type",
    [
        pytest.param("posts/{id}", {"id": 1}, "posts/1", True, id="string"),
        pytest.param(
            "posts/{resources.posts.id}",
            {"resources.posts.id": 1},
            "posts/1",
            True,
            id="string_with_dots",
        ),
        pytest.param(
            {"id": "{id}", "name": "{name}", "sort": "asc"},
            {"id": "1", "name": "John"},
            {"id": "1", "name": "John", "sort": "asc"},
            True,
            id="dict",
        ),
        pytest.param(
            {"id": "{resources.posts.id}", "name": "{resources.posts.name}", "sort": "asc"},
            {"resources.posts.id": "1", "resources.posts.name": "John"},
            {"id": "1", "name": "John", "sort": "asc"},
            True,
            id="dict_with_dots",
        ),
        pytest.param(
            {"dict": {"b": {"c": "{id}"}, "list": ["{id}", "{id2}"]}},
            {"id": 1, "id2": 2},
            {"dict": {"b": {"c": 1}, "list": [1, 2]}},
            True,
            id="nested_json_body",
        ),
        pytest.param(
            {
                "dict": {
                    "b": {"c": "{posts.id}"},
                    "list": ["{posts.id}", "{posts.id2}"],
                }
            },
            {"posts.id": 1, "posts.id2": 2},
            {"dict": {"b": {"c": 1}, "list": [1, 2]}},
            True,
            id="nested_json_body_with_dots",
        ),
        pytest.param(
            "posts/{id}/{{not_this}}",
            {"id": 1},
            "posts/1/{not_this}",
            True,
            id="string_with_escaped_braces",
        ),
        pytest.param(
            "posts/{{not_this}}/{id}/{{also_not}}",
            {"id": 1},
            "posts/{not_this}/1/{also_not}",
            True,
            id="string_with_mixed_braces",
        ),
        pytest.param(
            {
                "path": "posts/{id}/{{not_this}}",
                "nested": {"path": "{{not_this}}/{id}/{{also_not}}", "value": "{id}"},
            },
            {"id": 1},
            {
                "path": "posts/1/{not_this}",
                "nested": {"path": "{not_this}/1/{also_not}", "value": 1},
            },
            True,
            id="nested_dict_with_escaped_braces",
        ),
        pytest.param(
            {
                "list": ["{{not_this}}", "{id}", "{{also_not}}"],
                "dict": {"key": "{{not_this}}", "value": "{id}"},
            },
            {"id": 1},
            {
                "list": ["{not_this}", 1, "{also_not}"],
                "dict": {"key": "{not_this}", "value": 1},
            },
            True,
            id="mixed_structures_with_escaped_braces",
        ),
        pytest.param(
            "posts/{{{id}}}",
            {"id": 1},
            "posts/{1}",
            True,
            id="triple_braces",
        ),
        pytest.param(
            "posts/{{{{not_this}}}}/{id}",
            {"id": 1},
            "posts/{{not_this}}/1",
            True,
            id="double_escaped_braces",
        ),
        pytest.param(
            "posts/{{not_this {id}}}",
            {"id": 1},
            "posts/{not_this 1}",
            True,
            id="escaped_braces_with_placeholder",
        ),
        pytest.param(
            "posts/{{not_this {id}}}",
            {"id": 1},
            "posts/{not_this 1}",
            False,
            id="escaped_braces_with_placeholder_no_preserve",
        ),
        pytest.param(
            None,
            {},
            None,
            True,
            id="none",
        ),
        pytest.param(
            None,
            {},
            None,
            False,
            id="none_no_preserve",
        ),
        pytest.param(
            "posts/{id}",
            {"id": ""},
            "posts/",
            True,
            id="empty_string_placeholder",
        ),
        pytest.param(
            "posts/{id}",
            {"id": ""},
            "posts/",
            False,
            id="empty_string_placeholder_no_preserve",
        ),
        pytest.param(
            {"empty_list": [], "empty_dict": {}},
            {},
            {"empty_list": [], "empty_dict": {}},
            True,
            id="empty_containers",
        ),
        pytest.param(
            {"mixed": [{"id": "{id}"}, 123, ["{value}", True], {"nested": {"key": "{key}"}}]},
            {"id": "test", "value": 456, "key": "nested"},
            {"mixed": [{"id": "test"}, 123, [456, True], {"nested": {"key": "nested"}}]},
            True,
            id="mixed_nested_structures",
        ),
        pytest.param(
            "user/{user@domain}/settings/{setting-name}",
            {"user@domain": "john@example.com", "setting-name": "theme"},
            "user/john@example.com/settings/theme",
            True,
            id="special_chars_in_placeholders",
        ),
        pytest.param(
            "user/{user@domain}/settings/{setting-name}",
            {"user@domain": "john@example.com", "setting-name": "theme"},
            "user/john@example.com/settings/theme",
            False,
            id="special_chars_in_placeholders_no_preserve",
        ),
        pytest.param(
            "{name} ❤️ {id}",
            {"name": "测试", "id": "テスト"},
            "测试 ❤️ テスト",
            True,
            id="unicode_chars",
        ),
        pytest.param(
            "{id}",
            {"id": 42},
            42,
            True,
            id="single_placeholder_no_text",
        ),
        pytest.param(
            "{id}{id}",
            {"id": 42},
            "4242",
            True,
            id="two_placeholders_no_text",
        ),
        pytest.param(
            "{id}{id2}",
            {"id": 42, "id2": 100},
            "42100",
            True,
            id="two_different_placeholders",
        ),
        pytest.param(
            "{id}{id2}",
            {"id": 42, "id2": 100},
            "42100",
            False,
            id="two_different_placeholders_no_preserve",
        ),
        pytest.param(
            "{id} {id2}",
            {"id": 42, "id2": 100},
            "42 100",
            True,
            id="two_placeholders_with_text",
        ),
        pytest.param(
            "{id}",
            {"id": 42},
            "42",
            False,
            id="single_placeholder_no_text_no_preserve",
        ),
        pytest.param(
            "{resources.posts.id}",
            {"resources.posts.id": 42},
            42,
            True,
            id="single_placeholder_with_dots_no_text",
        ),
        pytest.param(
            "{resources.posts.id}",
            {"resources.posts.id": 42},
            "42",
            False,
            id="single_placeholder_with_dots_no_text_no_preserve",
        ),
        pytest.param(
            "{id:>5}",
            {"id": 42},
            "   42",
            True,
            id="single_placeholder_with_format_spec",
        ),
        pytest.param(
            "{id:>5}",
            {"id": 42},
            "   42",
            False,
            id="single_placeholder_with_format_spec_no_preserve",
        ),
        pytest.param(
            "{id!r}",
            {"id": "42"},
            "'42'",
            True,
            id="single_placeholder_with_conversion",
        ),
        pytest.param(
            "{id!r}",
            {"id": "42"},
            "'42'",
            False,
            id="single_placeholder_with_conversion_no_preserve",
        ),
        pytest.param(
            "prefix{id}",
            {"id": 42},
            "prefix42",
            True,
            id="single_placeholder_with_leading_text",
        ),
        pytest.param(
            "prefix{id}",
            {"id": 42},
            "prefix42",
            False,
            id="single_placeholder_with_leading_text_no_preserve",
        ),
        pytest.param(
            "{id}suffix",
            {"id": 42},
            "42suffix",
            True,
            id="single_placeholder_with_trailing_text",
        ),
        pytest.param(
            "{id}suffix",
            {"id": 42},
            "42suffix",
            False,
            id="single_placeholder_with_trailing_text_no_preserve",
        ),
        pytest.param(
            "{id}",
            {"id": None},
            None,
            True,
            id="single_placeholder_none_value",
        ),
        pytest.param(
            "{id}",
            {"id": None},
            "None",
            False,
            id="single_placeholder_none_value_no_preserve",
        ),
        pytest.param(
            "{id}",
            {"id": True},
            True,
            True,
            id="single_placeholder_boolean_value",
        ),
        pytest.param(
            "{id}",
            {"id": True},
            "True",
            False,
            id="single_placeholder_boolean_value_no_preserve",
        ),
        pytest.param(
            "{id}",
            {"id": 3.14},
            3.14,
            True,
            id="single_placeholder_float_value",
        ),
        pytest.param(
            "{id}",
            {"id": 3.14},
            "3.14",
            False,
            id="single_placeholder_float_value_no_preserve",
        ),
        pytest.param(
            "{id}",
            {"id": [1, 2, 3]},
            [1, 2, 3],
            True,
            id="single_placeholder_list_value",
        ),
        pytest.param(
            "{id}",
            {"id": [1, 2, 3]},
            "[1, 2, 3]",
            False,
            id="single_placeholder_list_value_no_preserve",
        ),
        pytest.param(
            "{id}",
            {"id": {"key": "value"}},
            {"key": "value"},
            True,
            id="single_placeholder_dict_value",
        ),
        pytest.param(
            "{id}",
            {"id": {"key": "value"}},
            "{'key': 'value'}",
            False,
            id="single_placeholder_dict_value_no_preserve",
        ),
        pytest.param(
            "{id}",
            {"id": ""},
            "",
            True,
            id="single_placeholder_empty_string",
        ),
        pytest.param(
            "{id}",
            {"id": ""},
            "",
            False,
            id="single_placeholder_empty_string_no_preserve",
        ),
        pytest.param(
            "{id}",
            {"id": 0},
            0,
            True,
            id="single_placeholder_zero",
        ),
        pytest.param(
            "{id}",
            {"id": 0},
            "0",
            False,
            id="single_placeholder_zero_no_preserve",
        ),
        pytest.param(
            "{id}",
            {"id": -1},
            -1,
            True,
            id="single_placeholder_negative",
        ),
        pytest.param(
            "{id}",
            {"id": -1},
            "-1",
            False,
            id="single_placeholder_negative_no_preserve",
        ),
        pytest.param(
            "{id}",
            {"id": 1e10},
            1e10,
            True,
            id="single_placeholder_scientific_notation",
        ),
        pytest.param(
            "{id}",
            {"id": 1e10},
            "10000000000.0",
            False,
            id="single_placeholder_scientific_notation_no_preserve",
        ),
        pytest.param(
            "{id}",
            {"id": b"bytes"},
            b"bytes",
            True,
            id="single_placeholder_bytes",
        ),
        pytest.param(
            "{id}",
            {"id": b"bytes"},
            "b'bytes'",
            False,
            id="single_placeholder_bytes_no_preserve",
        ),
    ],
)
def test_expand_placeholders(obj, placeholders, expected, preserve_value_type):
    assert (
        expand_placeholders(obj, placeholders, preserve_value_type=preserve_value_type) == expected
    )


@pytest.mark.parametrize(
    "obj, expected_error",
    [
        pytest.param("posts/{id}", "'id'", id="missing_placeholder"),
        pytest.param(
            "posts/{resources.posts.id}", "'resources'", id="missing_placeholder_with_dots"
        ),
    ],
)
def test_expand_placeholders_raises(obj, expected_error):
    with pytest.raises(KeyError) as exc_info:
        expand_placeholders(obj, {})

    assert str(exc_info.value) == expected_error


@pytest.mark.parametrize(
    "obj, prefixes, expected",
    [
        pytest.param("posts/{id}", None, {"id"}, id="string"),
        pytest.param(
            "posts/{resources.posts.id}", None, {"resources.posts.id"}, id="string_with_dots"
        ),
        pytest.param(
            {"id": "{id}", "name": "{name}", "sort": "asc"}, None, {"id", "name"}, id="dict"
        ),
        pytest.param(
            {"dict": {"b": {"c": "{id}"}, "list": ["{id}", "{id2}"]}},
            None,
            {"id", "id2"},
            id="nested_json_body",
        ),
        pytest.param("just a string", None, set(), id="string_without_placeholders"),
        pytest.param("", None, set(), id="empty_string"),
        pytest.param(
            "blog/{r.blog.id}/{not_this}", ["r."], {"r.blog.id"}, id="string_with_prefixes"
        ),
        pytest.param(
            "blog/{r.blog.id}/{{not_this}}", None, {"r.blog.id"}, id="string_with_escaped_braces"
        ),
        pytest.param("blog/{{not_this}}", None, set(), id="string_with_escaped_braces"),
        pytest.param(
            "{{not_this}} {{ and {{not_that}} }}", None, set(), id="string_nested_escaped_braces"
        ),
        pytest.param(
            "blog/{{r.not.this}}/{not_this}",
            ["r."],
            set(),
            id="string_with_escaped_braces_and_prefixes",
        ),
        pytest.param(
            {
                "dict": {
                    "a": {"b": "{r.blog.id}"},
                    "c": "{{r.not.this}}",
                    "d": "{{r.not.this {{r.nor.this}}}}",
                    "graphql": {"query": """
                            query Entity {{
                                entity(id: "123") {{
                                    id
                                    name
                                }}
                            }}
                        """},
                }
            },
            None,
            {"r.blog.id"},
            id="dict_with_escaped_braces",
        ),
    ],
)
def test_find_expressions(obj, prefixes, expected):
    assert _find_expressions(obj, prefixes) == expected


@pytest.mark.parametrize(
    "expressions, expected",
    [
        pytest.param(
            {"resources.posts.id"},
            [
                ResolvedParam(
                    "resources.posts.id", {"type": "resolve", "resource": "posts", "field": "id"}
                )
            ],
            id="simple_expression",
        ),
        pytest.param(
            {"resources.posts.blog.id"},
            [
                ResolvedParam(
                    "resources.posts.blog.id",
                    {"type": "resolve", "resource": "posts", "field": "blog.id"},
                ),
            ],
            id="expression_with_dots1",
        ),
        pytest.param(
            {"resources.a.b.c.d"},
            [
                ResolvedParam(
                    "resources.a.b.c.d", {"type": "resolve", "resource": "a", "field": "b.c.d"}
                )
            ],
            id="expression_with_dots2",
        ),
    ],
)
def test_expressions_to_resolved_params(expressions, expected):
    assert _expressions_to_resolved_params(expressions) == expected


def test_expressions_raise_error_for_invalid_format():
    with pytest.raises(ValueError) as exc_info:
        _expressions_to_resolved_params({"resources.name"})

    assert (
        str(exc_info.value)
        == "Invalid definition of `resources.name`. Expected format: 'resources.<resource>.<field>'"
    )
