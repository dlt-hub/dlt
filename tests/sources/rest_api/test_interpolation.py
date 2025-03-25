import pytest

from dlt.sources.rest_api.config_setup import (
    expand_placeholders,
    _find_expressions,
    _expressions_to_resolved_params,
    ResolvedParam,
)


@pytest.mark.parametrize(
    "obj, placeholders, expected",
    [
        pytest.param("posts/{id}", {"id": 1}, "posts/1", id="string"),
        pytest.param(
            "posts/{resources.posts.id}",
            {"resources.posts.id": 1},
            "posts/1",
            id="string_with_dots",
        ),
        pytest.param(
            {"id": "{id}", "name": "{name}", "sort": "asc"},
            {"id": "1", "name": "John"},
            {"id": "1", "name": "John", "sort": "asc"},
            id="dict",
        ),
        pytest.param(
            {"id": "{resources.posts.id}", "name": "{resources.posts.name}", "sort": "asc"},
            {"resources.posts.id": "1", "resources.posts.name": "John"},
            {"id": "1", "name": "John", "sort": "asc"},
            id="dict_with_dots",
        ),
        pytest.param(
            {"dict": {"b": {"c": "{id}"}, "list": ["{id}", "{id2}"]}},
            {"id": 1, "id2": 2},
            {"dict": {"b": {"c": "1"}, "list": ["1", "2"]}},
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
            {"dict": {"b": {"c": "1"}, "list": ["1", "2"]}},
            id="nested_json_body_with_dots",
        ),
        pytest.param(
            "posts/{id}/{{not_this}}",
            {"id": 1},
            "posts/1/{not_this}",
            id="string_with_escaped_braces",
        ),
        pytest.param(
            "posts/{{not_this}}/{id}/{{also_not}}",
            {"id": 1},
            "posts/{not_this}/1/{also_not}",
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
                "nested": {"path": "{not_this}/1/{also_not}", "value": "1"},
            },
            id="nested_dict_with_escaped_braces",
        ),
        pytest.param(
            {
                "list": ["{{not_this}}", "{id}", "{{also_not}}"],
                "dict": {"key": "{{not_this}}", "value": "{id}"},
            },
            {"id": 1},
            {
                "list": ["{not_this}", "1", "{also_not}"],
                "dict": {"key": "{not_this}", "value": "1"},
            },
            id="mixed_structures_with_escaped_braces",
        ),
        pytest.param(
            "posts/{{{{not_this}}}}/{id}",
            {"id": 1},
            "posts/{{not_this}}/1",
            id="double_escaped_braces",
        ),
        pytest.param(
            "posts/{{not_this {id}}}",
            {"id": 1},
            "posts/{not_this 1}",
            id="escaped_braces_with_placeholder",
        ),
        pytest.param(
            None,
            {},
            None,
            id="none",
        ),
        pytest.param(
            "posts/{id}",
            {"id": ""},
            "posts/",
            id="empty_string_placeholder",
        ),
        pytest.param(
            {"empty_list": [], "empty_dict": {}},
            {},
            {"empty_list": [], "empty_dict": {}},
            id="empty_containers",
        ),
        pytest.param(
            {"mixed": [{"id": "{id}"}, 123, ["{value}", True], {"nested": {"key": "{key}"}}]},
            {"id": "test", "value": 456, "key": "nested"},
            {"mixed": [{"id": "test"}, 123, ["456", True], {"nested": {"key": "nested"}}]},
            id="mixed_nested_structures",
        ),
        pytest.param(
            "user/{user@domain}/settings/{setting-name}",
            {"user@domain": "john@example.com", "setting-name": "theme"},
            "user/john@example.com/settings/theme",
            id="special_chars_in_placeholders",
        ),
        pytest.param(
            "{name} ❤️ {id}",
            {"name": "测试", "id": "テスト"},
            "测试 ❤️ テスト",
            id="unicode_chars",
        ),
    ],
)
def test_expand_placeholders(obj, placeholders, expected):
    assert expand_placeholders(obj, placeholders) == expected


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
        == "Invalid definition of resources.name. Expected format: 'resources.<resource>.<field>'"
    )
