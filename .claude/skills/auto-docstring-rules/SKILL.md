---
name: auto-docstring-rules
description: Enforces dlt docstring conventions. Use when writing or modifying docstrings, classes, or function signatures in Python files.
user-invocable: false
---

# Docstring rules

Google style docstrings throughout. Three levels of detail:

## Short form (internal methods, properties)
Description only — no Args/Returns. One-liner for simple accessors.

## Full form (public API)
Full Google style with Args, Returns, Raises. Describe WHAT the function does and its output, never HOW it works internally.

```python
def paginate(
    self,
    path: str = "",
    method: HTTPMethodBasic = "GET",
    params: Optional[Dict[str, Any]] = None,
) -> Iterator[PageData[Any]]:
    """Iterates over paginated API responses, yielding pages of data.

    Args:
        path (str): Endpoint path for the request, relative to `base_url`.
        method (HTTPMethodBasic): HTTP method for the request, defaults to 'get'.
        params (Optional[Dict[str, Any]]): Query parameters for the request.

    Yields:
        PageData[Any]: A page of data with request and response context.

    Raises:
        HTTPError: If the response status code is not a success code.
    """
```

- Public API args use `name (Type):` format; internal methods may omit types
- Args descriptions are short — one line unless truly complex
- Use `Yields:` for generators, `Returns:` for regular functions
- `Raises:` only for exceptions the caller should handle
- `Example:` section is optional, add only when usage is non-obvious

## Field docstrings
Inline triple-quoted string on the NEXT line after the field definition.

```python
@configspec
class ItemsNormalizerConfiguration(BaseConfiguration):
    add_dlt_id: bool = True
    """When true, items will have `_dlt_id` column added with a unique ID."""
    add_dlt_load_id: bool = False
    """When true, items will have `_dlt_load_id` column added with current load ID."""
```

- Only document fields where the name + type aren't self-explanatory
- Keep to one line when possible

## Anti-patterns — NEVER do these
- Do not explain HOW the code works, only WHAT it does and its output
- Do not state the obvious (`"""Returns the name"""` on `get_name`)
- Do not add docstrings to code you did not change
