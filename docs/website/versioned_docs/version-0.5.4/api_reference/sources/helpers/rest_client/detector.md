---
sidebar_label: detector
title: sources.helpers.rest_client.detector
---

## single\_entity\_path

```python
def single_entity_path(path: str) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/sources/helpers/rest_client/detector.py#L49)

Checks if path ends with path param indicating that single object is returned

## find\_all\_lists

```python
def find_all_lists(
    dict_: Dict[str, Any],
    path: Tuple[str, ...] = (),
    result: List[Tuple[Tuple[str, ...], List[Any]]] = None
) -> List[Tuple[Tuple[str, ...], List[Any]]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/sources/helpers/rest_client/detector.py#L62)

Recursively looks for lists in dict_ and returns tuples
in format (dictionary keys, list)

## find\_response\_page\_data

```python
def find_response_page_data(
    response: Union[Dict[str, Any], List[Any], Any]
) -> Tuple[Tuple[str, ...], Any]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/sources/helpers/rest_client/detector.py#L82)

Finds a path to response data, assuming that data is a list, returns a tuple(path, data)

## single\_page\_detector

```python
def single_page_detector(
        response: Response) -> Tuple[SinglePagePaginator, float]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/sources/helpers/rest_client/detector.py#L200)

This is our fallback paginator, also for results that are single entities

## PaginatorFactory Objects

```python
class PaginatorFactory()
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/sources/helpers/rest_client/detector.py#L205)

### \_\_init\_\_

```python
def __init__(detectors: List[Callable[[Response], Tuple[BasePaginator,
                                                        float]]] = None)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/sources/helpers/rest_client/detector.py#L206)

`detectors` are functions taking Response as input and returning paginator instance and
detection score. Score value:
1.0 - perfect detection
0.0 - fallback detection
in between - partial detection, several paginator parameters are defaults

