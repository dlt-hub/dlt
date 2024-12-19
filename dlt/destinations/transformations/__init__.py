from typing import Callable, Literal, Union, Any, Generator, List, TYPE_CHECKING, Iterable

from dataclasses import dataclass
from functools import wraps

from dlt.common.destination.reference import SupportsReadableDataset, SupportsReadableRelation


TTransformationMaterialization = Literal["table", "view"]
TTransformationWriteDisposition = Literal["replace", "append"]

TTransformationFunc = Callable[[SupportsReadableDataset], SupportsReadableRelation]

TTransformationGroupFunc = Callable[[], List[TTransformationFunc]]


def transformation(
    table_name: str,
    materialization: TTransformationMaterialization = "table",
    write_disposition: TTransformationWriteDisposition = "replace",
) -> Callable[[TTransformationFunc], TTransformationFunc]:
    def decorator(func: TTransformationFunc) -> TTransformationFunc:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> SupportsReadableRelation:
            return func(*args, **kwargs)

        # save the arguments to the function
        wrapper.__transformation_args__ = {  # type: ignore
            "table_name": table_name,
            "materialization": materialization,
            "write_disposition": write_disposition,
        }

        return wrapper

    return decorator


def transformation_group(
    name: str,
) -> Callable[[TTransformationGroupFunc], TTransformationGroupFunc]:
    def decorator(func: TTransformationGroupFunc) -> TTransformationGroupFunc:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> List[TTransformationFunc]:
            return func(*args, **kwargs)

        func.__transformation_group_args__ = {  # type: ignore
            "name": name,
        }
        return wrapper

    return decorator


def run_transformations(
    dataset: SupportsReadableDataset,
    transformations: Union[TTransformationFunc, List[TTransformationFunc]],
) -> None:
    if not isinstance(transformations, Iterable):
        transformations = [transformations]

    # TODO: fix typing
    with dataset.sql_client as client:  # type: ignore
        for transformation in transformations:
            # get transformation settings
            table_name = transformation.__transformation_args__["table_name"]  # type: ignore
            materialization = transformation.__transformation_args__["materialization"]  # type: ignore
            write_disposition = transformation.__transformation_args__["write_disposition"]  # type: ignore
            table_name = client.make_qualified_table_name(table_name)

            # get relation from transformation
            relation = transformation(dataset)
            if not isinstance(relation, SupportsReadableRelation):
                raise ValueError(
                    f"Transformation {transformation.__name__} did not return a ReadableRelation"
                )

            # materialize result
            select_clause = relation.query

            if write_disposition == "replace":
                client.execute(
                    f"CREATE OR REPLACE {materialization} {table_name} AS {select_clause}"
                )
            elif write_disposition == "append" and materialization == "table":
                try:
                    client.execute(f"INSERT INTO {table_name} {select_clause}")
                except Exception:
                    client.execute(f"CREATE TABLE {table_name} AS {select_clause}")
            else:
                raise ValueError(
                    f"Write disposition {write_disposition} is not supported for "
                    f"materialization {materialization}"
                )
