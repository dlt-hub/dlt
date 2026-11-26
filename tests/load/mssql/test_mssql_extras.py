import hashlib
from typing import Dict, List, Any, Literal
import random

import dlt
from dlt import pipeline
from dlt.common import pendulum
from dlt.common.utils import uniq_id

from dlt.destinations import mssql

from tests.pipeline.utils import (
    load_table_counts,
)


def generate_nested_json_structure(depth: int = 0, max_depth: int = 2) -> Dict[str, Any]:
    if depth >= max_depth:
        return {"leaf_value": random.randint(1, 1000), "leaf_date": pendulum.now()}

    return {
        f"level_{depth}_field": {
            "nested_doc": generate_nested_json_structure(depth + 1, max_depth),
            "nested_array": [
                generate_nested_json_structure(depth + 1, max_depth)
                for _ in range(random.randint(1, 2))
            ],
        }
    }


def generate_deterministic_id(index: int, stage: str) -> str:
    """Generates a deterministic but unique id based on index and stage"""
    return hashlib.sha256(f"{stage}_{index}".encode()).hexdigest()[:24]


def generate_json_like_data(
    stage: Literal["s1", "s2", "s3"], size: int = 100, seed: int = 42
) -> List[Dict[str, Any]]:
    random.seed(seed)
    data = []

    # Pre-generate common values to avoid repeated random calls
    names = ["Alice", "Bob", "Charlie", "David", None, None]  # Added more Nones
    ages = [random.randint(18, 80) for _ in range(10)] + [None, None]  # Added Nones to ages
    timestamps = [pendulum.now().subtract(days=d) for d in range(365)] + [None]
    versions = list(range(1, 6)) + [None]

    # Pre-generate a pool of nested structures with some None values
    nested_pool = {
        "small": [generate_nested_json_structure(0, 2) for _ in range(5)] + [None],
        "medium": [generate_nested_json_structure(0, 3) for _ in range(5)] + [None],
        "large": [generate_nested_json_structure(0, 4) for _ in range(5)] + [None],
    }

    nested_tables = {
        "users": (
            lambda: {
                "name": random.choice(names),
                "age": random.choice(ages),
                "preferences": (
                    random.choice(nested_pool["small"]) if random.random() > 0.2 else None
                ),
            },
            1.0,
        ),
        "orders": (
            lambda: {
                "order_date": random.choice(
                    [pendulum.now(), None, None]
                ),  # Increased chance of None
                "items": (
                    random.sample(nested_pool["medium"], k=random.randint(0, 3))
                    if random.random() > 0.3
                    else None
                ),
                "shipping": random.choice(nested_pool["small"]),
            },
            0.8,
        ),
        "products": (
            lambda: {
                "details": random.choice(nested_pool["large"]) if random.random() > 0.2 else None,
                "categories": random.choice(
                    [
                        random.sample(nested_pool["small"], k=2),
                        None,
                        [],
                        None,  # Added another None option
                    ]
                ),
                "specs": random.choice(nested_pool["medium"]) if random.random() > 0.25 else None,
            },
            0.7,
        ),
    }

    for i in range(size):
        base_doc = {
            "_id": generate_deterministic_id(i, stage),
            "timestamp": random.choice(timestamps),  # Now can be None
            "version": random.choice(versions),  # Now can be None
        }

        if stage == "s1":
            # Most consistent stage, but still with some missing fields
            doc = {
                **base_doc,
                **{k: v[0]() for k, v in nested_tables.items() if random.random() < v[1]},
            }
        elif stage == "s2":
            # Even more variable with missing fields
            doc = {
                **base_doc,
                **{k: v[0]() for k, v in nested_tables.items() if random.random() < v[1] * 0.9},
            }
            # Add some completely new fields, sometimes null
            if random.random() > 0.5:
                doc["dynamic_field"] = random.choice(
                    [generate_nested_json_structure(0, random.randint(1, 4)), None]
                )
        else:  # s3
            # Most chaotic stage with more nulls and missing fields
            doc = {
                **base_doc,
                **{
                    k: random.choice(
                        [
                            v[0](),
                            None,
                            None,  # Increased chance of None
                            {"modified": True},
                            [1, 2, 3],
                            "converted_to_string",
                        ]
                    )
                    for k, v in nested_tables.items()
                    if random.random() < v[1] * 0.7
                },
            }
            # Add stage specific fields, sometimes null
            if random.random() > 0.3:
                doc["s3_specific"] = random.choice([generate_nested_json_structure(0, 4), None])

            # Randomly remove some fields
            if random.random() > 0.8:
                keys_to_remove = random.sample(list(doc.keys() - {"_id"}), k=random.randint(1, 2))
                for key in keys_to_remove:
                    doc.pop(key)

        data.append(doc)

    return data


def test_generate_json_like_data():
    data = generate_json_like_data("s1", 10)
    assert len(data) == 10


def test_mssql_complex_json_loading() -> None:
    """Test loading jsonDB-like data into MSSQL with different sequences"""
    dest = mssql(create_indexes=True)

    @dlt.source(name="json_source", parallelized=True)
    def source(stage: Literal["s1", "s2", "s3"]):
        @dlt.resource(name="documents", write_disposition="merge", primary_key="_id")
        def documents():
            yield from generate_json_like_data(stage)

        return documents

    # Test sequence 1: s1 -> s2 -> s3
    p1 = pipeline("json_test_1", dataset_name="json_test_1_" + uniq_id())

    p1.run(source("s1"), destination=dest)
    p1.run(source("s2"), destination=dest)
    p1.run(source("s3"), destination=dest)

    # Test sequence 2: s2 -> s1 -> s3
    p2 = pipeline("json_test_2", dataset_name="json_test_2_" + uniq_id())

    p2.run(source("s2"), destination=dest)
    p2.run(source("s1"), destination=dest)
    p2.run(source("s3"), destination=dest)

    # Test sequence 3: s3 -> s2 -> s1
    p3 = pipeline("json_test_3", dataset_name="json_test_3_" + uniq_id())

    p3.run(source("s3"), destination=dest)
    p3.run(source("s2"), destination=dest)
    p3.run(source("s1"), destination=dest)

    # Test sequence 4: s2 with root_id -> s1 -> s3
    p4 = pipeline("json_test_4", dataset_name="json_test_4_" + uniq_id())

    @dlt.source(name="json_source_root_id", root_key=True)
    def source_with_root_id(stage: Literal["s1", "s2", "s3"]):
        @dlt.resource(name="documents", write_disposition="replace", primary_key="_id")
        def documents():
            yield from generate_json_like_data(stage)

        return documents

    p4.run(source_with_root_id("s2"), destination=dest)
    p4.run(source_with_root_id("s1"), destination=dest, write_disposition="merge")
    p4.run(source_with_root_id("s3"), destination=dest, write_disposition="merge")

    # Verify data for each pipeline
    for p in [p1, p2, p3, p4]:
        assert load_table_counts(p, "documents")["documents"] == 300
