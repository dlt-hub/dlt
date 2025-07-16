from typing import Any

import dlt

from dlt.common.destination.dataset import Dataset


def test_transformation_defaults() -> None:
    @dlt.transformation()
    def my_tf(dataset: Dataset) -> Any:
        yield dataset["example_table"].limit(5)

    assert my_tf.write_disposition == "append"
    # assert my_tf(dataset).materialization == "table"
    assert my_tf.table_name == "my_tf"
    assert my_tf.name == "my_tf"
