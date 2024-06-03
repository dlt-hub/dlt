from typing import Union, List

import numpy as np
from lancedb.embeddings import TextEmbeddingFunction
from lancedb.pydantic import LanceModel, Vector

from dlt.destinations.impl.lancedb.utils import infer_lancedb_model_from_data


class MockEmbeddingFunc(TextEmbeddingFunction):
    def generate_embeddings(
        self, texts: Union[List[str], np.ndarray]
    ) -> List[np.array]:
        return [np.array(None)]

    def ndims(self) -> int:
        return 2


def test_infer_lancedb_model_from_data() -> None:
    data = [
        {"id__": "1", "item": "tyre", "price": 12.0, "customer": "jack"},
        {"id__": "2", "item": "wheel", "price": 100, "customer": "jill"},
    ]
    id_field_name = "id__"
    vector_field_name = "vector__"
    embedding_fields = ["item", "price"]
    embedding_model_func = MockEmbeddingFunc()

    inferred_model = infer_lancedb_model_from_data(
        data, id_field_name, vector_field_name, embedding_fields, embedding_model_func
    )

    expected_fields = {
        "id__": (str, ...),
        "vector__": (Vector(2), ...),
        "item": (str, embedding_model_func.SourceField()),
        "price": (float, embedding_model_func.SourceField()),
        "customer": (str, ...),
    }

    assert issubclass(inferred_model, LanceModel)
    for field_name, (field_type, field_default) in expected_fields.items():
        assert field_name in inferred_model.model_fields
        inferred_default = inferred_model.model_fields[field_name].default
