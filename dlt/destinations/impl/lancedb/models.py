from typing import Union, List

import numpy as np
from lancedb.embeddings import OpenAIEmbeddings  # type: ignore
from lancedb.embeddings.registry import register  # type: ignore
from lancedb.embeddings.utils import TEXT  # type: ignore


@register("openai_patched")
class PatchedOpenAIEmbeddings(OpenAIEmbeddings):
    EMPTY_STRING_PLACEHOLDER: str = "___EMPTY___"

    def sanitize_input(self, texts: TEXT) -> Union[List[str], np.ndarray]:  # type: ignore[type-arg]
        """
        Replace empty strings with a placeholder value.
        """

        sanitized_texts = super().sanitize_input(texts)
        return [self.EMPTY_STRING_PLACEHOLDER if item == "" else item for item in sanitized_texts]

    def generate_embeddings(
        self,
        texts: Union[List[str], np.ndarray],  # type: ignore[type-arg]
    ) -> List[np.array]:  # type: ignore[valid-type]
        """
        Generate embeddings, treating the placeholder as an empty result.
        """
        embeddings: List[np.array] = super().generate_embeddings(texts)  # type: ignore[valid-type]

        for i, text in enumerate(texts):
            if text == self.EMPTY_STRING_PLACEHOLDER:
                embeddings[i] = np.zeros(self.ndims())

        return embeddings
