import hashlib
import math
from typing import Iterable, List

DEFAULT_DIM = 64


def _tokenize(text: str) -> List[str]:
    return [token for token in text.lower().split() if token]


def compute_embedding(text: str, dim: int = DEFAULT_DIM) -> List[float]:
    vector = [0.0] * dim
    tokens = _tokenize(text)
    if not tokens:
        return vector
    for token in tokens:
        digest = hashlib.sha256(token.encode("utf-8")).hexdigest()
        idx = int(digest, 16) % dim
        vector[idx] += 1.0
    norm = math.sqrt(sum(v * v for v in vector))
    if norm == 0:
        return vector
    return [v / norm for v in vector]


def embed_and_store(thread_ts: str, text: str, store_fn) -> None:
    vector = compute_embedding(text)
    store_fn(thread_ts, len(vector), vector)
