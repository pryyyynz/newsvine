from collections.abc import Mapping
import json
import math
from typing import Any

SparseVector = dict[str, float]


def serialize_sparse_vector(vector: Mapping[str, float]) -> str:
    payload = {
        str(key): float(value)
        for key, value in vector.items()
        if math.isfinite(float(value)) and float(value) != 0.0
    }
    return json.dumps(payload, separators=(",", ":"))


def deserialize_sparse_vector(raw: str | None) -> SparseVector:
    if not raw:
        return {}

    try:
        payload: Any = json.loads(raw)
    except json.JSONDecodeError:
        return {}

    if not isinstance(payload, dict):
        return {}

    parsed: SparseVector = {}
    for key, value in payload.items():
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(numeric) or numeric == 0.0:
            continue
        parsed[str(key)] = numeric

    return parsed


def l2_norm(vector: Mapping[str, float]) -> float:
    return math.sqrt(sum(value * value for value in vector.values()))


def l2_normalize(vector: Mapping[str, float]) -> SparseVector:
    norm = l2_norm(vector)
    if norm == 0.0:
        return {}
    return {key: value / norm for key, value in vector.items() if value != 0.0}


def cosine_similarity(lhs: Mapping[str, float], rhs: Mapping[str, float]) -> float:
    if not lhs or not rhs:
        return 0.0

    if len(lhs) > len(rhs):
        lhs, rhs = rhs, lhs

    dot = 0.0
    for key, value in lhs.items():
        dot += value * rhs.get(key, 0.0)

    lhs_norm = l2_norm(lhs)
    rhs_norm = l2_norm(rhs)
    if lhs_norm == 0.0 or rhs_norm == 0.0:
        return 0.0

    similarity = dot / (lhs_norm * rhs_norm)
    return max(-1.0, min(1.0, similarity))


def trim_sparse_vector(vector: Mapping[str, float], max_terms: int) -> SparseVector:
    if max_terms <= 0 or not vector:
        return {}
    if len(vector) <= max_terms:
        return {str(key): float(value) for key, value in vector.items() if value != 0.0}

    ranked = sorted(vector.items(), key=lambda item: abs(item[1]), reverse=True)
    return {str(key): float(value) for key, value in ranked[:max_terms] if value != 0.0}
