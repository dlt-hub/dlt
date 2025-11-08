from __future__ import annotations
from dataclasses import dataclass
from typing import Iterable, Iterator, List, Dict, Union, Sequence, Optional
import math

Number = Union[int, float]

@dataclass(frozen=True)
class RobustScaleConfig:
    """IQR-based scaling config."""
    clip_min: Optional[float] = None
    clip_max: Optional[float] = None
    eps: float = 1e-12  # protect against zero IQR

def _quantile(sorted_vals: Sequence[Number], q: float) -> float:
    if not 0.0 <= q <= 1.0:
        raise ValueError("q must be in [0,1]")
    n = len(sorted_vals)
    if n == 0:
        raise ValueError("Cannot compute quantile of empty data.")
    pos = (n - 1) * q
    lo = math.floor(pos); hi = math.ceil(pos)
    if lo == hi:
        return float(sorted_vals[lo])
    frac = pos - lo
    return float(sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac)

def robust_scale_values(values: Sequence[Number], cfg: RobustScaleConfig = RobustScaleConfig()) -> List[float]:
    """Return (x - median) / IQR with optional clipping."""
    if len(values) == 0:
        return []
    s = sorted(map(float, values))
    q1, q3, median = _quantile(s, 0.25), _quantile(s, 0.75), _quantile(s, 0.5)
    iqr = max(q3 - q1, cfg.eps)
    out = [(float(v) - median) / iqr for v in values]
    if cfg.clip_min is not None or cfg.clip_max is not None:
        lo = -math.inf if cfg.clip_min is None else cfg.clip_min
        hi =  math.inf if cfg.clip_max is None else cfg.clip_max
        out = [min(max(x, lo), hi) for x in out]
    return out

def robust_scale_rows(
    rows: Iterable[Dict[str, object]],
    columns: Sequence[str],
    cfg: RobustScaleConfig = RobustScaleConfig()
) -> Iterator[Dict[str, object]]:
    """Yield rows with selected numeric columns robust-scaled; others untouched."""
    cache: List[Dict[str, object]] = [dict(r) for r in rows]
    col_vectors: Dict[str, List[float]] = {}
    for col in columns:
        vec = []
        for r in cache:
            v = r.get(col)
            if v is None:
                continue
            try:
                vec.append(float(v))
            except (TypeError, ValueError):
                continue
        col_vectors[col] = vec

    scaled_cols: Dict[str, List[float]] = {c: robust_scale_values(v, cfg) for c, v in col_vectors.items()}
    idx = {c: 0 for c in columns}

    for r in cache:
        out = dict(r)
        for c in columns:
            v = r.get(c)
            if v is None:
                continue
            try:
                float(v)
            except (TypeError, ValueError):
                continue
            out[c] = scaled_cols[c][idx[c]]
            idx[c] += 1
        yield out
