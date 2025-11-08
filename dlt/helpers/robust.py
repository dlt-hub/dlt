from __future__ import annotations
from dataclasses import dataclass
from typing import Iterable, Iterator, Mapping, Any, Sequence, List, Dict


@dataclass
class RobustScaleConfig:
    """Config for robust scaling based on median/IQR."""
    clip_min: float = float("-inf")
    clip_max: float = float("inf")
    eps: float = 1e-12  # small number to avoid division by very small IQR


# ---------- small stats helpers ----------
def _median(vals: Sequence[float]) -> float:
    s = sorted(vals)
    n = len(s)
    if n == 0:
        return 0.0
    m = n // 2
    return s[m] if n % 2 else 0.5 * (s[m - 1] + s[m])


def _percentile(vals: Sequence[float], p: float) -> float:
    """Simple linear interpolation percentile, p in [0,100]."""
    s = sorted(vals)
    n = len(s)
    if n == 0:
        return 0.0
    if p <= 0:
        return s[0]
    if p >= 100:
        return s[-1]
    k = (n - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, n - 1)
    return s[f] + (k - f) * (s[c] - s[f])


def _iqr(vals: Sequence[float]) -> float:
    return _percentile(vals, 75.0) - _percentile(vals, 25.0)


# ---------- core helpers ----------
def robust_scale_values(values: Iterable[float] | Sequence[float],
                        cfg: RobustScaleConfig = RobustScaleConfig()) -> List[float]:
    """Return robust z-scores: (x - median) / IQR, with optional clipping and zero-IQR guard."""
    xs = [float(v) for v in values]
    if not xs:
        return []
    med = _median(xs)
    spread = _iqr(xs)
    if abs(spread) < cfg.eps:
        # all equal (or effectively equal) → map to zeros
        return [0.0 for _ in xs]

    zs = [(x - med) / spread for x in xs]
    # clip if requested
    lo, hi = cfg.clip_min, cfg.clip_max
    return [min(hi, max(lo, z)) for z in zs]


def robust_scale_rows(rows: Iterable[Mapping[str, Any]],
                      columns: Sequence[str],
                      cfg: RobustScaleConfig = RobustScaleConfig()) -> Iterator[Dict[str, Any]]:
    """Yield rows where selected numeric columns are robust-scaled (others left untouched)."""
    cache = [dict(r) for r in rows]

    # collect numeric vectors per column
    col_vecs: Dict[str, List[float]] = {}
    for c in columns:
        vec: List[float] = []
        for r in cache:
            v = r.get(c)
            if v is None:
                continue
            try:
                vec.append(float(v))
            except (TypeError, ValueError):
                # non-numeric → skip for scaling, will remain unchanged
                continue
        col_vecs[c] = vec

    # scale each column
    scaled_cols: Dict[str, List[float]] = {c: robust_scale_values(vec, cfg) for c, vec in col_vecs.items()}
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


def robust_scale_values_to_range(values: Iterable[float] | Sequence[float],
                                 target_min: float,
                                 target_max: float,
                                 cfg: RobustScaleConfig = RobustScaleConfig()) -> List[float]:
    """Robust-scale values, then linearly map to [target_min, target_max]."""
    z = robust_scale_values(values, cfg)
    if not z:
        return []
    zmin, zmax = min(z), max(z)
    span = target_max - target_min
    if abs(zmax - zmin) < cfg.eps:
        mid = (target_min + target_max) / 2.0
        return [mid for _ in z]
    return [((v - zmin) / (zmax - zmin)) * span + target_min for v in z]


def robust_scale_rows_to_range(rows: Iterable[Mapping[str, Any]],
                               columns: Sequence[str],
                               target_min: float,
                               target_max: float,
                               cfg: RobustScaleConfig = RobustScaleConfig()) -> Iterator[Dict[str, Any]]:
    """Row-wise version mapped to [target_min, target_max] for selected numeric columns."""
    cache = [dict(r) for r in rows]
    # collect numeric vectors
    col_vecs: Dict[str, List[float]] = {}
    for c in columns:
        vec: List[float] = []
        for r in cache:
            v = r.get(c)
            if v is None:
                continue
            try:
                vec.append(float(v))
            except (TypeError, ValueError):
                continue
        col_vecs[c] = vec

    # scale to range per column
    scaled_cols: Dict[str, List[float]] = {
        c: robust_scale_values_to_range(vec, target_min, target_max, cfg)
        for c, vec in col_vecs.items()
    }
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
        
