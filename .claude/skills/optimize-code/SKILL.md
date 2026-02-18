---
name: optimize-code
description: Analyze and optimize Python code performance in critical paths
argument-hint: <file-path-or-description> [-- <critical-path-hints>]
---

# Optimize Code

You are a Python performance optimization expert. You work diligently and check meticulously every fragment of code in the critical path. You never assume - you trace actual execution paths, verify with evidence, and question defensive patterns that may have become unnecessary.

**ALWAYS benchmark everything.** Use `timeit` oneliners for quick validation, write dedicated benchmarks for optimization variants, and as the final step, benchmark optimized code against the baseline (devel branch).

Parse `$ARGUMENTS` to extract:
- Everything before `--` is the **target**: file path, function name, or description of code to optimize
- Everything after `--` is **critical path hints**: which code paths matter most, what to ignore

## Phase 1: Understand the Critical Path

SKIP if optimizing a single (or 1-2 ) functions where critical path is clear, otherwise INTERROGATE the user to understand the hot path before making any changes.

### 1.1 Identify the target

If `$ARGUMENTS` contains a file path, read it. Otherwise, ask the user:
- "Which file or function should I optimize?"
- "What is the entry point for the hot path?"

### 1.2 Learn the typical case

Ask about the 90% case to focus optimization effort:
- "What data shapes are most common?" (flat dicts, nested objects, lists)
- "Which branches are taken most often?"
- "What can we ignore?" (e.g., "ignore pandas/arrow, focus on plain dicts")

### 1.3 Trace the call chain

Follow the code from entry point through each function call:
```
entry_point() → helper_a() → helper_b() → actual_work()
```

Read each function in the chain. Identify the innermost loop where per-item work happens.

### 1.4 Establish baseline benchmark

Before any optimization, create a benchmark for the current state:
```python
import timeit
# Quick validation
python -c "import timeit; print(timeit.timeit('target_function()', setup='...', number=1000))"
```

## Phase 2: Analyze for Optimization Opportunities

Look for these patterns in the critical path:

### 2.1 Unnecessary work

- **Redundant copies**: `dict(x)` or `list(x)` when reference would suffice
- **Defensive copies**: Trace data origin - if source returns fresh object, copy is redundant
- **Repeated computations**: Same value computed multiple times in loop

### 2.2 Function call overhead

- **Hot helper functions**: Consider inlining if called per-item
- **Uncached method lookups**: `self.method` in loop vs `method = self.method` before loop
- **Duplicate isinstance/hasattr checks**: Same check in multiple places

### 2.3 Happy path shortcuts

- **Early exit for common case**: Check fast path first, skip expensive work
- **Identity vs equality**: `x is y` faster than `x == y` when applicable


### 2.4 Consider `__slots__` for hot objects

Objects created frequently in hot paths benefit from `__slots__`:
- 30-40% less memory (no `__dict__`)
- Faster attribute access (92% of local variable speed vs 59% for regular attributes)

```python
# Bad: regular class, dict-based attributes
class Row:
    def __init__(self, table, data):
        self.table = table
        self.data = data

# Good: slotted class
class Row:
    __slots__ = ['table', 'data']
    def __init__(self, table, data):
        self.table = table
        self.data = data
```

### 2.5 Inner function overhead

Inner functions (closures) defined inside loops or hot functions are recreated on every call. This can be a **major** performance hit:

```python
# Bad: inner function recreated per call
def process(items):
    def transform(x):  # created every time process() is called
        return x * 2
    return [transform(i) for i in items]

# Good: module-level or method
def _transform(x):
    return x * 2

def process(items):
    return [_transform(i) for i in items]

# Good: inline if simple
def process(items):
    return [x * 2 for x in items]
```

Signs to look for:
- `def` inside another `def`
- Lambda inside frequently-called function
- Closures capturing loop variables


### 2.6 Convert recursion to iteration

Recursive functions have overhead per call (stack frame, argument passing) and risk stack overflow on deep structures. Convert to stack-based iteration:

```python
# Bad: recursive traversal
def flatten(obj, path=""):
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield from flatten(v, f"{path}.{k}")  # recursive call
    else:
        yield path, obj

# Good: stack-based iteration
def flatten(obj):
    stack = [(obj, "")]
    while stack:
        current, path = stack.pop()
        if isinstance(current, dict):
            for k, v in current.items():
                stack.append((v, f"{path}.{k}"))
        else:
            yield path, current
```

Benefits:
- No function call overhead per level
- Predictable memory (heap vs stack)
- No recursion limit issues
- Can be easier to add early-exit logic

WARN USER:
- stack method MAY change the order of processing and ie. yielded elements. ALWAYS suggest to write a test for original code.

## Phase 3: Benchmark Everything

### 3.1 Always benchmark caches

Caches (lru_cache, manual dicts, memoization) can hurt performance if:
- Cache miss is common (lookup overhead without benefit)
- Cached computation is cheap (cache overhead exceeds saved work)
- Memory pressure causes cache eviction

**Benchmark caches even if they already exist** - they may have been added speculatively:

```python
# Does this cache actually help? Benchmark with and without!
@lru_cache(maxsize=128)
def get_column_type(col_name):
    return self.schema.columns[col_name]["data_type"]

# Compare:
# 1. With cache (current)
# 2. Without cache (direct lookup)
# 3. Different cache size
```

### 3.2 Quick validation with timeit

Before and after each micro-optimization:
```bash
python -c "import timeit; d={'a':1,'b':2}; print('copy:', timeit.timeit('dict(d)', globals={'d':d}, number=1000000))"
python -c "import timeit; d={'a':1,'b':2}; print('ref:', timeit.timeit('x=d', globals={'d':d}, number=1000000))"
```

### 3.3 Write dedicated benchmarks

For optimization variants, create benchmark scripts in `experiments/` folder:
```python
"""Benchmark: optimization variant comparison"""
import timeit

def variant_original(): ...
def variant_optimized(): ...

print("original:", timeit.timeit(variant_original, number=10000))
print("optimized:", timeit.timeit(variant_optimized, number=10000))
```

### 3.4 Process isolation and thermal management

Run benchmarks in separate processes with pauses:
```bash
python experiments/bench.py devel_case
sleep 10
python experiments/bench.py optimized_case
```

### 3.5 Variance checking

Run each benchmark 3+ times to verify stability:
```bash
for i in 1 2 3; do
    echo "=== Run $i ==="
    python experiments/bench.py case_name
    sleep 5
done
```

Variance over 10-15% suggests external factors (thermal throttling, system load).

### 3.6 Use realistic benchmark data

Use real test data from `tests/normalize/cases/`:

| File | Size | Use Case |
|------|------|----------|
| `ethereum.blocks.*.json` + `schemas/ethereum.schema.json` | 2MB | Deep nesting, warm/cold path normalizer |
| `github.events.*.json` | 1.7MB | Dynamic table routing, many event types |
| `github.issues.*.json` | 526KB | REST API, moderate nesting |

Load with `from dlt.common.json import json`. For flat rows with ISO timestamps, use `mimesis` (in dev deps) to generate synthetic DB data.

### 3.7 Final benchmark: Optimized vs Devel

As the last step, compare against baseline branch:
```bash
# On devel branch (main repo)
cd /path/to/main && python experiments/bench.py case_name

sleep 10

# On optimized branch (worktree)
cd /path/to/worktree && python experiments/bench.py case_name
```

Report: `devel_time / optimized_time = X.XXx speedup`

## Phase 4: Implement and Verify

### 4.1 Make changes incrementally

One optimization at a time. Benchmark after each change.

### 4.2 Verify correctness

- Run existing tests: `make test-common`
- Compare outputs before/after on sample data

### 4.3 Document assumptions

Add comments explaining why optimization is safe:
```python
# columns dict is never mutated after get_table_columns() returns,
# so we can store reference instead of copying
self._current_columns = columns
```

## Phase 5: Report Results

Produce a summary:
```
## Optimization Summary

### Changes Made
1. Replaced dict copy with identity check (buffered.py:100)
2. Inlined count_rows_in_items for common case (buffered.py:102)

### Benchmark Results
| Case | Devel | Optimized | Speedup |
|------|-------|-----------|---------|
| flat_100 | 10.4s | 2.2s | 4.76x |
| nested_20 | 4.9s | 2.0s | 2.47x |

### Assumptions
- columns dict is immutable after creation
- 90% of items are plain dicts, not arrow/pandas
```
