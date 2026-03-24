# Utilities

Utility functions from `astra.utils`.

## `expand_path`

```python
def expand_path(path: str) -> str
```

Expand a path string to its full absolute path, resolving environment variables (`$VAR`)
and user home directory (`~`).

**Parameters:**

| Parameter | Type | Description |
|---|---|---|
| `path` | `str` | A short-hand path (e.g. `"$BOSS_SPECTRO_REDUX/v6_1_1"` or `"~/data"`). |

**Returns:** The fully expanded path as a string.

**Example:**

```python
from astra.utils import expand_path

expand_path("~/data/$RELEASE/spectra")
# "/home/user/data/sdss5/spectra"
```

---

## `Timer`

```python
class Timer(
    iterable,
    frequency=None,
    callback=None,
    attr_t_elapsed=None,
    attr_t_overhead=None,
    skip_result_callable=lambda x: x is Ellipsis
)
```

A context-manager and iterator that tracks elapsed time per yielded result and
separates computation time from overhead time. It is used internally by the `@task`
decorator to measure pipeline performance.

**Constructor parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `iterable` | iterable | *(required)* | The iterable to wrap and time. |
| `frequency` | `float` or `None` | `None` | Seconds between database checkpoint triggers. If `None`, checkpoints are never triggered. |
| `callback` | callable or `None` | `None` | A function called when the timer exits, receiving the elapsed time as its argument. |
| `attr_t_elapsed` | `str` or `None` | `None` | Attribute name to set on each yielded item with its elapsed time. |
| `attr_t_overhead` | `str` or `None` | `None` | Attribute name to set on each yielded item with the mean overhead time. |
| `skip_result_callable` | callable | `lambda x: x is Ellipsis` | If this returns `True` for a result, the interval is counted as overhead rather than computation time. Use `yield ...` (Ellipsis) to mark overhead intervals. |

**Properties:**

| Property | Description |
|---|---|
| `elapsed` | Total seconds since the timer started. |
| `mean_overhead_per_result` | Mean overhead time per result in seconds. |
| `check_point` | Returns `True` if `frequency` seconds have passed since the last checkpoint. |

**Methods:**

| Method | Description |
|---|---|
| `add_overheads(items)` | Distributes accumulated overhead time across a list of result items. |
| `pause()` | Returns a context manager; time spent inside `with timer.pause(): ...` is excluded from timing. |

**Example:**

```python
from astra.utils import Timer

with Timer(my_generator(), frequency=60, attr_t_elapsed="t_elapsed") as timer:
    for result in timer:
        if timer.check_point:
            save_results()
```

---

## `version_string_to_integer`

```python
def version_string_to_integer(version_string: str) -> int
```

Convert a version string like `"0.8.1"` to an integer representation. Each component
occupies three decimal digits: major is multiplied by 10^6, minor by 10^3, and patch by 1.

**Example:**

```python
from astra.utils import version_string_to_integer

version_string_to_integer("0.8.1")
# 8001
```

---

## `version_integer_to_string`

```python
def version_integer_to_string(version_integer: int) -> str
```

Convert an integer version back to a `"major.minor.patch"` string. The inverse of
`version_string_to_integer`.

**Example:**

```python
from astra.utils import version_integer_to_string

version_integer_to_string(8001)
# "0.8.1"
```

---

## `flatten`

```python
def flatten(struct) -> list
```

Recursively flatten a nested structure of dicts, lists, and scalars into a single flat list.
Dict values are extracted (keys are discarded). Strings are treated as atoms, not iterated.

**Example:**

```python
from astra.utils import flatten

flatten({"a": [1, 2], "b": {"c": 3}})
# [1, 2, 3]

flatten("hello")
# ["hello"]

flatten(None)
# []
```

---

## `log`

```python
from astra.utils import log
```

A pre-configured logger instance (from `sdsstools`) for astra. Uses the name `"astra"`
and formats messages as `"%(asctime)s %(message)s"`. Supports standard Python logging
methods: `log.info(...)`, `log.warning(...)`, `log.error(...)`, `log.exception(...)`, etc.
