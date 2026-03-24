# Core API

## `astra.__version__`

```python
astra.__version__  # e.g. "0.8.1"
```

The current version of astra as a string in `"major.minor.patch"` format.

---

## `@task`

```python
@task
def my_pipeline(spectra, ..., **kwargs) -> Iterable[MyModel]:
    ...
    yield result
```

A decorator for functions that serve as astra tasks. The decorated function should be a
**generator** that `yield`s result objects (typically subclasses of `astra.models.BaseModel`).
The decorator handles batching results and writing them to the database.

**Keyword arguments passed at call time:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `live` | `bool` | `False` | If `True`, results are yielded as they are completed, even if not yet written to the database. If `False`, results are written to the database in batches before being yielded. |
| `batch_size` | `int` | `1000` | Number of rows to insert per batch. |
| `write_frequency` | `int` | `300` | Seconds to wait between database write checkpoints. |
| `write_to_database` | `bool` | `True` | If `False`, results are yielded directly without writing to the database. |
| `re_raise_exceptions` | `bool` | `True` | If `True`, exceptions raised in the task are re-raised. Otherwise they are logged and ignored. |

**Example:**

```python
from astra import task

@task
def my_analysis(spectra, **kwargs):
    for spectrum in spectra:
        result = MyModel(spectrum_pk=spectrum.spectrum_pk, ...)
        yield result
```

When calling a decorated task, you can override the decorator parameters:

```python
for result in my_analysis(spectra, batch_size=500, write_frequency=60):
    print(result)
```

---

## `generate_queries_for_task`

```python
def generate_queries_for_task(
    task,
    input_model=None,
    sdss_ids=None,
    limit=None,
    page=None
)
```

Generate Peewee queries for input data that needs to be processed by a given task. The
function inspects the task's type annotations to determine which input spectrum or source
models are expected and which output model to check against. Queries are ordered by
modified time (most recent first), and exclude rows that have already been processed at the
current astra version (unless the input has been modified since).

**Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `task` | `str` or callable | *(required)* | The task name (e.g. `"slam.slam"`) or the callable itself. |
| `input_model` | model or `None` | `None` | The input spectrum model. If `None`, inferred from the task function signature. |
| `sdss_ids` | list or `None` | `None` | A list of SDSS IDs to filter results to specific sources. |
| `limit` | `int` or `None` | `None` | Maximum number of rows per query. |
| `page` | `int` or `None` | `None` | Page number for pagination (used with `limit`). |

**Yields:** `(input_model, query)` tuples, where `input_model` is a Peewee model class and `query` is a `SelectQuery`.

**Example:**

```python
from astra import generate_queries_for_task

for input_model, query in generate_queries_for_task("slam.slam", limit=100):
    print(f"{input_model}: {query.count()} spectra to process")
```
