# Tests

The test suite lives in the `tests/` directory at the repository root. Tests use [pytest](https://docs.pytest.org/).

## Running tests

```bash
# Run the full suite
pytest

# Run a specific test file
pytest tests/test_pipelines.py

# Run a specific test class or function
pytest tests/test_pipelines.py::TestFerreUtilsHelpers::test_get_ferre_spectrum_name

# With coverage
pytest --cov=astra
```

## In-memory database

Tests that interact with the database set `ASTRA_DATABASE_PATH` to `:memory:` **before** importing any Astra modules. This is done at the top of each test file:

```python
import os
os.environ["ASTRA_DATABASE_PATH"] = ":memory:"
```

This ensures that every test run starts with a fresh, empty SQLite database. No external database server is needed.

When a test needs tables to exist, it creates them explicitly:

```python
from astra.models.source import Source
from astra.models.spectrum import Spectrum
from astra.models.pipeline import PipelineOutputMixin

class Dummy(PipelineOutputMixin):
    pass

for model in (Source, Spectrum, Dummy):
    model.create_table()
```

## Test structure

The test files cover different aspects of the codebase:

| File | What it tests |
|---|---|
| `test_models.py` | ORM model behaviour: unique constraints, upsert logic, version handling. |
| `test_pipelines.py` | Pipeline utility functions (FERRE helpers, ASPCAP helpers, spectral resampling, The Payne utilities, SLAM wavelength tools, SnowWhite helpers). Uses `_load_module_from_file` to load pipeline modules directly, bypassing heavy `__init__.py` imports. |
| `test_fields.py` | Custom Peewee field types (`BitField`, etc.). |
| `test_glossary.py` | Glossary consistency checks. |
| `test_spectrum_mixin.py` | `SpectrumMixin` properties and methods. |
| `test_model_flags.py` | BitField flag definitions on pipeline models. |
| `test_model_paths.py` | File path generation on spectrum models. |
| `test_pipeline_from_spectrum.py` | `PipelineOutputMixin.from_spectrum()` behaviour. |
| `test_task_query_builder.py` | `generate_queries_for_task` query construction. |
| `test_utils.py` | Utility functions (version encoding, path expansion, etc.). |
| `test_base_helpers.py` | `BaseModel` helper methods. |

## Loading modules without side effects

Some pipeline modules have heavy imports (TensorFlow, large data files) or trigger database connections through their `__init__.py`. The test suite avoids this by loading individual `.py` files directly using `importlib`:

```python
import importlib.util

def _load_module_from_file(name, path):
    """Load a Python module directly from a file path."""
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

# Load just the utils module from a pipeline, skipping __init__.py
mod = _load_module_from_file(
    "aspcap_utils",
    os.path.join(_SRC, "astra", "pipelines", "aspcap", "utils.py"),
)
```

This pattern is used extensively in `test_pipelines.py` to test pipeline helper functions in isolation.

## Writing new tests

When adding a test for a new pipeline or model:

1. Set `os.environ["ASTRA_DATABASE_PATH"] = ":memory:"` at the top of the file, before any `astra` imports.
2. Create any needed tables in a fixture or at the start of the test.
3. Use `_load_module_from_file` if you only need to test utility functions and want to avoid heavy dependencies.
4. For integration tests that exercise the `@task` decorator, create a dummy pipeline model, define a small `@task` function, and pass a handful of test spectra through it (see `test_pipeline_replace_on_conflict` in `test_models.py` for an example).
