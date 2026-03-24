# API Reference

This page documents the core public API of `astra`. Since astra's dependencies are not
installable in the ReadTheDocs build environment, this documentation is written manually
rather than using autodoc.

## Core API (`astra`)

### `astra.__version__`

```python
astra.__version__  # e.g. "0.8.1"
```

The current version of astra as a string in `"major.minor.patch"` format.

---

### `@task`

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

### `generate_queries_for_task`

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

---

## Utilities (`astra.utils`)

### `expand_path`

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

### `Timer`

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

### `version_string_to_integer`

```python
def version_string_to_integer(version_string: str) -> int
```

Convert a version string like `"0.8.1"` to an integer representation. Each component
occupies three decimal digits: major is multiplied by 10^6, minor by 10^3, and patch by 1.

**Parameters:**

| Parameter | Type | Description |
|---|---|---|
| `version_string` | `str` | A version string in `"major.minor.patch"` format. |

**Returns:** An integer encoding of the version.

**Example:**

```python
from astra.utils import version_string_to_integer

version_string_to_integer("0.8.1")
# 8001
```

---

### `version_integer_to_string`

```python
def version_integer_to_string(version_integer: int) -> str
```

Convert an integer version back to a `"major.minor.patch"` string. The inverse of
`version_string_to_integer`.

**Parameters:**

| Parameter | Type | Description |
|---|---|---|
| `version_integer` | `int` | An integer-encoded version number. |

**Returns:** A version string in `"major.minor.patch"` format.

**Example:**

```python
from astra.utils import version_integer_to_string

version_integer_to_string(8001)
# "0.8.1"
```

---

### `flatten`

```python
def flatten(struct) -> list
```

Recursively flatten a nested structure of dicts, lists, and scalars into a single flat list.
Dict values are extracted (keys are discarded). Strings are treated as atoms, not iterated.

**Parameters:**

| Parameter | Type | Description |
|---|---|---|
| `struct` | any | A nested structure (dict, list, tuple, scalar, or `None`). |

**Returns:** A flat list of all leaf values. Returns `[]` for `None`.

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

### `log`

```python
from astra.utils import log
```

A pre-configured logger instance (from `sdsstools`) for astra. Uses the name `"astra"`
and formats messages as `"%(asctime)s %(message)s"`. Supports standard Python logging
methods: `log.info(...)`, `log.warning(...)`, `log.error(...)`, `log.exception(...)`, etc.

---

## Fields (`astra.fields`)

Custom Peewee field types used in astra's database models. All standard field types
(e.g. `FloatField`, `IntegerField`, `TextField`) include a `GlossaryFieldMixin` that
automatically sets `help_text` from the glossary if not explicitly provided.

### `GlossaryFieldMixin`

A mixin class applied to all astra field types. When a field is bound to a model, if no
`help_text` has been set, it automatically looks up the field name in `Glossary` and uses
the matching description.

---

### `BitField`

```python
class BitField(*args, **kwargs)
```

A binary bit-field (extends Peewee's `BitField`) that supports per-flag `help_text`.
Defaults to `0`. Each flag is a boolean descriptor on the model instance.

**Methods:**

#### `BitField.flag`

```python
def flag(value=None, help_text=None) -> FlagDescriptor
```

Register a new flag in the bit field.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `value` | `int` or `None` | `None` | The bit value. If `None`, auto-increments (1, 2, 4, 8, ...). |
| `help_text` | `str` or `None` | `None` | Description of the flag. If `None`, looked up from the glossary using the attribute name. |

**Returns:** A `FlagDescriptor` that acts as a boolean property on model instances.

**Example:**

```python
from astra.fields import BitField

class MyModel(BaseModel):
    result_flags = BitField(default=0)
    flag_bad_fit = result_flags.flag(help_text="Fit did not converge")
    flag_low_snr = result_flags.flag(help_text="Signal-to-noise ratio below threshold")

# Usage:
instance = MyModel()
instance.flag_bad_fit = True
print(instance.flag_bad_fit)  # True
```

The `FlagDescriptor` also supports SQL expressions for queries:

```python
MyModel.select().where(MyModel.flag_bad_fit)
```

---

### `PixelArray`

```python
class PixelArray(
    ext=None,
    column_name=None,
    transform=None,
    accessor_class=PixelArrayAccessorFITS,
    help_text=None,
    accessor_kwargs=None,
    **kwargs
)
```

A virtual Peewee field for array data (e.g. spectra, wavelengths) stored outside the
database in FITS, HDF5, or pickle files. The data is loaded lazily on first access.

**Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ext` | `int`, `str`, callable, or `None` | `None` | The FITS extension (or HDF5 dataset path). Can be a callable that takes the model instance and returns the extension. |
| `column_name` | `str` or `None` | `None` | Column name within the extension. If `None`, defaults to the field name. |
| `transform` | callable or `None` | `None` | A function to transform the data after loading. For FITS accessors it receives `(data, image, instance)`. |
| `accessor_class` | class | `PixelArrayAccessorFITS` | The accessor class to use. Options: `PixelArrayAccessorFITS`, `PixelArrayAccessorHDF`, `PickledPixelArrayAccessor`, `LogLambdaArrayAccessor`. |
| `help_text` | `str` or `None` | `None` | Description of the field. |
| `accessor_kwargs` | `dict` or `None` | `None` | Extra keyword arguments passed to the accessor constructor. |

**Example:**

```python
from astra.fields import PixelArray, PixelArrayAccessorFITS

class BossVisitSpectrum(BaseModel):
    wavelength = PixelArray(ext=1, column_name="loglam", transform=lambda x, *_: 10**x)
    flux = PixelArray(ext=1, column_name="flux")
    ivar = PixelArray(ext=1, column_name="ivar")

# Data is loaded lazily:
spectrum = BossVisitSpectrum.get_by_id(1)
print(spectrum.flux)  # numpy array loaded from FITS on first access
```

---

### `BasePixelArrayAccessor`

```python
class BasePixelArrayAccessor(model, field, name, ext, column_name, transform=None, help_text=None, **kwargs)
```

Base class for all pixel array accessors. Manages a per-instance `__pixel_data__` cache
dictionary. Subclasses implement `__get__` to load data from their respective file formats.

**Built-in accessor subclasses:**

| Class | Description |
|---|---|
| `PixelArrayAccessorFITS` | Loads arrays from FITS files using `astropy.io.fits`. Supports both image and column access. |
| `PixelArrayAccessorHDF` | Loads arrays from HDF5 files using `h5py`. |
| `PickledPixelArrayAccessor` | Loads arrays from Python pickle files. |

---

### `LogLambdaArrayAccessor`

```python
class LogLambdaArrayAccessor(model, field, name, ext, column_name, crval, cdelt, naxis, transform=None, help_text=None)
```

A pixel array accessor that generates a log-linear wavelength array from header parameters,
rather than reading it from a file. The wavelength grid is computed as:

```python
wavelength = 10 ** (crval + cdelt * np.arange(naxis))
```

**Additional parameters (beyond `BasePixelArrayAccessor`):**

| Parameter | Type | Description |
|---|---|---|
| `crval` | `float` | Log10 of the reference wavelength. |
| `cdelt` | `float` | Log10 wavelength step per pixel. |
| `naxis` | `int` | Number of pixels. |

**Example:**

```python
from astra.fields import PixelArray, LogLambdaArrayAccessor

class ApogeeVisitSpectrum(BaseModel):
    wavelength = PixelArray(
        accessor_class=LogLambdaArrayAccessor,
        accessor_kwargs=dict(crval=4.179, cdelt=6e-6, naxis=8575)
    )
```

---

## Glossary (`astra.glossary`)

The glossary system provides centralized, human-readable descriptions for database fields.
It is used automatically by `GlossaryFieldMixin` to populate `help_text` on Peewee fields.

### `Glossary`

```python
class Glossary(BaseGlossary, metaclass=GlossaryType)
```

A class whose attributes are field-name-to-description mappings. Access any attribute
to get its description string.

```python
from astra.glossary import Glossary

Glossary.teff
# "Effective temperature [K]"

Glossary.logg
# "Surface gravity [dex]"

Glossary.sdss_id
# "SDSS unique source identifier"
```

#### Special prefix/suffix resolution

If a field name is not defined directly, the glossary resolves it by checking for
known prefixes and suffixes:

| Pattern | Rule | Example |
|---|---|---|
| `e_X` | "Error on" + description of `X` | `Glossary.e_teff` -> `"Error on effective temperature [K]"` |
| `X_flags` | "Flags for" + description of `X` | `Glossary.teff_flags` -> `"Flags for effective temperature [K]"` |
| `initial_X` | "Initial" + description of `X` | `Glossary.initial_teff` -> `"Initial effective temperature [K]"` |
| `X_rchi2` | "Reduced chi-square value for" + description of `X` | `Glossary.teff_rchi2` -> `"Reduced chi-square value for effective temperature [K]"` |
| `raw_X` | "Raw" + description of `X` | `Glossary.raw_teff` -> `"Raw effective temperature [K]"` |
| `rho_X_Y` | "Correlation coefficient between X and Y" | `Glossary.rho_teff_logg` -> `"Correlation coefficient between TEFF and LOGG"` |

#### Context manager usage

The `Glossary` can be instantiated with a context string to prepend to all descriptions:

```python
from astra.glossary import Glossary

with Glossary("SLAM") as g:
    print(g.teff)
    # "SLAM effective temperature [K]"
```

### How fields use the glossary

When a field (e.g. `FloatField`, `BitField`) is bound to a model and no `help_text` is
provided, the `GlossaryFieldMixin.bind()` method automatically sets:

```python
self.help_text = getattr(Glossary, self.name, None)
```

This means naming a model field `teff` will automatically give it the help text
`"Effective temperature [K]"` without any manual annotation.
