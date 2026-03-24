# Fields

Custom Peewee field types used in astra's database models (`astra.fields`). All standard field types
(e.g. `FloatField`, `IntegerField`, `TextField`) include a `GlossaryFieldMixin` that
automatically sets `help_text` from the glossary if not explicitly provided.

## `GlossaryFieldMixin`

A mixin class applied to all astra field types. When a field is bound to a model, if no
`help_text` has been set, it automatically looks up the field name in `Glossary` and uses
the matching description.

---

## `BitField`

```python
class BitField(*args, **kwargs)
```

A binary bit-field (extends Peewee's `BitField`) that supports per-flag `help_text`.
Defaults to `0`. Each flag is a boolean descriptor on the model instance.

### `BitField.flag`

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

## `PixelArray`

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

## `BasePixelArrayAccessor`

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

## `LogLambdaArrayAccessor`

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
