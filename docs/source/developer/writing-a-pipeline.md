# Writing a Pipeline

This guide walks through how to add a new analysis pipeline to Astra. A pipeline consists of two parts:

1. A **model** in `src/astra/models/` that defines the database table for the pipeline's output.
2. A **task function** in `src/astra/pipelines/` that does the analysis and yields results.

We will use a fictional pipeline called "Rocket" as an example throughout.

## Step 1: Define the output model

Create `src/astra/models/rocket.py`. The model must extend `PipelineOutputMixin`, which provides common columns (`task_pk`, `source_pk`, `spectrum_pk`, `v_astra`, `created`, `modified`, timing fields, and `tag`).

```python
from astra.fields import FloatField, TextField, BitField
from astra.models.pipeline import PipelineOutputMixin


class Rocket(PipelineOutputMixin):

    """Results from the Rocket pipeline."""

    #> Stellar Parameters
    teff = FloatField(null=True, help_text="Effective temperature [K]")
    e_teff = FloatField(null=True, help_text="Uncertainty in Teff [K]")
    logg = FloatField(null=True, help_text="Surface gravity [dex]")
    e_logg = FloatField(null=True, help_text="Uncertainty in logg [dex]")

    #> Summary Statistics
    rchi2 = FloatField(null=True, help_text="Reduced chi-squared")
    result_flags = BitField(default=0)

    flag_low_snr = result_flags.flag(2**0, help_text="S/N is too low")
    flag_no_converge = result_flags.flag(2**1, help_text="Fit did not converge")
```

### Key conventions

- **Category headers**: Lines like `#> Stellar Parameters` above a field group are parsed by `BaseModel.category_headers` and used to add section headers in FITS output files.
- **`help_text`**: Used as the FITS column comment. Keep it under ~47 characters.
- **`BitField` flags**: Use `result_flags.flag(2**N)` to define individual boolean flags packed into a single integer column. Each flag is a power of two.
- **`null=True`**: Almost all pipeline output fields should be nullable, because a pipeline may fail on a given spectrum and still want to record a row (e.g., with a flag set).
- **Unique constraint**: `PipelineOutputMixin` enforces a unique constraint on `(spectrum_pk, v_astra_major_minor)`. This means each pipeline produces at most one result per spectrum per major.minor version of Astra. Re-running a pipeline for the same version will update existing rows rather than creating duplicates.

### `from_spectrum`

`PipelineOutputMixin` provides a `from_spectrum(spectrum, **kwargs)` class method that creates a new model instance, automatically copying `source_pk` and `spectrum_pk` from the input spectrum:

```python
result = Rocket.from_spectrum(spectrum, teff=5000, logg=4.5)
```

This is the standard way to construct result objects inside a pipeline.

## Step 2: Write the pipeline task

Create `src/astra/pipelines/rocket/__init__.py`. The key ingredients are:

1. Import `task` from `astra`.
2. Decorate the main function with `@task`.
3. The function must be a **generator** that `yield`s model instances.
4. Use Python type annotations to declare the input spectrum type and the output model type.

```python
from typing import Iterable, Optional
from astra import task
from astra.models.boss import BossVisitSpectrum
from astra.models.rocket import Rocket
from astra.utils import log


@task
def rocket(
    spectra: Iterable[BossVisitSpectrum],
    some_parameter: Optional[float] = 1.0,
    **kwargs,
) -> Iterable[Rocket]:
    """
    Estimate stellar parameters with the Rocket method.

    :param spectra:
        An iterable of input spectra.
    :param some_parameter:
        A tuning knob.
    """

    # One-time setup (e.g., load a model). The @task decorator's Timer
    # tracks this as "overhead" time separate from per-spectrum time.
    model = _load_rocket_model()

    for spectrum in spectra:
        try:
            teff, e_teff, logg, e_logg, rchi2 = _fit(
                spectrum.wavelength,
                spectrum.flux,
                spectrum.ivar,
                model,
                some_parameter,
            )
        except Exception:
            log.exception(f"Failed on {spectrum}")
            yield Rocket.from_spectrum(spectrum, flag_no_converge=True)
            continue

        yield Rocket.from_spectrum(
            spectrum,
            teff=teff,
            e_teff=e_teff,
            logg=logg,
            e_logg=e_logg,
            rchi2=rchi2,
        )
```

### How the `@task` decorator works

The `@task` decorator (defined in `src/astra/__init__.py`) wraps a generator function and handles:

1. **Timing**: It wraps the generator in a `Timer` context manager that measures elapsed time and overhead per result. The `t_elapsed` and `t_overhead` fields are automatically populated on each yielded result.

2. **Batched database writes**: Yielded results are collected into batches (default `batch_size=1000`) and periodically flushed to the database (default every `write_frequency=300` seconds). The flush uses an upsert (`INSERT ... ON CONFLICT ... UPDATE`) so re-running a pipeline for the same Astra version updates existing rows.

3. **Error handling**: Exceptions inside the generator are logged. By default (`re_raise_exceptions=True`) they are re-raised after logging; set to `False` to continue past errors.

4. **Return semantics**: After database writes, the decorator yields back the results (now with `task_pk` and `created` populated from the database). Callers iterating over the task get fully-persisted result objects.

You can control behaviour with keyword arguments when calling the task:

```python
# Run without writing to the database (useful for debugging)
for result in rocket(spectra, write_to_database=False):
    print(result.teff)

# Change batch size and write frequency
for result in rocket(spectra, batch_size=500, write_frequency=60):
    ...
```

### Type annotations matter

The type annotations on the task function are not just documentation -- Astra inspects them at runtime:

- **`spectra: Iterable[BossVisitSpectrum]`** tells `generate_queries_for_task` which input spectrum table(s) to query. The system uses this to build a query that finds spectra not yet processed by this pipeline for the current Astra version.
- **`-> Iterable[Rocket]`** tells the system which output model to use for the upsert conflict resolution.

If your pipeline can accept multiple spectrum types, use a `Union`:

```python
from typing import Union
from astra.models.apogee import ApogeeCoaddedSpectrumInApStar

def rocket(
    spectra: Iterable[Union[BossVisitSpectrum, ApogeeCoaddedSpectrumInApStar]],
    **kwargs,
) -> Iterable[Rocket]:
    ...
```

### Yielding partial or flagged results

When a spectrum cannot be processed (e.g., wrong target type, too low S/N), yield a result with the appropriate flag set and no parameter values. This records that the pipeline has seen this spectrum, preventing it from being re-queued:

```python
yield Rocket.from_spectrum(spectrum, flag_low_snr=True)
```

Because parameter fields are `null=True`, the unfilled fields will be `NULL` in the database.

## Step 3: Run the pipeline

### From Python

```python
from astra.models.boss import BossVisitSpectrum
from astra.pipelines.rocket import rocket

spectra = BossVisitSpectrum.select().limit(10)
for result in rocket(spectra):
    print(result.teff, result.logg)
```

### From the CLI

```bash
# Run locally
astra run rocket BossVisitSpectrum --limit 100

# Submit to Slurm
astra srun rocket BossVisitSpectrum --nodes 4 --time="24:00:00"
```

The CLI resolves the task name (`rocket`) to the pipeline function and the input model name (`BossVisitSpectrum`) to the ORM class. It then calls `generate_queries_for_task` to find unprocessed spectra and passes them to the task.

## Real-world example: Corv

The `corv` pipeline in `src/astra/pipelines/corv/__init__.py` is a good compact example. It:

1. Accepts `BossVisitSpectrum` and returns `Corv` results.
2. Checks pre-conditions (is the source a white dwarf? does it have a DA classification from SnowWhite?) and yields flagged results for spectra that do not pass.
3. Submits fitting work to a `ProcessPoolExecutor` for parallelism.
4. Yields `Corv(...)` instances (not using `from_spectrum` in the worker, since database connections cannot cross process boundaries -- instead it constructs the object directly with `source_pk` and `spectrum_pk`).

The corresponding model is in `src/astra/models/corv.py` and defines output fields for radial velocity, stellar parameters, initial values, and quality flags.
