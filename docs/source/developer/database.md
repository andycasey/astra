# Database

Astra uses [Peewee](http://docs.peewee-orm.com/) as its ORM. All models inherit from `BaseModel` (defined in `src/astra/models/base.py`), which binds them to a shared database connection.

## Connection setup

The database connection is established at import time in `src/astra/models/base.py` by the `get_database_and_schema` function. The resolution order is:

1. **`ASTRA_DATABASE_PATH` environment variable** -- If set, uses a SQLite database at that path.
2. **`TESTING` flag in config** -- If `True`, uses an in-memory SQLite database (`:memory:`).
3. **Config file** -- Reads `database` from the Astra config file (loaded by `sdsstools.get_config`). Supports both PostgreSQL (`dbname` key) and SQLite (`path` key).
4. **Fallback** -- If nothing is configured, defaults to an in-memory SQLite database with a warning.

Astra looks for configuration files in the standard `sdsstools` locations:

- `~/.config/sdss/astra/astra.yml` (user override)
- `src/astra/etc/astra.yml` (package default)

### PostgreSQL in production

In production (at Utah/CHPC), Astra uses PostgreSQL with a schema (e.g., `astra_043`). The `BaseModel.Meta.schema` attribute is set from the config. Astra wraps the PostgreSQL driver with retry logic (`ResilientDatabase`) to handle transient errors like deadlocks and connection drops.

### SQLite for development

For local development and testing, SQLite is simpler. WAL journaling mode, a 64 MB cache, and `synchronous=0` are set by default for performance.

## Core tables

### `Source`

Defined in `src/astra/models/source.py`. One row per astronomical source. Key columns:

- `pk` -- Auto-incrementing primary key.
- `sdss_id` -- The SDSS-V identifier (unique).
- `gaia_dr3_source_id`, `tic_v8_id`, etc. -- Cross-match identifiers.
- Astrometric, photometric, and targeting columns.

### `Spectrum`

Defined in `src/astra/models/spectrum.py`. A base table for all spectrum types. The `SpectrumMixin` adds computed properties like `.e_flux` and `.plot()`.

Concrete spectrum types (`ApogeeCoaddedSpectrumInApStar`, `BossVisitSpectrum`, `BossCombinedSpectrum`, etc.) extend `Spectrum` and add instrument-specific columns and pixel-data accessors.

### `PipelineOutputMixin`

Defined in `src/astra/models/pipeline.py`. The base class for every pipeline result table. Provides:

| Column | Type | Purpose |
|---|---|---|
| `task_pk` | AutoField | Primary key, auto-assigned on insert. |
| `source_pk` | ForeignKey(Source) | Link to the source. |
| `spectrum_pk` | ForeignKey(Spectrum) | Link to the input spectrum. |
| `v_astra` | Integer | Astra version encoded as an integer (e.g., `0.8.1` becomes `8001`). |
| `created` | DateTime | When the row was first created. |
| `modified` | DateTime | When the row was last updated. |
| `t_elapsed` | Float | Seconds spent on this spectrum (analysis time). |
| `t_overhead` | Float | Seconds of overhead (model loading, I/O). |
| `tag` | Text | Free-form label for organising runs. |

A generated column `v_astra_major_minor` (= `v_astra / 1000`) is used in the unique constraint `(spectrum_pk, v_astra_major_minor)`. This means only one result per spectrum per major.minor version is kept; re-running with the same version updates the existing row.

## Upsert behaviour

The `bulk_insert_or_replace_pipeline_results` function in `src/astra/__init__.py` performs batched upserts using PostgreSQL's `ON CONFLICT ... DO UPDATE` (or SQLite equivalent). When a conflict occurs on `(spectrum_pk, v_astra_major_minor)`, all fields except `task_pk` and `created` are overwritten. This means:

- The first run for a given version inserts new rows.
- Subsequent runs update existing rows in place.
- The original `created` timestamp is preserved; `modified` is updated.

## Custom fields

Astra defines several custom Peewee field types in `src/astra/fields.py`:

- **`BitField`** -- Stores multiple boolean flags in a single integer column. Individual flags are accessed as properties (e.g., `result.flag_low_snr`). Each flag is defined with `result_flags.flag(2**N, help_text="...")`.
- **`PixelArray`** -- A virtual field (not stored in the database) that lazily loads per-pixel arrays (flux, ivar, wavelength) from FITS files. Uses accessor classes to control how data is loaded.
- **`LogLambdaArrayAccessor`** -- Generates a wavelength array from `crval`, `cdelt`, and `naxis` parameters instead of reading from disk.

## Creating tables

In tests or local development, you can create tables manually:

```python
from astra.models.source import Source
from astra.models.spectrum import Spectrum
from astra.models.rocket import Rocket

for model in (Source, Spectrum, Rocket):
    model.create_table()
```

In production, table creation is managed by database migrations.

## Querying results

Peewee's query API applies directly:

```python
from astra.models.corv import Corv

# Get all results with teff > 10000
hot = Corv.select().where(Corv.teff > 10000)

# Join with Source to filter by sdss_id
from astra.models.source import Source
result = (
    Corv
    .select()
    .join(Source, on=(Corv.source_pk == Source.pk))
    .where(Source.sdss_id == 12345678)
    .first()
)
```
