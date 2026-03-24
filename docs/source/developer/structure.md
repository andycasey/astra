# Project Structure

This page describes the layout of the `src/astra/` source tree and the role of each major directory.

## `models/` -- Database schema

Each file in `models/` defines one or more [Peewee](http://docs.peewee-orm.com/) ORM classes that map to database tables. The key base classes are:

| File | Purpose |
|---|---|
| `base.py` | `BaseModel` -- the root model class that binds every table to the shared database connection. Also contains FITS-serialisation helpers. |
| `source.py` | `Source` -- an astronomical source (one row per `sdss_id`). |
| `spectrum.py` | `Spectrum` and `SpectrumMixin` -- the base spectrum table and a mixin that adds `.flux`, `.ivar`, `.wavelength`, `.e_flux`, and `.plot()`. |
| `pipeline.py` | `PipelineOutputMixin` -- the base class for all pipeline result tables. Adds `task_pk`, `source_pk`, `spectrum_pk`, `v_astra`, timing fields, and `from_spectrum()`. |
| `apogee.py` | APOGEE spectrum types (`ApogeeCoaddedSpectrumInApStar`, `ApogeeVisitSpectrum`, etc.). |
| `boss.py` | BOSS spectrum types (`BossVisitSpectrum`). |
| `mwm.py` | Combined MWM spectrum types (`BossCombinedSpectrum`, `ApogeeCombinedSpectrum`). |

Pipeline-specific result models live alongside these (e.g., `corv.py`, `snow_white.py`, `aspcap.py`). Each one extends `PipelineOutputMixin` and declares the output columns for that pipeline.

## `pipelines/` -- Analysis code

Each sub-directory under `pipelines/` contains the code for one analysis pipeline:

```
pipelines/
  apogeenet/       # Neural-network stellar parameters for APOGEE spectra
  aspcap/          # APOGEE Stellar Parameters and Chemical Abundances Pipeline
  astronn/         # AstroNN stellar parameters and abundances
  astronn_dist/    # AstroNN distance estimates
  best/            # Best-estimate parameter compilation
  bossnet/         # Neural-network stellar parameters for BOSS spectra
  clam/            # Classification and labelling
  corv/            # White-dwarf radial velocities
  ferre/           # FERRE spectral fitting
  line_forest/     # Emission/absorption line measurements
  mdwarftype/      # M-dwarf spectral typing
  nmf_rectify/     # NMF continuum rectification
  slam/            # SLAM stellar parameters for BOSS spectra
  snow_white/      # White-dwarf classification and parameters
  the_cannon/      # The Cannon data-driven model
  the_payne/       # The Payne spectral model fitting
```

Each pipeline directory typically contains:

- `__init__.py` -- the main entry point decorated with `@task` (see [Writing a Pipeline](writing-a-pipeline)).
- Supporting modules for fitting, utilities, model loading, etc.

## `products/` -- Data product generation

Code that generates FITS files for SDSS data releases:

| File | Purpose |
|---|---|
| `mwm.py` | Generates per-source `mwmVisit` and `mwmStar` FITS files. |
| `pipeline.py` / `pipeline_summary.py` | Generates summary tables (e.g., `astraAllStarASPCAP`). |
| `apogee.py` / `boss.py` | Instrument-specific product helpers. |
| `mwm_summary.py` | `mwmAllVisit`, `mwmAllStar`, `mwmTargets` summary products. |

## `spectrum/` -- Spectral synthesis framework

A framework for model atmosphere interpolation and spectral synthesis. Not currently used by any active pipeline, but available for future work:

- `photospheres/` -- Model atmosphere loading and interpolation (Kurucz, MARCS, ATLAS).
- `synthesis/` -- Wrappers for spectral synthesis codes (MOOG, SME, Turbospectrum, Korg).
- `transitions/` -- Atomic and molecular line list handling (VALD, GES formats).
- `resampling.py` -- Wavelength grid resampling utilities.

## `specutils/` -- Spectral utilities

Reusable routines for spectrum manipulation:

- `continuum/` -- Continuum normalisation methods.
- `resampling.py` -- Wavelength grid resampling.
- `lsf.py` -- Line-spread-function convolution.
- `frizzle.py` -- Spectral frizzling utilities.

## `cli/` -- Command-line interface

The CLI is built with [Typer](https://typer.tiangolo.com/). The main entry point is `astra.py`, which defines the `astra` command. Key sub-commands:

- `astra run <task> [input_model]` -- Run a pipeline task.
- `astra srun <task> [input_model]` -- Submit a pipeline task to Slurm.
- `astra create <product>` -- Generate a data product.
- `astra migrate` -- Ingest new spectra from the SDSS reduction pipelines.
- `astra config show|get|set` -- Manage configuration.

There is also a `casload` CLI for bulk-loading catalogues.

## `fields.py` -- Custom Peewee fields

Astra extends Peewee with custom field types:

- `BitField` -- Stores boolean flags as bits in an integer column. Pipelines use this for quality flags (e.g., `result_flags`).
- `PixelArray` / `ArrayField` -- Virtual fields that store per-pixel data (flux, wavelength) outside the database (typically in FITS files) and load them lazily via accessor classes.
- `LogLambdaArrayAccessor` -- Generates a wavelength array from CRVAL/CDELT/NAXIS instead of storing it.

## `migrations/` -- Database migrations

Scripts that ingest reduced data products into Astra's database tables (`Source`, `Spectrum`, `ApogeeVisitSpectrum`, `BossVisitSpectrum`, etc.). These are run via `astra migrate`.

## `utils/` -- Utilities

- `__init__.py` -- Logging, path expansion, version helpers, the `Timer` context manager (used by the `@task` decorator to track elapsed time and overhead), and task/model resolution functions.
- `slurm.py` -- Helpers for submitting Slurm batch jobs via `astra srun`.

## `etc/` -- Default configuration

Contains the default `astra.yml` configuration file that ships with the package. User overrides go in `~/.config/sdss/astra/astra.yml`.
