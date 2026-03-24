---
hide-toc: true
---

# Developer Guide

This guide is for developers who want to contribute to Astra, add a new analysis pipeline, or understand how the codebase works. It assumes familiarity with Python.

## What is Astra?

Astra is the analysis framework for the SDSS-V Milky Way Mapper. It manages spectroscopic analysis pipelines, stores results in a database, and produces data products (FITS files) for data releases. Each pipeline analyzes spectra (APOGEE, BOSS, or combined MWM products) and writes per-spectrum results to a shared database. Apache Airflow orchestrates the pipelines in production.

## Setting up a development environment

Astra uses [uv](https://docs.astral.sh/uv/) for dependency management. To get started:

```bash
# Clone the repository
git clone https://github.com/sdss/astra.git
cd astra

# Create a virtual environment and install in editable mode with dev dependencies
uv venv
source .venv/bin/activate
uv pip install -e ".[dev]"
```

### Database configuration

Astra needs a database connection. For local development, the simplest option is to use a SQLite database by setting an environment variable:

```bash
export ASTRA_DATABASE_PATH="/path/to/astra.db"
```

Alternatively, create a configuration file at `~/.config/sdss/astra/astra.yml`:

```yaml
# SQLite (simplest for local development)
database:
  path: /path/to/astra.db

# PostgreSQL (used in production)
# database:
#   dbname: astra
#   user: username
#   host: localhost
#   port: 5432
#   schema: astra
```

For tests, the database is automatically set to an in-memory SQLite instance (`:memory:`), so no configuration is needed to run the test suite.

### Running the CLI

Astra provides a `typer`-based CLI. After installation:

```bash
astra --help
astra config show
```

## Repository layout

```
astra/
  src/astra/           # Main source code
    models/            # Peewee ORM models (database schema)
    pipelines/         # Analysis pipeline code
    products/          # FITS data product generation
    specutils/         # Spectral utilities (continuum, resampling, LSF)
    cli/               # Command-line interface (typer)
    migrations/        # Database migration scripts
    operators/         # Airflow operators
    etc/               # Default configuration files
    fields.py          # Custom Peewee field types (BitField, ArrayField, PixelArray)
    glossary.py        # Standardised field descriptions for data models
    utils/             # General utilities, logging, Slurm helpers
  tests/               # Test suite
  dags/                # Airflow DAG definitions
  docs/                # Sphinx documentation
  pyproject.toml       # Project metadata and dependencies
```

See [Project Structure](structure) for a deeper look at each directory.

### Contents

```{toctree}
structure
writing-a-pipeline
database
tests
dags
```
