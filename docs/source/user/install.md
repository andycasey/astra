# Install

## From source

We recommend using [`uv`](https://docs.astral.sh/uv/) to manage your Python environment. Clone the repository and install from source:

```bash
git clone git@github.com:andycasey/astra.git
cd astra
uv venv
source .venv/bin/activate
uv pip install -e .
```

## Setup

You will need to initialize a database for Astra. A PostgreSQL database is recommended. The database connection details need to be stored in a `~/.astra/astra.yml` file in the following format:

```yaml
database:
  schema: <schema_name>
  dbname: <database_name>
  user: <user_name>
  host: <host_name>
```

If you're using a specific schema, you'll need to create that schema first, then run:

```bash
astra init
```

## Migrating data

Astra does not need to be run in the same computing environment where the data are stored. For this reason, it needs to _migrate_ details about available spectra into the Astra database, and link all the auxiliary data (photometry, astrometry) for those sources:

```bash
astra migrate
```

## Building the documentation

To build the Astra documentation locally:

```bash
uv pip install -e ".[docs]"
cd docs/
make html
```

The built documentation pages will be in the `docs/build/html` folder.
