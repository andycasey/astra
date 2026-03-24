# Command Line Interface

Astra provides a command line tool called `astra` for managing the database, migrating spectra, running analysis pipelines, creating data products, and managing configuration.

## General Usage

```
astra [COMMAND] [OPTIONS] [ARGUMENTS]
```

Run `astra --help` to see all available commands, or `astra COMMAND --help` for help on a specific command.

---

## `astra version`

Print the installed version of Astra.

**Usage:**

```
astra version
```

**Example:**

```
astra version
# Astra version: 0.6.0
```

---

## `astra init`

Initialize the Astra database by creating the schema and all required tables. This must be run before any other operations that interact with the database.

**Usage:**

```
astra init [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--drop-tables` | bool | `False` | Drop all existing tables before re-creating them. |
| `--delay` | int | `10` | Delay in seconds before dropping tables (gives you time to cancel). |

**Examples:**

```bash
# Initialize the database (create tables if they don't exist)
astra init

# Re-initialize by dropping and re-creating all tables
astra init --drop-tables

# Drop and re-create with no delay
astra init --drop-tables --delay 0
```

---

## `astra migrate`

Migrate spectra and auxiliary information (photometry, astrometry, targeting, extinction, etc.) into the Astra database. This command ingests APOGEE and/or BOSS spectra, creates source entries, links spectra to sources, and populates metadata from external catalogs.

Migration tasks are executed in dependency order: spectra are ingested first, then sources are created, then metadata is populated.

**Usage:**

```
astra migrate [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--apred` | str | `None` | APOGEE data reduction pipeline version (e.g., `dr17`, `1.3`). |
| `--run2d` | str | `None` | BOSS data reduction pipeline version (e.g., `v6_1_3`). |
| `--limit` | int | `None` | Limit the number of spectra to migrate. |
| `--max-mjd` | int | `None` | Maximum MJD of spectra to migrate. |
| `--metadata / --no-metadata` | bool | `True` | Migrate metadata (photometry, astrometry, targeting, etc.). |
| `--incremental / --no-incremental` | bool | `True` | Only attempt to migrate new spectra (skip already-ingested ones). |
| `--extinction / --no-extinction` | bool | `True` | Compute extinction values. |

**Examples:**

```bash
# Migrate APOGEE spectra from a specific data reduction version
astra migrate --apred 1.3

# Migrate BOSS spectra
astra migrate --run2d v6_1_3

# Migrate both APOGEE and BOSS spectra
astra migrate --apred 1.3 --run2d v6_1_3

# Migrate with a limit, skipping metadata
astra migrate --apred 1.3 --limit 1000 --no-metadata

# Migrate SDSS4 DR17 APOGEE spectra
astra migrate --apred dr17
```

When metadata migration is enabled, the following catalogs and computations are included:

- Gaia DR3 source IDs, astrometry, and photometry
- Gaia synthetic photometry
- Zhang stellar parameters
- Bailer-Jones distances
- 2MASS, unWISE, and GLIMPSE photometry
- HEALPix values
- TIC v8 identifiers
- SDSS4 APOGEE IDs
- Galactic coordinates
- Targeting cartons
- Visit spectra counts
- W1/W2 magnitudes
- Extinction / reddening

---

## `astra run`

Run an Astra analysis pipeline task on spectra. The task is executed locally on the current machine.

**Usage:**

```
astra run TASK [SPECTRUM_MODEL] [SDSS_IDS...] [OPTIONS]
```

**Arguments:**

| Argument | Description |
|----------|-------------|
| `TASK` | The task name to run. Can be a short name (e.g., `aspcap`) or a fully qualified path (e.g., `astra.pipelines.aspcap.aspcap`). |
| `SPECTRUM_MODEL` | The spectrum model to use (e.g., `ApogeeCombinedSpectrum`, `BossCombinedSpectrum`). If omitted, all spectrum models accepted by the task will be analyzed. |
| `SDSS_IDS` | Optional list of SDSS IDs to restrict processing to specific sources. |

**Options:**

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--limit` | int | `None` | Limit the number of spectra to process. |
| `--page` | int | `None` | Page number for paginated results (`limit` spectra per page). |

**Examples:**

```bash
# Run the ASPCAP pipeline on all eligible spectra
astra run aspcap

# Run on a specific spectrum model
astra run aspcap ApogeeCombinedSpectrum

# Run on specific SDSS IDs
astra run aspcap ApogeeCombinedSpectrum 12345 67890

# Limit the number of spectra processed
astra run aspcap --limit 100

# Run with pagination
astra run aspcap --limit 100 --page 2
```

---

## `astra srun`

Distribute an Astra pipeline task across multiple nodes using Slurm. This command generates Slurm job scripts and submits them with `srun`, displaying live progress for each node.

**Usage:**

```
astra srun TASK [MODEL] [SDSS_IDS...] [OPTIONS]
```

**Arguments:**

| Argument | Description |
|----------|-------------|
| `TASK` | The task name to run (e.g., `aspcap`, or `astra.pipelines.aspcap.aspcap`). |
| `MODEL` | The input model to use (e.g., `ApogeeCombinedSpectrum`). If omitted, the first model accepted by the task that has spectra will be used. |
| `SDSS_IDS` | Optional list of SDSS IDs to restrict processing to. |

**Options:**

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--nodes` | int | `1` | Number of Slurm nodes to use. |
| `--procs` | int | `1` | Number of `astra` processes to use per node. |
| `--limit` | int | `None` | Limit the number of inputs. |
| `--account` | str | `sdss-np` | Slurm account name. |
| `--partition` | str | same as `--account` | Slurm partition. |
| `--qos` | str | `None` | Slurm Quality of Service. |
| `--gres` | str | `None` | Slurm generic resources (e.g., `gpu:1`). |
| `--mem` | str | `0` | Memory per node (e.g., `64G`). |
| `--time` | str | `24:00:00` | Wall-time limit. |
| `--exclusive / --no-exclusive` | bool | `True` | Use exclusive node allocation. |

**Examples:**

```bash
# Run ASPCAP across 4 nodes
astra srun aspcap --nodes 4

# Run with 2 processes per node on a specific model
astra srun aspcap ApogeeCombinedSpectrum --nodes 4 --procs 2

# Run with custom Slurm settings
astra srun aspcap --nodes 8 --account my-account --time 12:00:00 --mem 128G

# Request GPU resources
astra srun aspcap --nodes 2 --gres gpu:1
```

---

## `astra create`

Create Astra summary data products (FITS files). Multiple products can be created in a single invocation.

**Usage:**

```
astra create PRODUCTS... [OPTIONS]
```

**Arguments:**

| Argument | Description |
|----------|-------------|
| `PRODUCTS` | One or more product names to create (see table below). |

**Options:**

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--overwrite` | bool | `False` | Overwrite the product if it already exists. |
| `--limit` | int | `None` | Limit the number of rows per product. |

**Available products:**

| Product | Description |
|---------|-------------|
| `mwmTargets` | MWM targets summary |
| `mwmAllStar` | MWM all-star summary |
| `mwmAllVisit` | MWM all-visit summary |
| `mwmStar` | MWM per-star products |
| `mwmVisit` | MWM per-visit products |
| `mwmVisit/mwmStar` | Both MWM visit and star products |
| `astraAllStarASPCAP` | ASPCAP all-star summary |
| `astraAllStarAPOGEENet` | APOGEENet all-star summary |
| `astraAllVisitAPOGEENet` | APOGEENet all-visit summary |
| `astraAllStarBOSSNet` | BOSSNet all-star summary |
| `astraAllVisitBOSSNet` | BOSSNet all-visit summary |
| `astraAllStarLineForest` | LineForest all-star summary |
| `astraAllVisitLineForest` | LineForest all-visit summary |
| `astraAllStarAstroNN` | AstroNN all-star summary |
| `astraAllVisitAstroNN` | AstroNN all-visit summary |
| `astraAllStarAstroNNDist` | AstroNN-dist all-star summary |
| `astraAllStarSlam` | SLAM all-star summary |
| `astraAllStarMDwarfType` | MDwarfType all-star summary |
| `astraAllVisitMDwarfType` | MDwarfType all-visit summary |
| `astraAllVisitCorv` | Corv all-visit summary |
| `astraAllStarSnowWhite` | SnowWhite all-star summary |

**Examples:**

```bash
# Create the MWM all-star product
astra create mwmAllStar

# Create multiple products
astra create mwmAllStar mwmAllVisit

# Overwrite existing products
astra create mwmAllStar --overwrite

# Create a product with a row limit (useful for testing)
astra create astraAllStarASPCAP --limit 1000
```

---

## `astra config`

Manage Astra configuration settings. User configuration is stored in `~/.config/sdss/astra/astra.yml`.

### `astra config show`

Display all current configuration settings.

```
astra config show
```

### `astra config get`

Get the value of a specific configuration key. Supports dot notation for nested keys.

```
astra config get KEY
```

**Example:**

```bash
astra config get database.host
```

### `astra config set`

Set a configuration value in the user config file. Values are parsed as YAML, so booleans and numbers are handled automatically.

```
astra config set KEY VALUE
```

**Example:**

```bash
astra config set database.host localhost
astra config set database.port 5432
```

### `astra config path`

Show the path to the user configuration file and whether it exists.

```
astra config path
```
