# DAGs

Astra uses [Apache Airflow](https://airflow.apache.org/) to orchestrate pipeline execution in production. The DAG (Directed Acyclic Graph) definitions live in the `dags/` directory at the repository root.

## Overview

Each DAG file defines a workflow that runs on a schedule (typically daily). The DAGs use Airflow's `BashOperator` to invoke `astra` CLI commands, and `TaskGroup` to organise related steps.

| File | Purpose |
|---|---|
| `dags/main.py` | The primary production DAG. Runs all pipelines and generates data products. |
| `dags/dr20.py` | A DAG tailored to the DR20 data release, with a subset of pipelines. |

## Structure of a DAG

A typical DAG follows this pattern:

1. **Initialisation** -- Run `astra init` and `astra migrate` to set up tables and ingest new spectra.
2. **Summary spectrum products** -- Generate `mwmTargets`, `mwmAllVisit`, `mwmAllStar`.
3. **Pipeline execution** -- Each pipeline is a `TaskGroup` containing:
   - A `BashOperator` that runs `astra srun <pipeline> <InputModel>` to submit a Slurm job.
   - A downstream `BashOperator` that runs `astra create <product>` to generate the summary FITS file.
4. **Spectrum products** -- Generate per-source `mwmVisit` and `mwmStar` FITS files (typically after pipelines finish).
5. **Dependencies** -- Airflow `>>` operators define execution order. For example, SnowWhite must complete before Corv runs (since Corv uses SnowWhite classifications).

## Example: DR20 DAG excerpt

```python
with TaskGroup(group_id="SnowWhite") as snowwhite:
    snow_white_star = BashOperator(
        task_id="star_filter",
        bash_command="astra run astra.pipelines.snow_white.snow_white_filter BossCombinedSpectrum",
    )
    snow_white_star >> BashOperator(
        task_id="star",
        bash_command='astra srun snow_white BossCombinedSpectrum --nodes 1 --time="48:00:00"',
    ) >> BashOperator(
        task_id="create_all_star_product",
        bash_command="astra create astraAllStarSnowWhite --overwrite",
    )

with TaskGroup(group_id="CORV") as corv:
    BashOperator(
        task_id="visit",
        bash_command="astra srun corv --nodes 1 --time='48:00:00'",
    ) >> BashOperator(
        task_id="create_all_visit_product",
        bash_command="astra create astraAllVisitCorv --overwrite",
    )

snowwhite >> corv
```

The `astra srun` command submits work to Slurm. It internally calls `generate_queries_for_task` to find unprocessed spectra, partitions them across nodes, and submits batch jobs. Common Slurm flags (`--nodes`, `--time`, `--mem`, `--gres`) are passed through.

## Adding a pipeline to a DAG

To add a new pipeline (e.g., "Rocket") to a DAG:

1. Add a `TaskGroup` with the pipeline's Slurm command and product-generation command.
2. Wire it into the dependency graph with `>>`.
3. If your pipeline depends on outputs from another pipeline, add that dependency explicitly.

```python
with TaskGroup(group_id="Rocket") as rocket:
    BashOperator(
        task_id="star",
        bash_command="astra srun rocket BossVisitSpectrum --nodes 2",
    ) >> BashOperator(
        task_id="create_product",
        bash_command="astra create astraAllStarRocket --overwrite",
    )

begin >> rocket
```
