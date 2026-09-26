from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator, BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.models import DagRun
from airflow.utils.session import create_session
from airflow.exceptions import AirflowSkipException

REPO_BRANCH = "main"
APRED = "1.6"

# Cap on sdss-np nodes used by this DAG (across all active runs). Create the pool first:
#   airflow pools set sdss_np 10 "sdss-np nodes (1 slot = 1 node)"
# Each sdss-np task uses pool_slots equal to its --nodes.
SDSS_NP_POOL = "sdss_np"
# 8/2 split: ASPCAP gets 8 nodes (~12,500 spectra per node, ~47 h per batch); mwm and
# astraStar take turns on the other 2. Every other task in a run must finish within one
# ASPCAP batch, or the next run isn't created in time and ASPCAP sits idle.
MWM_NODES = 2
ASPCAP_NODES = 8
ASTRA_STAR_NODES = 2
ASTRONN_DIST_NODES = 1
ASPCAP_LIMIT = 12_500 * ASPCAP_NODES
MWM_LIMIT = 2_500_000  # ~100k/h on 2 nodes with BLAS limited -> ~25 h
ASTRONN_STAR_LIMIT = 700_000

def skippy(*args, **kwargs):
    raise AirflowSkipException()


def is_first_run(**context):
    dag_id = context['dag'].dag_id
    with create_session() as session:
        past_runs = session.query(DagRun).filter(
            DagRun.dag_id == dag_id,
            DagRun.run_id != context['run_id'],
            #DagRun.state == 'success'
        ).count()
    return "init" if past_runs == 0 else "begin"


with DAG(
    "DR21",
    start_date=datetime(2026, 9, 9), # datetime(2014, 7, 18),
    schedule="0 12 * * *", # 8 am ET
    # Two runs can overlap, but no task runs in more than one run at a time, so the
    # incremental pipelines never process the same spectra twice or clobber the same file.
    max_active_runs=2,
    dagrun_timeout=timedelta(days=7),
    catchup=False,
    default_args={"max_active_tis_per_dag": 1},
) as dag:


    # init = BashOperator(task_id="init", bash_command="astra init")
    # #migrate = BashOperator(task_id="migrate", bash_command=f"astra migrate --run2d {RUN2D} --apred {APRED}")
    # migrate = BashOperator(task_id="migrate", bash_command=f"astra migrate --apred {APRED} --no-extinction")
    # migratedr17 = BashOperator(task_id="migratedr17", bash_command=f"astra migrate --apred dr17")

    begin = EmptyOperator(task_id="begin", trigger_rule="all_done")

    with TaskGroup(group_id="SummarySpectrumProducts") as summary_spectrum_products:
        BashOperator(task_id="mwmTargets", bash_command='astra create mwmTargets --overwrite')
        BashOperator(task_id="mwmAllVisit", bash_command='astra create mwmAllVisit --overwrite')
        BashOperator(task_id="mwmAllStar", bash_command='astra create mwmAllStar --overwrite')


    with TaskGroup(group_id="SpectrumProducts") as spectrum_products:
        (
            BashOperator(
                task_id="mwmVisit_mwmStar",
                bash_command=f'OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 astra srun astra.products.mwm.create_mwmVisit_and_mwmStar_products --apreds {APRED} --apreds dr17 --limit {MWM_LIMIT} --nodes {MWM_NODES} --procs 16 --mem 0 --time="60:00:00"',
                pool=SDSS_NP_POOL,
                pool_slots=MWM_NODES,
            )
        )


    with TaskGroup(group_id="ApogeeNet") as apogeenet:
        apogeenet_star = (
            BashOperator(
                task_id="star",
                bash_command='astra srun apogeenet apogee.ApogeeCoaddedSpectrumInApStar --mem=0 --gres="gpu:v100" --account="notchpeak-gpu" --time="48:00:00"'
            )
        )
        apogeenet_star >> (
            BashOperator(
                task_id="create_all_star_product",
                bash_command="astra create astraAllStarAPOGEENet --overwrite"
            )
        )
        (
            BashOperator(
                task_id="visit",
                bash_command='astra srun apogeenet apogee.ApogeeVisitSpectrumInApStar --mem=0 --gres="gpu:v100" --account="notchpeak-gpu" --time="48:00:00"'
            )
        ) >> (
            BashOperator(
                task_id="create_all_visit_product",
                bash_command="astra create astraAllVisitAPOGEENet --overwrite"
            )
        )

    with TaskGroup(group_id="ASPCAP") as aspcap:
        (
            BashOperator(
                task_id="aspcap",
                # We should be able to do ~20,000 spectra per node per day.
                # To be safe while testing, let's do 4 nodes with 40,000 spectra (should be approx 12 hrs wall time)
                #bash_command='astra srun aspcap --limit 10000 --nodes 8 --time="48:00:00"'
                #bash_command='astra srun aspcap --limit 125000 --nodes 10 --time="48:00:00"'
                bash_command=f'astra srun aspcap --limit {ASPCAP_LIMIT} --nodes {ASPCAP_NODES} --time="60:00:00" --qos=sdss-np --partition=sdss-np --account=sdss-np',
                pool=SDSS_NP_POOL,
                pool_slots=ASPCAP_NODES,
                # Smaller sdss-np tasks can otherwise keep filling freed slots and starve aspcap.
                priority_weight=10,
            )
        ) >> (
            BashOperator(
                task_id="create_all_star_product",
                bash_command="astra create astraAllStarASPCAP --overwrite"
            )
        ) >> (
            BashOperator(
                task_id="create_astra_star_product",
                bash_command=f'OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 astra srun astra.products.pipeline.create_astraStar_and_astraVisit_products --pipeline ASPCAP --nodes {ASTRA_STAR_NODES} --procs 16 --mem 0 --time="48:00:00"',
                pool=SDSS_NP_POOL,
                pool_slots=ASTRA_STAR_NODES,
            )
        )

    # No longer doing the payne???
    # with TaskGroup(group_id="ThePayne") as the_payne:
    #     the_payne_star = (
    #         BashOperator(
    #             task_id="star",
    #             bash_command='astra srun the_payne apogee.ApogeeCoaddedSpectrumInApStar --procs 8 --nodes 1 --mem 0 --time="48:00:00"'
    #         )
    #     )
    #     the_payne_star >> (
    #         BashOperator(
    #             task_id="create_all_star_product",
    #             bash_command="astra create astraAllStarThePayne --overwrite"
    #         )
    #     )

    with TaskGroup(group_id="AstroNN") as astronn:
        astronn_star = BashOperator(
            task_id="star",
            bash_command=f'astra srun astronn --limit {ASTRONN_STAR_LIMIT} apogee.ApogeeCoaddedSpectrumInApStar --mem=0 --gres="gpu:v100" --account="notchpeak-gpu" --time="48:00:00"'
        )
        astronn_star >> (
            BashOperator(
                task_id="create_all_star_product",
                bash_command="astra create astraAllStarAstroNN --overwrite"
            )
        )
        (
            BashOperator(
                task_id="visit",
                bash_command=f'astra srun astronn --limit {ASTRONN_STAR_LIMIT} apogee.ApogeeVisitSpectrumInApStar --mem=0 --gres="gpu:v100" --account="notchpeak-gpu" --time="48:00:00"'
            )
        ) >> (
            BashOperator(
                task_id="create_all_visit_product",
                bash_command="astra create astraAllVisitAstroNN --overwrite"
            )
        )

    #summary_spectrum_products >>

    with TaskGroup(group_id="AstroNN_Dist") as astronn_dist:
        (
            BashOperator(
                task_id="astronn_dist",
                # astronn_dist does not use GPUS
                bash_command=f'astra srun astronn_dist --nodes {ASTRONN_DIST_NODES} --time="48:00:00"', # --mem=16000 --gres="gpu:v100" --account="notchpeak-gpu" --time="48:00:00"'
                pool=SDSS_NP_POOL,
                pool_slots=ASTRONN_DIST_NODES,
            )
        ) >> (
            BashOperator(
                task_id="create_all_star_product",
                bash_command="astra create astraAllStarAstroNNDist --overwrite"
            )
        )

    # astronn_star >> astronn_dist

    apogeenet_star >> aspcap

    # spectrum_products >> aspcap

    # BranchPythonOperator(
    #     task_id='check_first_run',
    #     python_callable=is_first_run,
    # ) >> (begin, init)

    # init >> migrate >> migratedr17 >> begin
    begin >> (
        spectrum_products,
        summary_spectrum_products,
        apogeenet,
        aspcap,
        astronn,
        astronn_dist,
    )
