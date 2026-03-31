from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator, BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.models import DagRun
from airflow.utils.session import create_session
from airflow.exceptions import AirflowSkipException

REPO_BRANCH = "dev"
RUN2D = "v6_2_1"
APRED = "1.5"

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
    "IPL-4",
    start_date=datetime(2024, 11, 14), # datetime(2014, 7, 18),
    schedule="0 12 * * *", # 8 am ET
    max_active_runs=1,
    dagrun_timeout=timedelta(days=7),
    catchup=False,
) as dag:


    init = BashOperator(task_id="init", bash_command="astra init")
    #migrate = BashOperator(task_id="migrate", bash_command=f"astra migrate --run2d {RUN2D} --apred {APRED}")
    migrate = BashOperator(task_id="migrate", bash_command=f"astra migrate")

    begin = EmptyOperator(task_id="begin", trigger_rule="all_done")

    with TaskGroup(group_id="SummarySpectrumProducts") as summary_spectrum_products:
        BashOperator(task_id="mwmTargets", bash_command='astra create mwmTargets --overwrite')
        BashOperator(task_id="mwmAllVisit", bash_command='astra create mwmAllVisit --overwrite')
        BashOperator(task_id="mwmAllStar", bash_command='astra create mwmAllStar --overwrite')


    with TaskGroup(group_id="SpectrumProducts") as spectrum_products:
        (
            BashOperator(
                task_id="mwmVisit_mwmStar",
                bash_command='astra srun astra.products.mwm.create_mwmVisit_and_mwmStar_products --nodes 4 --procs 8 --mem 0 --time="48:00:00"'
            )
        )


    with TaskGroup(group_id="BOSSNet") as bossnet:
        bossnet_star = (
            BashOperator(
                task_id="star",
                bash_command='astra srun bossnet BossCombinedSpectrum --mem=16000 --gres="gpu:v100" --account="notchpeak-gpu" --time="48:00:00"'
            )
        )
        bossnet_star >> (
            BashOperator(
                task_id="create_all_star_product",
                bash_command="astra create astraAllStarBOSSNet --overwrite"
            )
        )
        (
            BashOperator(
                task_id="visit",
                bash_command='astra srun bossnet boss.BossVisitSpectrum --nodes 1 --procs 64'
            )
        ) >> (
            BashOperator(
                task_id="create_all_visit_product",
                bash_command="astra create astraAllVisitBOSSNet --overwrite"
            )
        )

    with TaskGroup(group_id="LineForest") as lineforest:
        lineforest_star = (
            BashOperator(
                task_id="star",
                bash_command='astra srun line_forest BossCombinedSpectrum --nodes 1 --procs 64'
            )

        )
         #   --nodes 1 --mem 0 --time="48:00:00"

        lineforest_star >> (
            BashOperator(
                task_id="create_all_star_product",
                bash_command="astra create astraAllStarLineForest --overwrite"
            )
        )
        (
            BashOperator(
                task_id="visit",
                bash_command='astra srun line_forest boss.BossVisitSpectrum --nodes 1 --procs 64'
            )
        ) >> (
            BashOperator(
                task_id="create_all_visit_product",
                bash_command="astra create astraAllVisitLineForest --overwrite"
            )
        )

    with TaskGroup(group_id="Slam") as slam:
        slam_filter = (
            BashOperator(
                task_id="slam_filter",
                bash_command='astra run astra.pipelines.slam.slam_filter BossCombinedSpectrum'
            )
        )
        slam_star = (
            BashOperator(
                task_id="star",
                bash_command='astra srun slam BossCombinedSpectrum --nodes 1 --limit 250000 --mem 0 --time="96:00:00"'
            )
        )
        slam_filter >> slam_star >> (
            BashOperator(
                task_id="create_all_star_product",
                bash_command="astra create astraAllStarSlam --overwrite"
            )
        )

    with TaskGroup(group_id="MDwarfType") as mdwarftype:
        mdwarftype_star = (
            BashOperator(
                task_id="star",
                bash_command='astra srun mdwarftype BossCombinedSpectrum --nodes 1 --procs 64 --mem 0 --time="48:00:00"'
            )
        )
        mdwarftype_star >> (
            BashOperator(
                task_id="create_all_star_product",
                bash_command="astra create astraAllStarMDwarfType --overwrite"
            )
        )
        (
            BashOperator(
                task_id="visit",
                bash_command='astra srun mdwarftype boss.BossVisitSpectrum --nodes 1 --procs 64 --mem 0 --time="48:00:00"'
            )
        ) >> (
            BashOperator(
                task_id="create_all_visit_product",
                bash_command="astra create astraAllVisitMDwarfType --overwrite"
            )
        )

    with TaskGroup(group_id="SnowWhite") as snowwhite:
        snow_white_star = (
            BashOperator(
                task_id="star_filter",
                bash_command='astra run astra.pipelines.snow_white.snow_white_filter BossCombinedSpectrum'
            )
        )
        snow_white_star >> (
            BashOperator(
                task_id="star",
                bash_command='astra srun snow_white BossCombinedSpectrum --nodes 1 --mem 0 --time="48:00:00"'
            )
        ) >> (
            BashOperator(
                task_id="create_all_star_product",
                bash_command="astra create astraAllStarSnowWhite --overwrite"
            )
        )

    with TaskGroup(group_id="CORV") as corv:
        (
            BashOperator(
                task_id="visit",
                bash_command="astra srun corv --nodes 1 --time='48:00:00' --mem 0"
            )
        ) >> (
            BashOperator(
                task_id="create_all_visit_product",
                bash_command="astra create astraAllVisitCorv --overwrite"
            )
        )



    with TaskGroup(group_id="ApogeeNet") as apogeenet:
        apogeenet_star = (
            BashOperator(
                task_id="star",
                bash_command='astra srun apogeenet apogee.ApogeeCoaddedSpectrumInApStar --mem=16000 --gres="gpu:v100" --account="notchpeak-gpu" --time="48:00:00"'
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
                bash_command='astra srun apogeenet apogee.ApogeeVisitSpectrumInApStar --mem=16000 --gres="gpu:v100" --account="notchpeak-gpu" --time="48:00:00"'
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
                bash_command='astra srun aspcap --limit 250000 --nodes 4 --time="48:00:00" --qos=sdss-np --partition=sdss-np --account=sdss-np'
            )
        ) >> (
            BashOperator(
                task_id="create_all_star_product",
                bash_command="astra create astraAllStarASPCAP --overwrite"
            )
        )


    with TaskGroup(group_id="ThePayne") as the_payne:
        the_payne_star = (
            BashOperator(
                task_id="star",
                bash_command='astra srun the_payne apogee.ApogeeCoaddedSpectrumInApStar --procs 8 --nodes 1 --mem 0 --time="48:00:00"'
            )
        )
        the_payne_star >> (
            BashOperator(
                task_id="create_all_star_product",
                bash_command="astra create astraAllStarThePayne --overwrite"
            )
        )

    with TaskGroup(group_id="AstroNN") as astronn:
        astronn_star = BashOperator(
            task_id="star",
            bash_command='astra srun astronn --limit 500000 apogee.ApogeeCoaddedSpectrumInApStar --mem=16000 --gres="gpu:v100" --account="notchpeak-gpu" --time="48:00:00"'
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
                bash_command='astra srun astronn --limit 500000 apogee.ApogeeVisitSpectrumInApStar --mem=16000 --gres="gpu:v100" --account="notchpeak-gpu" --time="48:00:00"'
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
                bash_command='astra srun astronn_dist --nodes 1 --time="48:00:00"' # --mem=16000 --gres="gpu:v100" --account="notchpeak-gpu" --time="48:00:00"'
            )
        ) >> (
            BashOperator(
                task_id="create_all_star_product",
                bash_command="astra create astraAllStarAstroNNdist --overwrite"
            )
        )

    astronn_star >> astronn_dist

    apogeenet_star >> aspcap

    #repo >> task_migrate

    #task_migrate >> (summary_spectrum_products, ) #spectrum_products)
    #task_migrate >> apogeenet
    #(task_migrate, apogeenet_star) >> aspcap
    #apogeenet_star >> aspcap
    star_tasks = (lineforest_star, slam_star, bossnet_star, mdwarftype_star, snow_white_star)

    BranchPythonOperator(
        task_id='check_first_run',
        python_callable=is_first_run,
    ) >> (begin, init)

    init >> migrate >> begin
    begin >> (
        spectrum_products,
        summary_spectrum_products,
        bossnet,
        slam,
        lineforest,
        mdwarftype,
        snowwhite,
        corv,
        apogeenet,
        aspcap,
        astronn,
        astronn_dist,
        the_payne
    )
    snowwhite >> corv
    summary_spectrum_products >> star_tasks
