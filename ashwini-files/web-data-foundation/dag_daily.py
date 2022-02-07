import datetime
from pathlib import Path
import sys

from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dummy_operator import DummyOperator

# This lets us import from workflows folder as Airflow only sees folders / this is not a real Python package.
sys.path.append(str(Path(__file__).parent.parent.parent))

from workflows.airflow_DAG import DAG  # noqa
from workflows import common  # noqa
from workflows import config  # noqa

DAG_ID = Path(__file__).parent.name
default_dag_args = {
    "owner": "ashetty",
    "start_date": datetime.datetime(2021, 6, 22),
    "on_failure_callback": common.slack_notify_for_failure,
}

DATASET = "dwh_web"
DIM_DAILY_TBLS = [
    "dim_sessions_daily"
    ,"fct_page_visit_daily"
]

TBLS_NO_PARTITION = ["dim_visitor"
                 ,"lookup_sfdc_lead_to_ga"
                 ,"lookup_sfdc_lead_to_segment"]

with DAG(DAG_ID, schedule_interval="0 8 * * *", default_args=default_dag_args) as dag:
    create_dwh_web = common.create_dataset_op(DATASET)
    
    dim_daily_tbl_tasks = []
    for d_daily in DIM_DAILY_TBLS:
        dim_daily_tbl_tasks.append(
            BigQueryOperator(
                task_id=d_daily,
                sql=f"sql/{d_daily}.sql",
                destination_dataset_table=f"{config.PROJECT_ID}.{DATASET}.{d_daily}$"
                + "{{ next_ds_nodash }}",
                write_disposition="WRITE_TRUNCATE",
                schema_update_options=("ALLOW_FIELD_ADDITION",),
                time_partitioning={"field": "extract_date", "type": "DAY"},
                use_legacy_sql=False,
            )
        )

    task_group_1 = DummyOperator(task_id="task_group_1")

    for c_tble in TBLS_NO_PARTITION:
        cust_tbl_tasks.append(
            BigQueryOperator(
                task_id=c_tble,
                sql=f"sql/{c_tble}.sql",
                destination_dataset_table=f"{config.PROJECT_ID}.{DATASET}.{c_tble}",
                write_disposition="WRITE_TRUNCATE",
                use_legacy_sql=False,
            )
        )

    (
        create_dwh_web
        >> cust_tbl_tasks
        >> task_group_1
        >> dim_daily_tbl_tasks
        
    )
