""""
Notes:
1. Automates refresh of mapping tables of sfdc leads to ga and segment respectively
2. Preliminary version - refresh the SQL scripts available in the SQL folder everyday
3. To-DO: Prodcutionize with right schema and incremental refreshes as opposed to delete and create tables everyday
"""

from datetime import datetime
from pathlib import Path
import sys

from airflow.contrib.operators import bigquery_operator

# This lets us import from workflows folder as Airflow only sees folders / this is not a real Python package.
sys.path.append(str(Path(__file__).parent.parent.parent))

from workflows.airflow_DAG import DAG  # noqa
from workflows import common  # noqa

DAG_ID = Path(__file__).parent.name
default_args = {
    'owner': 'ashetty',
    'start_date': datetime(2022, 1, 31),
    'on_failure_callback': common.slack_notify_for_failure
}

with DAG(DAG_ID, default_args=default_args, schedule_interval='0 8 * * *') as dag:
    sql = {}
    task_id = {}
    operator = {}
    tasks = [
        'lookup_sfdc_lead_to_ga'
        ,'lookup_sfdc_lead_to_segment'
        ,'dim_visitor'
    ]

    for task in tasks:
        sql[task] = '/'.join(['sql', ('.'.join([task, 'sql']))])
        task_id[task] = task
        operator[task] = bigquery_operator.BigQueryOperator(
            task_id=task_id[task],
            sql=sql[task],
            use_legacy_sql=False,
            bigquery_conn_id='bigquery_with_gdrive_scope')

    record = common.record_change_op('dwh_web', tasks)
    common.create_dataset_op('dwh_web') >> operator.values() >> record
