from airflow.decorators import dag
from datetime import datetime
from airflow.operators.bash import BashOperator
from cosmos_config import DBT_CONFIG, DBT_PROJECT_CONFIG, DBT_EXECUTION_CONFIG
from cosmos import DbtTaskGroup
import os


@dag(
    schedule=None,
    start_date=datetime(2025, 3, 1),
    catchup=False,
    tags=["retail", "analytics", "dbt"],
)
def retail_dbt_transform():
    dbt_project_dir = "/usr/local/airflow/dags/dbt/dbt_project"

    profile_config = DBT_CONFIG

    project_config = DBT_PROJECT_CONFIG

    execution_config = DBT_EXECUTION_CONFIG

    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=f"source /usr/local/airflow/dbt_venv/bin/activate && cd /usr/local/airflow/dags/dbt/dbt_project && dbt debug --profiles-dir /usr/local/airflow/dags/dbt/dbt_project -v",
    )

    dbt_run = DbtTaskGroup(
        group_id="dbt_run",
        profile_config=profile_config,
        project_config=project_config,
        execution_config=execution_config,
        default_args={"retries": 2},
        operator_args={
            "env": {
                "PATH": f"/usr/local/airflow/dbt_venv/bin:{os.environ['PATH']}",
                "VIRTUAL_ENV": "/usr/local/airflow/dbt_venv"
            }
        }
    )

    dbt_debug >> dbt_run


retail_dbt_transform_dag = retail_dbt_transform()