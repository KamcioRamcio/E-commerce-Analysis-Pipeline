from cosmos.config import ProfileConfig, ProjectConfig, ExecutionConfig
from pathlib import Path

DBT_EXECUTABLE_PATH = "/usr/local/airflow/dbt_venv/bin/dbt"

DBT_EXECUTION_CONFIG = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH
)

DBT_CONFIG = ProfileConfig(
    profile_name='dbt_project',
    target_name='dev',
    profiles_yml_filepath=Path('/usr/local/airflow/dags/dbt/dbt_project/profiles.yml')
)

DBT_PROJECT_CONFIG = ProjectConfig(
    dbt_project_path=Path('/usr/local/airflow/dags/dbt/dbt_project'),
)
