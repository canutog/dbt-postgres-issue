"""
A basic dbt DAG that shows how to run dbt commands via the BashOperator

Follows the standard dbt seed, run, and test pattern.
"""

from pendulum import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

# We're hardcoding this value here for the purpose of the demo, but in a production environment this
# would probably come from a config file and/or environment variables!
DBT_PROJECT_DIR = "/usr/local/airflow/dbt"


with DAG(
    "microbatch_issue_full_refresh",
    start_date=datetime(2024, 12, 1),
    description="A sample Airflow DAG to invoke dbt runs using a BashOperator",
    schedule_interval=None,
    catchup=False,
    default_args={
        "env": {
            "DBT_USER": "{{ conn.postgres.login }}",
            "DBT_ENV_SECRET_PASSWORD": "{{ conn.postgres.password }}",
            "DBT_HOST": "{{ conn.postgres.host }}",
            "DBT_SCHEMA": "{{ conn.postgres.schema }}",
            "DBT_PORT": "{{ conn.postgres.port }}",
        }
    },
) as dag:
    # This task loads the CSV files from dbt/data into the local postgres database for the purpose of this demo.
    # In practice, we'd usually expect the data to have already been loaded to the database.
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"dbt seed --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_run_stg_encoded_tree = BashOperator(
        task_id="dbt_run_stg_encoded_tree",
        bash_command=f"dbt run --select stg_encoded_tree --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR} --full-refresh --debug",
    )
    dbt_run_tree_routes_no_error = BashOperator(
        task_id="dbt_run_tree_routes_no_error",
        bash_command=f"dbt run --select tree_routes_no_error --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR} --full-refresh --debug",
    )
    dbt_run_tree_routes_with_alias_error = BashOperator(
        task_id="dbt_run_tree_routes_with_alias_error",
        bash_command=f"dbt run --select tree_routes_with_alias_error --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR} --debug --full-refresh",
        retries=0
    )



    dbt_seed >> dbt_run_stg_encoded_tree >> dbt_run_tree_routes_no_error >> dbt_run_tree_routes_with_alias_error
