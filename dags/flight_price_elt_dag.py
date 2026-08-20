"""Flight Price pipeline: ingest -> validate -> transform & load.

Transform and load are one task, not two: transform's output only matters
if it's immediately persisted, and splitting them would mean either
recomputing the transform twice or passing ~57k rows through XCom (XCom is
for small metadata, not bulk data). Neither is worth it.

batch_id is generated once, by ingest, and passed to every task after it
via XCom - so validate/load always act on the exact batch this run just
produced, not just "whatever the latest batch happens to be".
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def _ingest(**context):
    from src.ingestion.ingest_to_mysql import ingest

    result = ingest()
    context["ti"].xcom_push(key="batch_id", value=result["batch_id"])
    print(f"batch {result['batch_id']}: ingested {result['rows_loaded']:,} rows")


def _validate(**context):
    from src.validation.validate_staging import validate

    batch_id = context["ti"].xcom_pull(task_ids="ingest", key="batch_id")
    print(validate(batch_id))


def _transform_and_load(**context):
    from src.loading.load_to_postgres import load

    batch_id = context["ti"].xcom_pull(task_ids="ingest", key="batch_id")
    print(load(batch_id))


with DAG(
    dag_id="flight_price_elt_dag",
    start_date=datetime(2026, 1, 1),
    # Fares change as departure approaches (see Days Before Departure) - a
    # real price tracker needs a fresh snapshot every day, not a one-off run.
    schedule="@daily",
    catchup=False,
    default_args={
        "retries": 1,
        "email": ["ops@flight-price-pipeline.local"],
        "email_on_failure": True,
        "email_on_retry": False,
    },
) as dag:
    ingest_task = PythonOperator(task_id="ingest", python_callable=_ingest)
    validate_task = PythonOperator(task_id="validate", python_callable=_validate)
    transform_and_load_task = PythonOperator(task_id="transform_and_load", python_callable=_transform_and_load)

    ingest_task >> validate_task >> transform_and_load_task
