"""Flight Price pipeline: ingest -> validate -> transform -> load.

transform and load are separate tasks, not one. transform computes and
saves its output to disk (see compute_kpis.save_transform_result); load
reads that file back rather than recomputing or receiving it via XCom
(XCom is for small metadata, not the ~57k rows this actually is). Shared
disk between tasks is fine here since Airflow runs under LocalExecutor -
both tasks execute on the same machine.

batch_id is generated once, by ingest, and passed to every task after it
via XCom - so validate/transform/load always act on the exact batch this
run just produced, not just "whatever the latest batch happens to be".
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


def _transform(**context):
    from src.transform.compute_kpis import save_transform_result, transform

    batch_id = context["ti"].xcom_pull(task_ids="ingest", key="batch_id")
    result = transform(batch_id)
    path = save_transform_result(result)
    print(f"batch {batch_id}: transformed {len(result['rows']):,} rows, saved to {path}")


def _load(**context):
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
    transform_task = PythonOperator(task_id="transform", python_callable=_transform)
    load_task = PythonOperator(task_id="load", python_callable=_load)

    ingest_task >> validate_task >> transform_task >> load_task
