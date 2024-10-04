from airflow.models import Variable, DAG
from datetime import datetime, timedelta
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator, BigQueryInsertJobOperator

dag = DAG(
    dag_id="dag-bigquery",
    start_date=datetime(2024, 10, 4),
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=10)
        },
    catchup=False
)

# Asumiendo que se cargó archivo dag-bigquery.json con las variables en Airflow
var_ambt = Variable.get("dag-bigquery", deserialize_json=True)

load_data = GCSToBigQueryOperator(
    task_id="move_gcs_to_bq",
    bucket=var_ambt["BUCKET"],
    source_objects=var_ambt["SOURCE_OBJECTS"],
    destination_project_dataset_table=var_ambt["DEST_TABLE"],
    source_format=var_ambt["SOURCE_FORMAT"],
    max_bad_records=var_ambt["MAX_BAD_RECS"],
    create_disposition=var_ambt["CREATE_DISP"],
    write_disposition=var_ambt["WRITE_DISP"],
    econding=var_ambt["ENCODING"],
    dag=dag
)

proyecto=var_ambt["PROJECT"]
dataset=var_ambt["DATASET"]
tabla_origen=var_ambt["DEST_TABLE"]
tabla_destino=var_ambt["FINAL_TABLE"]

transform_load = BigQueryInsertJobOperator(
    task_id="transform_load_new_table",
    configuration={
        "query": {
            "query": f"""
            CREATE OR REPLACE TABLE '{proyecto}.{dataset}.{tabla_destino}'
            AS
            --SELECT
            FROM '{proyecto}.{dataset}.{tabla_origen}'
            --APLICAR ALGUNA TRANSFORMACIÓN
            """,
            "useLegacySql": False
        }
    },
    dag=dag
)

load_data >> transform_load
