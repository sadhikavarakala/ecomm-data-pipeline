from airflow.decorators import dag, task
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.bash import BashOperator
from pendulum import datetime
import uuid

# Disable Jinja template rendering on BigQueryInsertJobOperator
class NoTemplateBQInsertJobOperator(BigQueryInsertJobOperator):
    template_fields: list[str] = []  # No templating on any field

@dag(
    dag_id="gcs_to_bq_dataproc_ecommerce",
    description="Load JSON to BigQuery, run Dataproc PySpark job, and append to enriched table.",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["astronomer", "gcs", "bigquery", "dataproc", "ecommerce"],
)
def gcs_to_bq_dataproc_ecommerce():
    project_id = "silken-forest-466023-a2"
    dataset = "retail_data"
    bucket = "astro-airflow-sadhika"
    temp_bucket = "sadhika-temp-bucket"

    # Load products.json
    load_products = NoTemplateBQInsertJobOperator(
        task_id="load_products",
        gcp_conn_id="gcp_conn",
        configuration={
            "load": {
                "destinationTable": {
                    "projectId": project_id,
                    "datasetId": dataset,
                    "tableId": "products"
                },
                "sourceUris": [f"gs://{bucket}/datasets/products/products.json"],
                "sourceFormat": "NEWLINE_DELIMITED_JSON",
                "writeDisposition": "WRITE_TRUNCATE",
                "autodetect": True
            }
        },
        location="us-central1",
    )

    # Load orders.json
    load_orders = NoTemplateBQInsertJobOperator(
        task_id="load_orders",
        gcp_conn_id="gcp_conn",
        configuration={
            "load": {
                "destinationTable": {
                    "projectId": project_id,
                    "datasetId": dataset,
                    "tableId": "orders"
                },
                "sourceUris": [f"gs://{bucket}/datasets/orders/orders.json"],
                "sourceFormat": "NEWLINE_DELIMITED_JSON",
                "writeDisposition": "WRITE_TRUNCATE",
                "autodetect": True
            }
        },
        location="us-central1",
    )

    # Dataproc PySpark Batch Job
    batch_id = f"ecom-transform-{str(uuid.uuid4())[:8]}"
    run_dataproc = DataprocCreateBatchOperator(
        task_id="run_dataproc_transform_join",
        gcp_conn_id="gcp_conn",
        project_id=project_id,
        region="us-central1",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": f"gs://{bucket}/scripts/transform_join_ecommerce.py",
                "args": [
                    "--project", project_id,
                    "--dataset", dataset,
                    "--temp_bucket", f"gs://{temp_bucket}"
                ],
                "jar_file_uris": []
            },
            "runtime_config": {
                "version": "2.2"
            },
            "environment_config": {
                "execution_config": {
                    "service_account": "pyspark-gcs-service-acc@silken-forest-466023-a2.iam.gserviceaccount.com",
                    "network_uri": f"projects/{project_id}/global/networks/default",
                    "subnetwork_uri": f"projects/{project_id}/regions/us-central1/subnetworks/default",
                }
            }
        },
        batch_id=batch_id,
    )

    # Final Dummy Task
    dummy_message = BashOperator(
        task_id='Dummy_Message_OP',
        bash_command='echo "Job Done !!!"',
    )

    # Set task dependencies
    [load_products, load_orders] >> run_dataproc >> dummy_message

dag = gcs_to_bq_dataproc_ecommerce()

