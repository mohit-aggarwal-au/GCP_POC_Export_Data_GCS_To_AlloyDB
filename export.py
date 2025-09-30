"""
# AlloyDB Import DAG

This DAG demonstrates an event-driven workflow in which it listens for a
BigQuery job completion event via a Pub/Sub topic. If the job is successful,
it triggers a task to import data from a Google Cloud Storage (GCS)
destination to an AlloyDB table.

This workflow assumes the following prerequisites:
1.  A Pub/Sub topic is configured to receive BigQuery job completion events from Cloud Logging.
2.  A Pub/Sub subscription is created for that topic.
3.  The Airflow service account has the necessary permissions to read from the Pub/Sub subscription and
    to trigger the AlloyDB import API.
"""

from __future__ import annotations

import json
import logging
import base64
from datetime import datetime

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor
from google.api_core.client_options import ClientOptions
from google.cloud import alloydb_v1 as alloydb

# [START import_variables]
# It is highly recommended to use Airflow Variables for these values
# so they can be easily managed in the Airflow UI.
GCP_PROJECT_ID = "611457311679"
GCS_BUCKET = "fcip_test_bucket"
PUBSUB_SUBSCRIPTION = "ExportTopic-sub"
ALLOYDB_REGION = "australia-southeast2"
#ALLOYDB_CLUSTER_ID = "export-to-alloydb"
ALLOYDB_DATABASE_ID = "postgres"
ALLOYDB_CLUSTER_ID = "admin"
ALLOYDB_USER = "postgres"
ALLOYDB_TABLE = "export"
# [END import_variables]


def parse_pubsub_message_and_trigger_import(
    ti, project_id, region, cluster_id, db_id, user, table
):
    """
    Parses the Pub/Sub message to get the BigQuery job status and
    the GCS destination URI. If the job was successful, it invokes
    the AlloyDB import API.
    """
    # XComs are used to pass information between tasks.
    pulled_messages = ti.xcom_pull(task_ids="await_bq_job_completion_event", key="return_value")

    if not pulled_messages:
        logging.info("No messages pulled. The DAG run might have timed out waiting for an event.")
        return
    else:
        logging.info(f"Message pulled, now going for the next step {pulled_messages[0]} {pulled_messages[0]['message']} {pulled_messages[0]['message']['data']}")

    
    base64_data_bytes = pulled_messages[0]['message']['data']

    if not base64_data_bytes:
        logging.error("Pub/Sub message data is empty. Cannot parse JSON.")
        return

    try:
        # Step 1: Decode the Base64 byte string to a UTF-8 byte string.
        decoded_data_bytes = base64.b64decode(base64_data_bytes)
        
        # Step 2: Decode the UTF-8 byte string to a regular Python string.
        decoded_data_str = decoded_data_bytes.decode('utf-8')

        # Step 3: Parse the string as JSON.
        message_data = json.loads(decoded_data_str)
        
        logging.info(f"Successfully parsed Pub/Sub message: {message_data}")
    
    except (base64.binascii.Error, json.JSONDecodeError) as e:
        logging.error(f"Failed to decode or parse message data: {e}")
        logging.error(f"Problematic data (Base64): {base64_data_bytes}")
        raise # Re-raise the error to fail the task for investigation
    
    
    # Extract the job status and destination URI from the Cloud Logging audit log message
    job_status = message_data.get("state", "")
    
    # destination_uri = message_data.get("protoPayload", {}).get("metadata", {}).get("tableDataChange", {}).get("jobName", "").split("destinationTable:")[1] if "destinationTable:" in message_data.get("protoPayload", {}).get("metadata", {}).get("tableDataChange", {}).get("jobName", "") else None

     # Correctly extract the URI from the 'params' and 'query' keys
    destination_uri = None
    query = message_data.get("params", {}).get("query", "")
    if "uri='" in query:
        # Find the URI string and strip the quotes and surrounding parentheses
        start_index = query.find("uri='") + 5
        end_index = query.find("'", start_index)
        destination_uri = query[start_index:end_index]
        logging.info(f"extracted uri is: {destination_uri} and job status is {job_status}")
    
    destination_uri='gs://fcip_test_bucket/ocv/000000000000.csv'

    if job_status == "SUCCEEDED" and destination_uri:
        logging.info(f"BigQuery job completed successfully. Triggering AlloyDB import. destintion_uri is {destination_uri}")

        # AlloyDB requires a CSV format for import from GCS
       # import_request = {
        #    "name": f"projects/{project_id}/locations/{region}/clusters/{cluster_id}",
         #   "gcs_uri": destination_uri,          # e.g. gs://bucket/path/file.csv (or .sql/.gz)
          #  "database": db_id,                    # target DB name (not resource path)
           # "user": user,                         # DB user to run the import as
       #     "csv_import_options": {               # for CSV imports
       #         "table": table                    # target table name
       #         # "columns": ["col1","col2"],    # optional
       #         # "field_delimiter": "2C",       # optional: comma
       #         # "quote_character": "22",       # optional: double-quote
       #         # "escape_character": "5C",      # optional: backslash
       #     },
       #s }


        import_request = {
            "name": f"projects/{project_id}/locations/{region}/clusters/{cluster_id}",
            "gcs_uri": "gs://fcip_test_bucket/ocv/000000000000.csv",
            "database": db_id,
            "user": user,
            "csv_import_options": { "table": table },
        }

        
        # Initialize the AlloyDB client
        client_options = ClientOptions(api_endpoint="alloydb.googleapis.com")
        client = alloydb.AlloyDBAdminClient(client_options=client_options)
        
        # Invoke the import API
        try:
            logging.info(f"Importing data from {destination_uri} to AlloyDB table {table}")
            operation = client.import_cluster(request=import_request)
            # You can poll the operation status here if needed
            logging.info(f"AlloyDB import operation started: {operation.operation.name}")
            return operation.operation.name
        except Exception as e:
            logging.error(f"Failed to start AlloyDB import: {e}")
            raise

    else:
        logging.warning("BigQuery job either failed or a valid GCS destination URI was not found. Not triggering AlloyDB import.")


with DAG(
    dag_id="bq_job_completion_to_alloydb_import_dag",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["gcp", "bigquery", "alloydb", "event-driven"],
) as dag:
    
    # 1. Await a message on the Pub/Sub subscription.
    # The sensor will pull a single message and push it to XCom.
    await_bq_job_completion_event = PubSubPullSensor(
        task_id="await_bq_job_completion_event",
        project_id=GCP_PROJECT_ID,
        subscription=PUBSUB_SUBSCRIPTION,
        max_messages=1,
        ack_messages=True,  # Acknowledge the message so it's not pulled again
    )

    # 2. Parse the message and trigger the AlloyDB import if successful.
    trigger_alloydb_import = PythonOperator(
        task_id="trigger_alloydb_import",
        python_callable=parse_pubsub_message_and_trigger_import,
        op_kwargs={
            "project_id": GCP_PROJECT_ID,
            "region": ALLOYDB_REGION,
            "cluster_id": ALLOYDB_CLUSTER_ID,
            "db_id": ALLOYDB_DATABASE_ID,
            "user": ALLOYDB_USER,
            "table": ALLOYDB_TABLE,
        },
    )

    # Define the task dependency
    await_bq_job_completion_event >> trigger_alloydb_import
