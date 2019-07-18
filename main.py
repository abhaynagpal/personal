import base64
import os
from google.cloud import bigquery

# Environ variables
PROJECTID = os.environ.get('GCP_PROJECT')

def hello_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')

    
def export_to_table(query,query_job):
   # BQ Query to get add to cart sessions
   QUERY = "INSERT INTO `mpc-dev-459470.DELEVERIES.C_TMP_EM_PIVOT` SELECT * FROM `mpc-dev-459470.DELEVERIES.MC_BASE_PROC_VIEW`"
   bq_client = bigquery.Client(project=PROJECTID)
   query_job = bq_client.query(QUERY) # API request
  # rows_df = query_job.result().to_dataframe() # Waits for query to finish


