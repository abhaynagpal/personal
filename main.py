import base64
import os
from google.cloud import bigquery
import time
import datetime

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
   query_job = bq_client.query(QUERY, project="mpc-dev-459470", location = "australia-southeast1") # API request
   #rows_df = query_job.result().to_dataframe() # Waits for query to finish
   
   print("Last 10 jobs:")
   for job in client.list_jobs(max_results=10):  # API request(s)
    print(job.job_id)
   #while query_job.state == "RUNNING":
    #print( "Job {} is currently in state {}".format( query_job.job_id, query_job.state ) )
    #time.sleep( 5 )
   if query_job.errors != None:
    print( "Query Failed." )
    raise Exception( "Query Failed. Error: [ %s ]." % query_job.error_result )



