
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
	 # Read the sql file
	 fd = open('mypostsql.sql', 'r')
	 QUERY = fd.read()
	 fd.close()
	 bq_client = bigquery.Client(project=PROJECTID)
	 query_job = bq_client.query(
	     QUERY, project="mpc-dev-459470", location = "australia-southeast1"
	 ) # API request
    results = query_job.result()
	 return 'OK'
	 #while not query_job.done():
	 #   print( "Job {} is currently in state {}".format( query_job.job_id, query_job.state ) )
	 #   time.sleep( 10 )
	 #if query_job.errors != None:
	 #   print( "Query Failed." )
	 #   raise Exception( "Query Failed. Error: [ %s ]." % query_job.error_result )
