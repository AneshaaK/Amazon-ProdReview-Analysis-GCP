# dataflow.py

# import neccessary libraries
import apache_beam as beam
from apache_beam.options import pipeline_options
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.runners import DataflowRunner
from apache_beam.options.pipeline_options import SetupOptions

import google.auth
from datetime import datetime, timedelta
import re
import json

# Setting up the Apache Beam pipeline options.
options = pipeline_options.PipelineOptions(flags=['--streaming'])

options.view_as(pipeline_options.StandardOptions).streaming = True
_, options.view_as(GoogleCloudOptions).project = google.auth.default()

options.view_as(GoogleCloudOptions).job_name = 'amazon_dataflow'
options.view_as(GoogleCloudOptions).region = 'us-west1'
options.view_as(GoogleCloudOptions).staging_location = 'gs://group1-dataflow/staging'
options.view_as(GoogleCloudOptions).temp_location = 'gs://group1-dataflow/temp'
options.view_as(SetupOptions).save_main_session = True

# assign required topics path
topic_stream = "projects/data228-final-project/topics/streaming_data_in"
topic_batch = "projects/data228-final-project/topics/batch_fileupload_notification"

## Additional code to send errors to dead letter queue table
# Define the dead letter queue table
dead_letter_table = 'data228-final-project-312006:term_project.bqinsert_dead_letter_table'

# pipeline begins here
with beam.Pipeline(options=options) as pipeline:
    import csv

    # read the metadata of batch file
    metadata = pipeline | "Pubsub notification" >> beam.io.ReadFromPubSub(topic=topic_batch)
    transformed_meta = metadata | "Parse data" >> beam.Map(json.loads)
    file_path = transformed_meta | "Batchfile path" >> beam.Map(lambda x: (f'gs://{x["bucket"]}/{x["name"]}'))

    # read batch data from file path
    batch_data = (file_path | "Read batch data" >> beam.io.ReadAllFromText())
   
    # read streaming data from topic
    streaming_data = pipeline | "Read streaming data" >> beam.io.ReadFromPubSub(topic=topic_stream)
    windowed_data = (streaming_data | "Fixed Window" >> beam.WindowInto(beam.window.FixedWindows(3600)))
    transformed = windowed_data | "Convert to bytes" >> beam.Map(lambda x: x.decode("utf-8"))
    
    # merge both batch and streaming data
    merged = (
        (batch_data, transformed) 
        | "Merge batch & streaming" >> beam.Flatten() 
        | "Read merged data" >> beam.Map(lambda line: next(csv.reader([line])))
        )
   
    # filter merged data and write to Bigquery
    (merged 
        | "Filter & Format data" >> beam.Map(lambda x: {"id":x[0], "dateAdded": x[1].replace('Z', ''), "dateUpdated": x[2].replace('Z', ''), "name": x[3], "asins": x[4], "brand": x[5], "categories": x[6], "primaryCategories": x[7], "reviews_date": x[10].replace('Z', ''), "reviews_rating": x[16], "reviews_text": x[17], "reviews_title": x[18], "reviews_username": x[19], "reviews_users_gender": x[20], "shipping": x[21]})
        | "Write to BQ" >> beam.io.WriteToBigQuery("data228-final-project-312006:term_project.amazon_data_raw",
                                                   create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
                                                   write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                                   retry_strategy=beam.io.gcp.bigquery_tools.RetryStrategy.RETRY_ON_TRANSIENT_ERROR, ## Additional code to retry
                                                   dead_letter_table=dead_letter_table ## Additional code to send errors to dead letter queue table
                                                  )
    )
    
    DataflowRunner().run_pipeline(pipeline, options=options).wait_until_finish()
