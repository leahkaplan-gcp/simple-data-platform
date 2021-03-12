# Copyright 2021 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#            http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import os
import pytz
from pytz import timezone
from datetime import datetime
import json
import pandas

from google.api_core import retry
from google.cloud import bigquery
from google.cloud import storage

PROJECT_ID = os.getenv('GCP_PROJECT')
BQ_DATASET = 'prod'

cs = storage.Client()
bq = bigquery.Client()
job_config = bigquery.LoadJobConfig()

truck_cycle_name = 'truckcycle'
activity_history_name = 'activityhistory'



def load(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    file = event

    #clean up the filename for pattern matching
    filename = event['name'].strip().casefold().replace('_', '')
    print("filename for matching is {}".format(filename))
    
    if filename.find(truck_cycle_name) != -1:
      print('loading truck cycle data')
      load_file(file['bucket'], file['name'], 'stg_truck_cycle')

    elif filename.find(activity_history_name) != -1:
      print('loading activity history data')
      load_file(file['bucket'], file['name'], 'stg_activity_history')
    
    else:
      raise Exception("filename {0} is not catered for. Not loading".format(file['name']))


    log_run_information(file['bucket'], file['name'])


def load_file(bucket_name, file_name, table_name):
    schema = create_schema_object_from_json(table_name)

    check_if_table_exists(table_name, schema)

    job_config.schema = schema
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.write_disposition = 'WRITE_APPEND'
    job_config.skip_leading_rows = 1
    job_config.max_bad_records = 5
    job_config.allow_jagged_rows = True #allow loading without optional null columns, in this case run_id


    uri = "gs://{0}/{1}".format(bucket_name, file_name)
    table_id = bq.dataset(BQ_DATASET).table(table_name)

    print('creating load job for {0} using {1}'.format(table_name, uri))
    load_job = bq.load_table_from_uri(
      uri,
      table_id,
      job_config=job_config
    )

    print(("Load job for file {0} sent to BigQuery").format(uri))


def log_run_information(bucket_name, file_name):
    TABLE_NAME = 'runs'
    timenow = datetime.utcnow()

    table_schema = create_schema_object_from_json(TABLE_NAME)

    check_if_table_exists(TABLE_NAME, table_schema)

    dataset_ref = bq.dataset(BQ_DATASET)
    table_ref = dataset_ref.table(TABLE_NAME)
    table_id = bq.get_table(table_ref)

    rows = [
      {
        "run_id": None,
        "file_name" : file_name,
        "load_datetime": timenow,
        "run_completed_datetime" : None
      }
    ]

    df = pandas.DataFrame(
      rows,
      columns=["run_id", "file_name", "load_datetime", "run_completed_datetime"]
    )

    job_config = bigquery.LoadJobConfig(
      schema=table_schema,
      write_disposition="WRITE_APPEND"
    )

    bq.load_table_from_dataframe(
      df, table_id, job_config=job_config
    )

    # this uses the streaming buffer-  can't update in a timely fashion
    # # syntax requires python 'None' instead of SQL null
    # rows = [
    #   (None, file_name, timenow, None)
    # ]

    # errors = bq.insert_rows(table_id, rows)


def create_schema_object_from_json(table_name):
    f = open("./{0}.json".format(table_name))
    txt = f.read()
    json_data = json.loads(txt)

    datatype = ''
    schema = []

    for e in json_data:
      if e['type'] == 'INTEGER':
        datatype = 'INT64'
      elif e['type'] == 'FLOAT':
        datatype = 'FLOAT64'
      else:
        datatype = e['type']

      schemaField = bigquery.SchemaField(e['name'], datatype)
      schema.append(schemaField)

    return schema



def check_if_table_exists(tableName,tableSchema):
    # get table_id reference
    table_id = bq.dataset(BQ_DATASET).table(tableName)

     # check if table exists, otherwise create
    try:
      bq.get_table(table_id)
    except Exception:
      logging.warn('Creating table: %s' % (tableName))
      table = bigquery.Table(table_id, schema=tableSchema)
      table = bq.create_table(table)
      print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))
