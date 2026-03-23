from airflow.sdk import dag, task
from gcloud.aio.bigquery import Dataset
from google.cloud import storage
import requests
import pandas as pd
import logging
import tempfile
from pendulum import datetime, duration
from airflow.timetables.trigger import DeltaTriggerTimetable
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.sdk import Variable
from datasets import raw_crashes
import os



@dag(
        dag_id="pull_crashes_dag"

)



def pull_crashes_dag():
    @task.python
    def extract(**kwargs):
        ti = kwargs['ti']
        url = "https://data.cityofchicago.org/resource/85ca-t3if"
        
        chunk_size =150000
        offset =0
        crash_columns = [
        "crash_record_id",
        "crash_date",
        "weather_condition",
        "lighting_condition",
        "damage",
        "injuries_total",
        "injuries_fatal",
        "latitude",
        "longitude"
   ]
        tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
        temp_path = tmp_file.name
        tmp_file.close()
        first_chunk = True
        
        schema = {
            'crash_record_id': 'str',
            'crash_date': 'datetime64[ns]',
            'weather_condition': 'str',
            'lighting_condition': 'str',
            'damage': 'str',
            'injuries_total': 'int64',
            'injuries_fatal': 'int64',
            'latitude': 'float64',
            'longitude': 'float64'
        }
        
        
                
        while True:
            params = {
            "$limit": chunk_size,
            "$offset": offset,
            "$$app_token": "YwIAZkmjAhaYhdDM2hIYhHOuu"
        }
            try:
                response=requests.get(url,params=params, timeout=40)
                response.raise_for_status()
            except requests.exceptions.RequestException as err:
                logging.error(f"An error occurred: {err}")
                break
            except Exception as e:
                logging.error(f'An unexpected error occurred: {e}')
                break
            
            try:
                json_data = response.json()
                if not json_data:
                    logging.info("No data returned, exit loop.")
                    break
                df = pd.DataFrame(json_data)
                df = pd.DataFrame(json_data)
                df= df[crash_columns]
                df["injuries_total"] = df["injuries_total"].fillna(-1)
                df["injuries_fatal"] = df["injuries_fatal"].fillna(-1)
                df["latitude"] = df["latitude"].fillna(-1)
                df["longitude"] = df["longitude"].fillna(-1)
                df = df.fillna("Unknown")
                
                df_chunk = df.astype(schema)
                
                if first_chunk:
                    df_chunk.to_csv(temp_path, mode='w', header=first_chunk , index=False)
                    first_chunk = False
                else:
                    df_chunk.to_csv(temp_path, mode='a', header=first_chunk , index=False)
            
                
                # spark_df_chunk = spark.createDataFrame(df_chunk, schema=spark_schema)
            except Exception as e:
                logging.error(f"An unexpected error occurred while processing data: {e}")
                break
            logging.info(f'{offset + len(df)} records extracted and written to {temp_path}')               
            offset += chunk_size
            
        ti.xcom_push(key='return_value', value=temp_path) 
        
        
        
        
    @task(outlets=[raw_crashes])
    def upload_to_gcs_task(**kwargs):
        ti = kwargs["ti"]
        local_path = ti.xcom_pull(key="return_value", task_ids="extract")
        object_name = Variable.get("gcs_object_name", default=None)
        bucket_name = Variable.get("gcs_bucket", default=None)

        hook = GCSHook(gcp_conn_id="google_cloud_default")
        hook.upload(
            bucket_name=bucket_name,
            object_name="chicago_traffic/crashes.csv",
            filename=local_path,
            timeout= 500,
            mime_type="text/csv",
        )
        os.remove(local_path)
        logging.info(f"Uploaded {local_path} to gs://{bucket_name}/{object_name} and removed local file")
        
   
            
        
    extract_data = extract()
    
    upload_data = upload_to_gcs_task()
    
    extract_data >> upload_data
    
pull_crashes_dag()