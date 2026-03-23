from airflow.sdk import dag, task
from google.cloud import storage
import requests
import pandas as pd
import logging
import tempfile
from pendulum import datetime, duration
from airflow.timetables.trigger import DeltaTriggerTimetable
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.sdk import Variable
from datasets import raw_crashes,raw_persons,raw_vehicles   
import os


@dag(
        dag_id="pull_vehicles_dag",
        schedule=[raw_persons]

)



def pull_vehicles_dag():
    @task.python
    def extract(**kwargs):
        ti = kwargs['ti']
        url = "https://data.cityofchicago.org/resource/68nd-jvt3"
        
        chunk_size =150000
        offset =0
        vehicle_columns = [
        "crash_unit_id",
        "crash_record_id",
        "crash_date",
        "vehicle_id",
        "make",
        "model",
        "vehicle_year",
   ]
        tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
        temp_path = tmp_file.name
        tmp_file.close()
        first_chunk = True
        
        schema = {
        'crash_unit_id': 'int64',
        'crash_record_id': 'str',
        'crash_date': 'datetime64[ns]',
        'vehicle_id': 'int64',
        'make': 'str',
        'model': 'str',
        'vehicle_year': 'int64' }
        
        
                
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
                df= df[vehicle_columns]
                df["vehicle_year"] = df["vehicle_year"].fillna(-1)
                df["crash_unit_id"] = df["crash_unit_id"].fillna(-1)
                df["vehicle_id"] = df["vehicle_id"].fillna(-1)
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
        
    @task(outlets=[raw_vehicles])
    def upload_to_gcs_task(**kwargs):
        ti = kwargs["ti"]
        local_path = ti.xcom_pull(key="return_value", task_ids="extract")

        bucket_name = Variable.get("gcs_bucket", default=None)
        object_name = Variable.get("gcs_object_name", default=None)

        hook = GCSHook(gcp_conn_id="google_cloud_default")
        hook.upload(
            bucket_name=bucket_name,
            object_name="chicago_traffic/vehicles.csv",
            filename=local_path,
            mime_type="text/csv",
            timeout=500,
        )
        os.remove(local_path)
        logging.info(f"Uploaded {local_path} to gs://{bucket_name}/{object_name} and removed local file")
        
    
        
   
            
        
    extract_data = extract()
    
    upload_data = upload_to_gcs_task()
    
    extract_data >> upload_data 
    
pull_vehicles_dag()