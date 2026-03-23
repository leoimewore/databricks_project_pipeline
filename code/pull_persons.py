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
from datasets import raw_crashes,raw_persons
import os


@dag(
        dag_id="pull_persons_dag",
        schedule=[raw_crashes]
)



def pull_persons_dag():
    @task.python
    def extract(**kwargs):
        ti = kwargs['ti']
        
        url = "https://data.cityofchicago.org/resource/u6pd-qa9d"
        chunk_size =150000
        offset =0
        person_columns = [
            "person_id",
            "person_type",
            "crash_record_id",
            "vehicle_id",
            "crash_date",
            "city",
            "state",
            "zipcode",
            "sex",
            "age"
        ]
        tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
        temp_path = tmp_file.name
        tmp_file.close()
        first_chunk = True
        
        schema = {
        'person_id': 'str',
        'person_type': 'str',
        'crash_record_id': 'str',
        'vehicle_id': 'int64',
        'crash_date': 'datetime64[ns]',
        'city': 'str',
        'state': 'str',
        'zipcode': 'str',
        'sex': 'str',
        'age': 'int64',
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
                df= df[person_columns]
                df["age"] = df["age"].fillna(-1)
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
        
    @task(outlets=[raw_persons])
    def upload_to_gcs_task(**kwargs):
        ti = kwargs["ti"]
        local_path = ti.xcom_pull(key="return_value", task_ids="extract")

        bucket_name = Variable.get("gcs_bucket", default=None)
        object_name = Variable.get("gcs_object_name", default=None)

        hook = GCSHook(gcp_conn_id="google_cloud_default")
        hook.upload(
            bucket_name=bucket_name,
            object_name="chicago_traffic/persons.csv",
            filename=local_path,
            mime_type="text/csv",
            timeout=500,
        )
        os.remove(local_path)
        logging.info(f"Uploaded {local_path} to gs://{bucket_name}/{object_name} and removed local file")
        
   
            
        
    extract_data = extract()
    
    upload_data = upload_to_gcs_task()
    
    extract_data >> upload_data   
        

    
pull_persons_dag()