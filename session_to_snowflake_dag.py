from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import timedelta
from datetime import datetime

def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')

    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()

@task
def set_stage():
    """
    Creating/replacing stage with the data from url
    """
    cur = return_snowflake_conn()
    try:
        cur.execute("""
            CREATE OR REPLACE STAGE raw.blob_stage
            url = 's3://s3-geospatial/readonly/'
            file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"');""")
        
    except Exception as e:
        print(e)
        raise e
    
@task
def load():
    """
    Load data into user_session_channel and session_timestamp tables
    """
    cur = return_snowflake_conn()
    try:
        cur.execute("BEGIN;")
        
        # create tables
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.user_session_channel (
                userId int not NULL,
                sessionId varchar(32) primary key,
                channel varchar(32) default 'direct'  
            );""")
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.session_timestamp (
                sessionId varchar(32) primary key,
                ts timestamp  
            );""")
        
        # copy data into tables
        cur.execute("""
            COPY INTO raw.user_session_channel
            FROM @raw.blob_stage/user_session_channel.csv;""")
        
        cur.execute("""
            COPY INTO raw.session_timestamp
            FROM @raw.blob_stage/session_timestamp.csv;""")

        cur.execute("COMMIT;")
        
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise e
 

with DAG(
  dag_id = 'SessionToSnowflake',
  start_date = datetime(2025,10,20),
  catchup=False,
  tags=['ETL'],
  schedule = None
) as dag:

    stage = set_stage()
    load_data = load()

    stage >> load_data
