from airflow.decorators import task
from airflow import DAG
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
from datetime import timedelta
import logging

def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')

    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()

@task
def run_ctas(schema, table, select_sql, primary_key=None):

    logging.info(table)
    logging.info(select_sql)

    cur = return_snowflake_conn()

    try:
        sql = f"CREATE OR REPLACE TABLE {schema}.temp_{table} AS {select_sql}"
        logging.info(sql)
        cur.execute(sql)

        # do primary key uniquess check
        if primary_key is not None:
            sql = f"""
              SELECT {primary_key}, COUNT(1) AS cnt 
              FROM {schema}.temp_{table}
              GROUP BY 1
              ORDER BY 2 DESC
              LIMIT 1"""
            print(sql)
            cur.execute(sql)
            result = cur.fetchone()
            print(result, result[1])
            if int(result[1]) > 1:
                print("!!!!!!!!!!!!!!")
                raise Exception(f"Primary key uniqueness failed: {result}")
            
            # second duplicate check
            sql = f"""
                SELECT COUNT(DISTINCT {primary_key}) as distinct_keys, COUNT(1) as total_keys
                FROM {schema}.temp_{table}"""
            cur.execute(sql)
            result_2 = cur.fetchone()
            distinct_keys = result_2[0]
            total_keys = result_2[1]
            duplicate_count = total_keys - distinct_keys
            if total_keys != distinct_keys:
                print("Duplicate keys present:", duplicate_count)
                raise Exception(f"Duplicate primary keys detected: {duplicate_count}")
            
        main_table_creation_if_not_exists_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} AS
            SELECT * FROM {schema}.temp_{table} WHERE 1=0;"""
        cur.execute(main_table_creation_if_not_exists_sql)

        swap_sql = f"""ALTER TABLE {schema}.{table} SWAP WITH {schema}.temp_{table};"""
        cur.execute(swap_sql)
    except Exception as e:
        raise

with DAG(
    dag_id = 'BuildSummary',
    start_date = datetime(2024,10,2),
    catchup=False,
    tags=['ELT'],
    schedule = None
) as dag:

    schema = "analytics"
    table = "session_summary"
    select_sql = """SELECT u.*, s.ts
    FROM raw.user_session_channel u
    JOIN raw.session_timestamp s ON u.sessionId=s.sessionId
    """

    run_ctas(schema, table, select_sql, primary_key='sessionId')
