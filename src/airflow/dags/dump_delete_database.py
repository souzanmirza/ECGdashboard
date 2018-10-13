import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import psycopg2
import pandas as pd
import boto3
import gzip
from io import StringIO, BytesIO

sys.append('../../python/')
import helpers

s3_config_infile = '../../../.config/s3bucket.config'
postgres_config_infile = '../../../.config/postgres.config'

s3_config = helpers.parse_config(s3_config_infile)
postgres_config = helpers.parse_config(postgres_config_infile)

schema = ['id', 'batchnum', 'signame', 'time', 'ecg1', 'ecg2', 'ecg3']

def connectToDB(postgres_config):
    """
    :return: database cursor
    """
    try:
        conn = psycopg2.connect(host=postgres_config['host'],
                                database=postgres_config['database'],
                                port=postgres_config['port'],
                                user=postgres_config['user'],
                                password=postgres_config['password'])
    except Exception as e:
        print(e)
    return conn


def dump_to_s3(schema, s3bucket_config):
    file_key = 'signal_samples_dump_' + datetime.now() + '.csv'
    conn = connectToDB(postgres_config)
    cur = conn.cursor()

    sqlcmd = "SELECT * FROM signal_samples \
                        ORDER BY time;"
    cur.execute(sqlcmd)
    df = pd.DataFrame(cur.fetchall(), columns=schema)
    cur.close()
    conn.close()

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    # reset stream position
    csv_buffer.seek(0)
    # create binary stream
    gz_buffer = BytesIO()

    # compress string stream using gzip
    with gzip.GzipFile(mode='w', fileobj=gz_buffer) as gz_file:
        gz_file.write(bytes(csv_buffer.getvalue(), 'utf-8'))

    # write stream to S3
    s3 = boto3.client('s3')
    s3.put_object(Bucket=s3bucket_config['bucket'], Key=file_key, Body=gz_buffer.getvalue())


def drop_old_chunks():
    conn = connectToDB(postgres_config)
    cur = conn.cursor()
    sqlcmd = "SELECT drop_chunks(interval '5 minutes', 'signal_samples');"
    cur.execute(sqlcmd)
    conn.commit()
    cur.close()
    conn.close()


default_args = {
    'owner': 'me',
    'start_date': datetime(2017, 6, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG('airflow_tutorial_v01',
         default_args=default_args,
         schedule_interval='0 * * * *',
         ) as dag:

    dump_to_s3 = PythonOperator(task_id='dump_to_s3',
                                 python_callable=dump_to_s3)
    sleep = BashOperator(task_id='sleep',
                         bash_command='sleep 5')
    drop_old_chunks = PythonOperator(task_id='drop_old_chunks',
                                 python_callable=drop_old_chunks)

dump_to_s3 >> sleep >> drop_old_chunks