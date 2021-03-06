import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import psycopg2
import pandas as pd
import boto3
from StringIO import StringIO

sys.path.append('../../python/')
import helpers

s3bucket_config_infile = '../../../.config/s3bucket.config'
postgres_config_infile = '../../../.config/postgres.config'

s3bucket_config = helpers.parse_config(s3bucket_config_infile)
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


def dumpToS3():
    """
    Saves current snapshot of the database to S3 bucket.
    """
    file_key = 'signal_samples_dump_' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '.csv'
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
    print('dataframe has rows: ', df.count())
    s3 = boto3.client('s3')
    s3.put_object(Bucket=s3bucket_config['bucket'], Key='db_dumps/' + file_key, Body=csv_buffer.getvalue())


def dropOldChunks():
    """
    Drops chunks older than now()-1 minute.
    """
    conn = connectToDB(postgres_config)
    cur = conn.cursor()
    sqlcmd = "SELECT drop_chunks(interval '1 minute', 'signal_samples');"
    cur.execute(sqlcmd)
    conn.commit()
    cur.close()
    conn.close()


default_args = {
    'owner': 'souzan',
    'start_date': datetime(2018, 10, 12)
}

# Setting up DAG
with DAG('maintain_database',
         default_args=default_args,
         schedule_interval=timedelta(minutes=5),
         catchup=False,
         ) as dag:
    dumpToS3 = PythonOperator(task_id='dumpToS3',
                              python_callable=dumpToS3)
    sleep = BashOperator(task_id='sleep',
                         bash_command='sleep 5')
    dropOldChunks = PythonOperator(task_id='dropOldChunks',
                                   python_callable=dropOldChunks)

#Order of operations.
dumpToS3 >> sleep >> dropOldChunks
