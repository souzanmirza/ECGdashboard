import os
import sys

sys.path.append('../python/')

# spark_config = helpers.parse_config('../../.config/spark.config')

# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 consumer.py 52.201.50.203:9092 ecg-data'

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from datetime import datetime
import detect_peaks
import numpy as np
import psycopg2.pool as pool
import helpers
import logging


def findHR(ts, ecg):
    logging.info('fxn findHR')
    '''detect HR using avg of R-R intervals'''
    # ecg = filter_ecg(ecg)
    maxpeak = 0.33 * max(ecg)
    if maxpeak > 0:
        locs = detect_peaks.detect_peaks(ecg, mph=maxpeak)
        if len(locs) > 1:
            Rpeaks_ts = ts[locs]
            diff_ts = np.diff(Rpeaks_ts)
            bps = np.sum([diff_ts[i].total_seconds() for i in range(len(diff_ts))]) / len(diff_ts)
            bpm = bps * 60
            if bpm > 0:
                return int(bpm)
            else:
                logging.debug('No HR returned')
                return None
        else:
            logging.debug('No HR returned')
            return None
    else:
        logging.debug('No HR returned')
        return None


def _calculateHR(x):
    logging.info('fxn _calculateHR')
    # for key in rdd_.keys().collect():
    #     x = rdd_.lookup(key)
    ts_str = x[1][:, 0]
    # print(ts_str)
    if len(ts_str) > 3:
        # print('passed: ', ts_str.shape)
        ts_datetime = [datetime.strptime(ts_str[i], '%Y-%m-%d %H:%M:%S.%f') for i in range(len(ts_str))]
        ts_datetime = np.array(ts_datetime)
        ecg1 = np.array(x[1][:, 1]).astype(float)
        # print(ecg1)
        ecg2 = np.array(x[1][:, 2]).astype(float)
        ecg3 = np.array(x[1][:, 3]).astype(float)
        sampleHR = [x[0], findHR(ts_datetime, ecg1), findHR(ts_datetime, ecg2), findHR(ts_datetime, ecg3)]
        print(sampleHR)
        return sampleHR
    else:
        logging.debug('No HR returned')
        return None

def get_connection(connpool):
    logging.info('fxn get_connection')
    conn = connpool.getconn()
    try:
        yield conn
    finally:
        connpool.putconn(conn)

def _insert(connpool, x):
    logging.info('fxn _insert')
    sqlcmd = "INSERT INTO signal_samples(signame, time, ecg1, ecg2, ecg3) " \
                     "VALUES (%s, %s, %s, %s, %s)"
    for conn in get_connection(connpool).__iter__():
        print(conn)
        try:
            cur = conn.cursor()
            cur.execute(sqlcmd, x)
            cur.close()
            conn.commit()
        except Exception as e:
            logging.debug('Exception %s, rolling back'%e)
            conn.rollback()

def insert(connpool, record):
    logging.info('fxn insert')
    record.foreach(lambda x: _insert(connpool, x))


def calculateHR(rdd):
    logging.debug('fxn calculateHR')
    HR = []
    HR.append(rdd.foreach(_calculateHR))
    print('calculateHR result is: ', HR)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(levelname)s %(message)s',
                        filename='./tmp/spark_consumer.log',
                        filemode='w')
    sc = SparkContext(appName='PythonStreamingDirectKafkaWordCount')
    sc.setLogLevel("FATAL")
    ssc = StreamingContext(sc, 5)
    logging.info('Opened spark Context')

    kafka_config = helpers.parse_config('../../.config/spark.config')
    postgres_config = helpers.parse_config('../../.config/postgres.config')
    # print(postgres_config)
    connpool = pool.SimpleConnectionPool(1, 10, host=postgres_config['host'],
                                                  database=postgres_config['database'], port=postgres_config['port'],
                                                  user=postgres_config['user'], password=postgres_config['password'])
    logging.info('Created db pooled connections')

    kafkastream = KafkaUtils.createDirectStream(ssc, [kafka_config['topic']],
                                                {'metadata.broker.list': kafka_config['ip-addr']})
    logging.info('Connected kafka stream to spark context')

    lines = kafkastream.map(lambda x: x[1])
    logging.info('Reading in kafka stream line')

    raw_record = lines.map(lambda line: line.encode('utf-8')). \
        map(lambda line: line.split(','))
    raw_record.foreachRDD(lambda x: insert(connpool, x))
    logging.info('Saved records to db')

    record_interval = raw_record.groupByKey().map(lambda x: (x[0], np.array(list(x[1]))))

    record_interval.foreachRDD(calculateHR)
    logging.info('Calculated HR for 2s spark stream mini-batch')

    ssc.start()
    logging.info('Spark context started')
    ssc.awaitTermination()
    logging.info('Spark context terminated')
