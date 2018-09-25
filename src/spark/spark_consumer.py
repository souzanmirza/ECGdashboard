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
    logger.info('fxn findHR')
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
                logger.debug('Invalid HR returned')
                return -1
        else:
            logger.debug('Invalid HR returned')
            return -1
    else:
        logger.debug('Invalid HR returned')
        return -1


def _calculateHR(connpool, a, x):
    print('fxn _calculateHR')
    logger.info('fxn _calculateHR')
    sqlcmd =  "INSERT INTO inst_hr(batchnum, signame, hr1, hr2, hr3) " \
             "VALUES (%s, %s, %s, %s, %s)"
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
        _insert(connpool, sqlcmd, a, sampleHR)
    else:
        logger.debug('No HR returned')


def calculateHR(connpool, a, record):
    logger.info('fxn calculateHR')
    record.foreach(lambda x: _calculateHR(connpool, a, x))


def get_connection(connpool):
    logger.info('fxn get_connection')
    conn = connpool.getconn()
    try:
        yield conn
    finally:
        connpool.putconn(conn)

def _insert(connpool, sqlcmd, a, x):
    #print(x)
    #print('fxn _insert ')
    logger.info('fxn _insert')
    for conn in get_connection(connpool).__iter__():
        #print(conn)
        try:
            cur = conn.cursor()
            cur.execute(sqlcmd, [a]+x)
            cur.close()
            conn.commit()
        except Exception as e:
            logger.debug('Exception %s, rolling back'%e)
            print('Exception %s, rolling back'%e)
            conn.rollback()

def insert(connpool, a, record):
    #print('fxn insert ', logger)
    print('accum in insert %s'%a)
    logger.info('fxn insert')
    sqlcmd = "INSERT INTO signal_samples(batchnum, signame, time, ecg1, ecg2, ecg3) " \
             "VALUES (%s, %s, %s, %s, %s, %s)"
    record.foreach(lambda x: _insert(connpool, sqlcmd, a, x))

if __name__ == '__main__':
    global logger
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(levelname)s %(message)s',
                        filename='./tmp/spark_consumer.log',
                        filemode='w')
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.INFO)
    kafka_config = helpers.parse_config('../../.config/spark.config')
    postgres_config = helpers.parse_config('../../.config/postgres.config')
    #print(postgres_config)
    sc = SparkContext(appName='PythonStreamingDirectKafkaWordCount')
    sc.setLogLevel("FATAL")
    ssc = StreamingContext(sc, 5)
    logger.info('Opened spark Context')
    connpool = pool.SimpleConnectionPool(1, 10, host=postgres_config['host'],
                                                  database=postgres_config['database'], port=postgres_config['port'],
                                                  user=postgres_config['user'], password=postgres_config['password'])
    logger.info('Created db pooled connections')

    kafkastream = KafkaUtils.createDirectStream(ssc, [kafka_config['topic']],
                                                {'metadata.broker.list': kafka_config['ip-addr']})
    logger.info('Connected kafka stream to spark context')
    a = sc.accumulator(0)
    def accum():
       a.add(1)
       return a.value
    #print('dstream time: ', kafkastream._jtime(datetime.now()))

    lines = kafkastream.map(lambda x: x[1])
    logger.info('Reading in kafka stream line')
    
    raw_record = lines.map(lambda line: line.encode('utf-8')). \
        map(lambda line: line.split(','))
    raw_record.foreachRDD(lambda x: (insert(connpool, accum(), x)))
    #raw_record.pprint()
    logger.info('Saved records to db')

    record_interval = raw_record.map(lambda line: (line[0], line[1:])). \
        groupByKey(). \
        map(lambda x: (x[0], np.array(list(x[1]))))

    record_interval.foreachRDD(lambda x: calculateHR(connpool, a.value, x))
    logger.info('Calculated HR for 2s spark stream mini-batch')

    ssc.start()
    logger.info('Spark context started')
    ssc.awaitTermination()
    logger.info('Spark context terminated')
