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
import psycopg2
import psycopg2.pool as pool
import helpers
import logging


def accum(a):
    a.add(1)
    return a.value

def findHR(ts, ecg):
    #self.logger.info('fxn findHR')
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
                #self.logger.debug('Invalid HR returned')
                return -1
        else:
            #self.logger.debug('Invalid HR returned')
            return -1
    else:
        #self.logger.debug('Invalid HR returned')
        return -1

def calculateHR(postgres_config, a, record):
    print('fxn calculateHR')
    #self.logger.info('fxn calculateHR')
    #a = self.a.value
    #postgres_config = self.postgres_config

    def _calculateHR(x):
        print('fxn _calculateHR')
        #self.logger.info('fxn _calculateHR')
        ts_str = x[1][:, 0]
        if len(ts_str) > 3:
            # print('passed: ', ts_str.shape)
            ts_datetime = [datetime.strptime(ts_str[i], '%Y-%m-%d %H:%M:%S.%f') for i in range(len(ts_str))]
            ts_datetime = np.array(ts_datetime)
            ecg1 = np.array(x[1][:, 1]).astype(float)
            # print(ecg1)
            ecg2 = np.array(x[1][:, 2]).astype(float)
            ecg3 = np.array(x[1][:, 3]).astype(float)
            sampleHR = [a, x[0], findHR(ts_datetime, ecg1), findHR(ts_datetime, ecg2), findHR(ts_datetime, ecg3)]

            def insert_inst_hr(x):
                sqlcmd = "INSERT INTO inst_hr(batchnum, signame, hr1, hr2, hr3) " \
                         "VALUES (%s, %s, %s, %s, %s)"
                print('in fx _insert_inst_hr %s' % sqlcmd)
                try:
                    conn = psycopg2.connect(host=postgres_config['host'],
                                            database=postgres_config['database'],
                                            port=postgres_config['port'],
                                            user=postgres_config['user'],
                                            password=postgres_config['password'])
                    cur = conn.cursor()
                    cur.execute(sqlcmd, x)
                    conn.commit()
                    cur.close()
                    conn.close()
                except Exception as e:
                    # self.logger.debug('Exception %s' % e)
                    print('Exception %s' % e)

            insert_inst_hr(sampleHR)
            return

        else:
            return
            #self.logger.debug('No HR returned')
    #print('record ', record, type(record))
    record.foreach(lambda x: _calculateHR(x))


def insert_sample(postgres_config, a, record):
    sqlcmd = "INSERT INTO signal_samples(batchnum, signame, time, ecg1, ecg2, ecg3) " \
             "VALUES (" + str(a) + ", %s, %s, %s, %s, %s) " \
                                   "ON CONFLICT DO NOTHING"
    print(sqlcmd)
    def _insert_sample(x):
        try:
            conn = psycopg2.connect(host=postgres_config['host'],
                                    database=postgres_config['database'],
                                    port=postgres_config['port'],
                                    user=postgres_config['user'],
                                    password=postgres_config['password'])
            cur = conn.cursor()
            cur.executemany(sqlcmd, list(x))
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            print('Exception %s' % e)
    record.foreachPartition(_insert_sample)


class SparkConsumer:

    def __init__(self, spark_config_infile, postgres_config_infile):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(levelname)s %(message)s',
                            filename='./tmp/spark_consumer.log',
                            filemode='w')
        self.logger = logging.getLogger('py4j')
        self.logger.setLevel(logging.INFO)
        self.spark_config = helpers.parse_config(spark_config_infile)
        self.postgres_config = helpers.parse_config(postgres_config_infile)
        self.sc = SparkContext(appName='PythonStreamingDirectKafkaWordCount')
        self.sc.setLogLevel("FATAL")
        self.ssc = StreamingContext(self.sc, 5)
        self.logger.info('Opened spark Context')
        self.kafkastream = self.openKafka()
        self.a = self.sc.accumulator(0)

    def start(self):
        self.ssc.start()
        self.logger.info('Spark context started')
        self.ssc.awaitTermination()
        self.logger.info('Spark context terminated')

    def openKafka(self):
        kafkastream = KafkaUtils.createDirectStream(self.ssc, [self.spark_config['topic']],
                                                    {'metadata.broker.list': self.spark_config['ip-addr']})
        self.logger.info('Connected kafka stream to spark context')
        return kafkastream

    def run(self):
        lines = self.kafkastream.map(lambda x: x[1])
        self.logger.info('Reading in kafka stream line')

        raw_record = lines.map(lambda line: line.encode('utf-8')). \
            map(lambda line: line.split(','))
        raw_record.foreachRDD(lambda x: (insert_sample(self.postgres_config, accum(self.a), x)))
        self.logger.info('Saved records to db')


        record_interval = raw_record.map(lambda line: (line[0], line[1:])). \
            groupByKey().map(lambda x: (x[0], np.array(list(x[1]))))

        record_interval.foreachRDD(lambda x: calculateHR(self.postgres_config, self.a.value, x))
        self.logger.info('Calculated HR for 2s spark stream mini-batch')


        self.ssc.start()
        self.logger.info('Spark context started')
        self.ssc.awaitTermination()
        self.logger.info('Spark context terminated')

if __name__ == '__main__':
    spark_config_infile = '../../.config/spark.config'
    postgres_config_infile = '../../.config/postgres.config'
    consumer = SparkConsumer(spark_config_infile, postgres_config_infile)
    # consumer.start()
    consumer.run()

