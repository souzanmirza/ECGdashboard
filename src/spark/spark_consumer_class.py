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
        self.sc, self.scc = self.startSpark()
        self.kafkastream = self.openKafka()
        self.a = self.sc.accumulator(0)
        self.connpool = self.openDBConnectionPool()

    def start(self):
        self.ssc.start()
        self.logger.info('Spark context started')

    def startSpark(self):
        sc = SparkContext(appName='PythonStreamingDirectKafkaWordCount')
        sc.setLogLevel("FATAL")
        ssc = StreamingContext(sc, 5)
        self.logger.info('Opened spark Context')
        return sc, ssc

    def openKafka(self):
        kafkastream = KafkaUtils.createDirectStream(self.ssc, [self.spark_config['topic']],
                                                         {'metadata.broker.list': self.spark_config['ip-addr']})
        self.logger.info('Connected kafka stream to spark context')
        return kafkastream

    def openDBConnectionPool(self):
        connpool = pool.SimpleConnectionPool(1, 10, host=self.postgres_config['host'],
                                             database=self.postgres_config['database'], port=self.postgres_config['port'],
                                             user=self.postgres_config['user'], password=self.postgres_config['password'])
        self.logger.info('Created db pooled connections')
        return connpool

    def run(self):
        lines = self.kafkastream.map(lambda x: x[1])
        self.logger.info('Reading in kafka stream line')

        raw_record = lines.map(lambda line: line.encode('utf-8')). \
            map(lambda line: line.split(','))
        raw_record.foreachRDD(lambda x: (self.insert(accum(self.a), x)))
        self.logger.info('Saved records to db')
        '''
        record_interval = raw_record.map(lambda line: (line[0], line[1:])). \
            groupByKey().map(lambda x: (x[0], np.array(list(x[1]))))

        record_interval.foreachRDD(lambda x: self.calculateHR(self.a.value, x))
        self.logger.info('Calculated HR for 2s spark stream mini-batch')

        # ssc.awaitTermination()
        # logger.info('Spark context terminated')
        '''

    def findHR(self, ts, ecg):
        self.logger.info('fxn findHR')
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
                    self.logger.debug('Invalid HR returned')
                    return -1
            else:
                self.logger.debug('Invalid HR returned')
                return -1
        else:
            self.logger.debug('Invalid HR returned')
            return -1


    def _calculateHR(self, a, x):
        print('fxn _calculateHR')
        self.logger.info('fxn _calculateHR')
        sqlcmd = "INSERT INTO inst_hr(batchnum, signame, hr1, hr2, hr3) " \
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
            sampleHR = [x[0], self.findHR(ts_datetime, ecg1), self.findHR(ts_datetime, ecg2), self.findHR(ts_datetime, ecg3)]
            print(sampleHR)
            self._insert(self.connpool, sqlcmd, a, sampleHR)
        else:
            self.logger.debug('No HR returned')


    def calculateHR(self, a, record):
        self.logger.info('fxn calculateHR')
        record.foreach(lambda x: self._calculateHR(a, x))


    def get_connection(self):
        self.logger.info('fxn get_connection')
        conn = self.connpool.getconn()
        try:
            yield conn
        finally:
            self.connpool.putconn(conn)


    def _insert(self, sqlcmd, a, x):
        # print(x)
        # print('fxn _insert ')
        self.logger.info('fxn _insert')
        for conn in self.get_connection().__iter__():
            # print(conn)
            try:
                cur = conn.cursor()
                cur.execute(sqlcmd, [a] + x)
                cur.close()
                conn.commit()
            except Exception as e:
                self.logger.debug('Exception %s, rolling back' % e)
                print('Exception %s, rolling back' % e)
                conn.rollback()


    def insert(self, a, record):
        # print('fxn insert ', logger)
        print('accum in insert %s' % a)
        self.logger.info('fxn insert')
        sqlcmd = "INSERT INTO signal_samples(batchnum, signame, time, ecg1, ecg2, ecg3) " \
                 "VALUES (%s, %s, %s, %s, %s, %s)"
        record.foreach(lambda x: self._insert(sqlcmd, a, x))


    def insert_partition(self, a, record):
        # print('fxn insert ', logger)
        print('accum in insert %s' % a)
        self.logger.info('fxn insert')
        sqlcmd = "INSERT INTO signal_samples(batchnum, signame, time, ecg1, ecg2, ecg3) " \
                 "VALUES (%s, %s, %s, %s, %s, %s)"
        record.foreachPartition(lambda x: self._insert_partition(sqlcmd, a, x))


    def _insert_partition(self, sqlcmd, a, x):
        # print(x)
        # print('fxn _insert ')
        self.logger.info('fxn _insert')
        try:
            conn = psycopg2.connect(host=self.postgres_config['host'],
                                        database=self.postgres_config['database'],
                                        port=self.postgres_config['port'],
                                        user=self.postgres_config['user'],
                                        password=self.postgres_config['password'])
        except Exception as e:
            self.logger.debug('Exception %s' % e)
            print('Exception %s' % e)

        cur = conn.cursor()
        cur.execute(sqlcmd, [a] + x)
        conn.commit()
        cur.close()
        conn.close()

if __name__ == '__main__':
    spark_config_infile = '../../.config/spark.config'
    postgres_config_infile = '../../.config/postgres.config'
    consumer = SparkConsumer(spark_config_infile, postgres_config_infile)
    consumer.run()



