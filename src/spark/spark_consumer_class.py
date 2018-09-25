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
        raw_record.foreachRDD(lambda x: (self.insert_sample(accum(self.a), x)))
        self.logger.info('Saved records to db')
        self.ssc.start()
        self.logger.info('Spark context started')
        self.ssc.awaitTermination()
        self.logger.info('Spark context terminated')

        record_interval = raw_record.map(lambda line: (line[0], line[1:])). \
            groupByKey().map(lambda x: (x[0], np.array(list(x[1]))))

        record_interval.foreachRDD(self.calculateHR)
        self.logger.info('Calculated HR for 2s spark stream mini-batch')

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

    def _calculateHR(self, x):
        print('fxn _calculateHR')
        self.logger.info('fxn _calculateHR')
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
            sampleHR = [self.a.value, x[0], self.findHR(ts_datetime, ecg1), self.findHR(ts_datetime, ecg2), self.findHR(ts_datetime, ecg3)]
            self.insert_inst_hr(sampleHR)
        else:
            self.logger.debug('No HR returned')


    def calculateHR(self, record):
        self.logger.info('fxn calculateHR')
        record.foreach(self._calculateHR)


    def insert_inst_hr(self, x):
        sqlcmd = "INSERT INTO inst_hr(batchnum, signame, hr1, hr2, hr3) " \
                 "VALUES (%s, %s, %s, %s, %s)"
        print('in fx _insert_inst_hr %s' % sqlcmd)
        try:
            conn = psycopg2.connect(host=self.postgres_config['host'],
                                    database=self.postgres_config['database'],
                                    port=self.postgres_config['port'],
                                    user=self.postgres_config['user'],
                                    password=self.postgres_config['password'])
            cur = conn.cursor()
            cur.execute(sqlcmd, x)
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            # self.logger.debug('Exception %s' % e)
            print('Exception %s' % e)

    def insert_sample(self, a, record):
        # print('fxn insert ', logger)
        # print('accum in insert', a)
        self.logger.info('fxn insert')
        sqlcmd = "INSERT INTO signal_samples(batchnum, signame, time, ecg1, ecg2, ecg3) " \
                 "VALUES (" + str(a) + ", %s, %s, %s, %s, %s) " \
                                       "ON CONFLICT DO NOTHING"
        #print(sqlcmd)
        # print('fxn insert_partition: record is %s '%type(record))
        # record_bcast = self.sc.broadcast(record)
        postgres_config = self.postgres_config

        def _insert_sample(x):
            # print('x in _insert_partition', list(x), type(x))
            # print(x)
            # print('fxn _insert ')
            # self.logger.info('fxn _insert')
            try:
                conn = psycopg2.connect(host=postgres_config['host'],
                                        database=postgres_config['database'],
                                        port=postgres_config['port'],
                                        user=postgres_config['user'],
                                        password=postgres_config['password'])
                cur = conn.cursor()
                cur.executemany(sqlcmd, list(x))
                # cur.execute("DEALLOCATE inserts")
                # cur.execute(sqlcmd, [a] + list(x))
                conn.commit()
                cur.close()
                conn.close()
            except Exception as e:
                # self.logger.debug('Exception %s' % e)
                print('Exception %s' % e)
        record.foreachPartition(_insert_sample)

if __name__ == '__main__':
    spark_config_infile = '../../.config/spark.config'
    postgres_config_infile = '../../.config/postgres.config'
    consumer = SparkConsumer(spark_config_infile, postgres_config_infile)
    # consumer.start()
    consumer.run()