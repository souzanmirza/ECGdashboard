import os
import sys

sys.path.append('../../python/')

# spark_config = helpers.parse_config('../../.config/spark.config')

# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 consumer.py 52.201.50.203:9092 ecg-data'

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition
from datetime import datetime
import detect_peaks
import numpy as np
import psycopg2
import psycopg2.extras as extras
import helpers
import logging
import json


def accum(a):
    a.add(1)
    return a.value


def findHR(ts, ecg):
    # self.logger.warn('fxn findHR')
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
                # self.logger.debug('Invalid HR returned')
                return -1
        else:
            # self.logger.debug('Invalid HR returned')
            return -1
    else:
        # self.logger.debug('Invalid HR returned')
        return -1


def process_sample(logger, postgres_config, s3bucket_config, a, record):
    logger.warn('fxn insert_sample')


    def _insert_sample(sqlcmd1, sqlcmd2, signals):
        logger.warn('fxn _insert_sample')
        #print('fxn _insert_sample signals is ', len(list(signals)), type(signals))
        #print('fxn _insert_sample', list(signals))
        for signal in signals:
            #print(type(signal))
            #print('fxn _insert_sample', signal[0])
            _sqlcmd1 = sqlcmd1.format(a, signal[0])
            try:
                #print('in try block')
                conn = psycopg2.connect(host=postgres_config['host'],
                                        database=postgres_config['database'],
                                        port=postgres_config['port'],
                                        user=postgres_config['user'],
                                        password=postgres_config['password'])
                #print('conn successful')
                cur = conn.cursor()
                #print('cur opened')
                print(_sqlcmd1)
                logger.warn(_sqlcmd1)
                cur.execute(_sqlcmd1)
                extras.execute_batch(cur, sqlcmd2, signal[1])
                cur.execute("DEALLOCATE inserts")
                conn.commit()
                cur.close()
                conn.close()
            except Exception as e:
                logger.warn('Exception %s' % e)

    def _calculateHR(signals):
        # print('fxn _calculateHR')
        logger.warn('fxn _calculateHR')
        signals_HR = []
        for x in signals:
            # print('x', len(x), type(x))
            signame = str(x[0])
            signal = np.array(x[1])
            ts_str = signal[:, 0]
            if len(ts_str) > 3:
                # print('passed: ', ts_str.shape)
                ts_datetime = [datetime.strptime(ts_str[i], '%Y-%m-%d %H:%M:%S.%f') for i in range(len(ts_str))]
                ts_datetime = np.array(ts_datetime)
                ecg1 = np.array(signal[:, 1]).astype(float)
                # print(ecg1)
                ecg2 = np.array(signal[:, 2]).astype(float)
                ecg3 = np.array(signal[:, 3]).astype(float)
                logger.warn("calling findhr")
                sampleHR = (signame, [[findHR(ts_datetime, ecg1), findHR(ts_datetime, ecg2), findHR(ts_datetime, ecg3)]])
                #print(sampleHR)
                signals_HR.append(sampleHR)
        else:
            logger.debug('No HR returned')

        sqlcmd3 = "PREPARE inserts AS INSERT INTO inst_hr(batchnum, signame, hr1, hr2, hr3) VALUES ({}, '{}', $1, $2, $3) ON CONFLICT DO NOTHING;"
        sqlcmd4 = "EXECUTE inserts (%s, %s, %s)"
        _insert_sample(sqlcmd3, sqlcmd4, signals_HR)
    record.foreachPartition(_calculateHR)



class SparkConsumer:

    def __init__(self, kafka_config_infile, hr_spark_config_infile, postgres_config_infile, s3bucket_config_infile):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(levelname)s %(message)s',
                            filename='./tmp/spark_consumer.log',
                            filemode='w')
        self.logger = logging.getLogger('py4j')
        self.logger.setLevel(logging.WARN)
        self.spark_config = helpers.parse_config(hr_spark_config_infile)
        self.postgres_config = helpers.parse_config(postgres_config_infile)
        self.s3bucket_config = helpers.parse_config(s3bucket_config_infile)
        self.kafka_config = helpers.parse_config(kafka_config_infile)
        self.sc = SparkContext(appName='PythonStreamingDirectKafkaWordCount')
        self.sc.setLogLevel("FATAL")
        self.ssc = StreamingContext(self.sc, 10)
        self.logger.warn('Opened spark Context')
        self.kafkastream = self.openKafka()
        self.a = self.sc.accumulator(0)

    def start(self):
        self.ssc.start()
        self.logger.warn('Spark context started')
        self.ssc.awaitTermination()
        self.logger.warn('Spark context terminated')

    def openKafka(self):
        kafkastream = KafkaUtils.createDirectStream(self.ssc, [self.kafka_config['topic']],
                                                {"metadata.broker.list": self.kafka_config['ip-addr'],
                                                 "group.id": self.spark_config['group-id'],
                                                 "num.partitions": str(self.kafka_config['partitions'])})
        # kafkastream = KafkaUtils.createDirectStream(self.ssc, [self.spark_config['topic']],
        #                                             {'metadata.broker.list': self.spark_config['ip-addr'],
        #                                              'group.id': self.spark_config['group-id']})
        self.logger.warn('Connected kafka stream to spark context')
        return kafkastream

    def run(self):
        lines = self.kafkastream.map(lambda x: x[1])
        self.logger.warn('Reading in kafka stream line')
        raw_record = lines.map(lambda line: line.encode('utf-8')). \
            map(lambda line: line.split(','))
        if raw_record is not None:
            raw_record.pprint()
        else:
            print('raw_record is none')
        record_interval = raw_record.map(lambda line: (line[0], line[1:])). \
            groupByKey().map(lambda x: (x[0], list(x[1])))
        record_interval.foreachRDD(
            lambda x: process_sample(self.logger, self.postgres_config, self.s3bucket_config, accum(self.a), x))
        self.logger.warn('Saved records to DB')

        self.ssc.start()
        self.logger.warn('Spark context started')
        self.ssc.awaitTermination()
        self.logger.warn('Spark context terminated')


if __name__ == '__main__':
    hr_spark_config_infile = '../../../.config/hrspark.config'
    kafka_config_infile = '../../../.config/kafka.config'
    postgres_config_infile = '../../../.config/postgres.config'
    s3bucket_config_infile = '../../../.config/s3bucket.config'
    consumer = SparkConsumer(kafka_config_infile, hr_spark_config_infile, postgres_config_infile, s3bucket_config_infile)
    consumer.run()

