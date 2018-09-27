import os
import sys

sys.path.append('../python/')

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


def process_and_save_sample(logger, postgres_config, s3bucket_config, a, record):
    logger.warn('fxn insert_sample')

    def _insert_sample(sqlcmd1, sqlcmd2, signals):
        for signal in signals:
            try:
                conn = psycopg2.connect(host=postgres_config['host'],
                                        database=postgres_config['database'],
                                        port=postgres_config['port'],
                                        user=postgres_config['user'],
                                        password=postgres_config['password'])
                cur = conn.cursor()
                sqlcmd1 = sqlcmd1.format(a, signal[0])
                print(sqlcmd1)
                cur.execute(sqlcmd1)
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
                sampleHR = (
                signame, [[findHR(ts_datetime, ecg1), findHR(ts_datetime, ecg2), findHR(ts_datetime, ecg3)]])
                signals_HR.append(sampleHR)
        else:
            logger.debug('No HR returned')

        sqlcmd3 = "PREPARE inserts AS INSERT INTO inst_hr(batchnum, signame, hr1, hr2, hr3) VALUES ({}, '{}', $1, $2, $3) ON CONFLICT DO NOTHING;"
        sqlcmd4 = "EXECUTE inserts (%s, %s, %s)"
        _insert_sample(sqlcmd3, sqlcmd4, signals_HR)

    sqlcmd1 = "PREPARE inserts AS INSERT INTO signal_samples(batchnum, signame, time, ecg1, ecg2, ecg3) VALUES ({}, '{}', $1, $2, $3, $4) ON CONFLICT DO NOTHING;"
    sqlcmd2 = "EXECUTE inserts (%s, %s, %s, %s);"

    record.foreachPartition(lambda x: _insert_sample(sqlcmd1, sqlcmd2, x))
    record.foreachPartition(_calculateHR)

    record.repartition(1).saveAsTextFile(
        "s3a://{}:{}@{}/processed/batchnum{:05d}-{}.txt".format(s3bucket_config['aws_access_key_id'],
                                                                s3bucket_config['aws_secret_access_key'],
                                                                s3bucket_config['bucket'],
                                                                a, datetime.now()))


class SparkConsumer:

    def __init__(self, spark_config_infile, postgres_config_infile, s3bucket_config_infile):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(levelname)s %(message)s',
                            filename='./tmp/spark_consumer.log',
                            filemode='w')
        self.logger = logging.getLogger('py4j')
        self.logger.setLevel(logging.WARN)
        self.spark_config = helpers.parse_config(spark_config_infile)
        self.postgres_config = helpers.parse_config(postgres_config_infile)
        self.s3bucket_config = helpers.parse_config(s3bucket_config_infile)
        self.sc = SparkContext(appName='PythonStreamingDirectKafkaWordCount')
        self.sc.setLogLevel("FATAL")
        self.ssc = StreamingContext(self.sc, 5)
        self.logger.warn('Opened spark Context')
        self.kafkastream = self.openKafka()
        self.a = self.sc.accumulator(0)
        self.setupDB()

    def start(self):
        self.ssc.start()
        self.logger.warn('Spark context started')
        self.ssc.awaitTermination()
        self.logger.warn('Spark context terminated')

    def openKafka(self):
        topic, n = self.spark_config["topic"], self.spark_config["partitions"]
        try:
            fromOffsets = {TopicAndPartition(topic, i): long(0) for i in range(n)}
        except:
            fromOffsets = None
        # not exactly sure what fromOffsets does
        kafkastream = KafkaUtils.createDirectStream(self.ssc, [topic],
                                                {"metadata.broker.list": self.spark_config['ip-addr'],
                                                 'group.id': self.spark_config['group-id']},
                                                fromOffsets=fromOffsets)
        # kafkastream = KafkaUtils.createDirectStream(self.ssc, [self.spark_config['topic']],
        #                                             {'metadata.broker.list': self.spark_config['ip-addr'],
        #                                              'group.id': self.spark_config['group-id']})
        self.logger.warn('Connected kafka stream to spark context')
        return kafkastream

    def setupDB(self):
        self.logger.warn("Setting up DB tables")
        try:
            conn = psycopg2.connect(host=self.postgres_config['host'],
                                    database=self.postgres_config['database'],
                                    port=self.postgres_config['port'],
                                    user=self.postgres_config['user'],
                                    password=self.postgres_config['password'])
            cur = conn.cursor()
            # print(self.postgres_config)
            cur.execute("CREATE TABLE IF NOT EXISTS signal_samples (id serial PRIMARY KEY,\
                                                       batchnum int NOT NULL, \
                                                       signame varchar(50) NOT NULL, \
                                                       time timestamp NOT NULL, \
                                                       ecg1 float(1) NOT NULL, \
                                                       ecg2 float(1) NOT NULL, \
                                                       ecg3 float(1) NOT NULL);")
            # print("created signal_samples table")
            cur.execute("CREATE INDEX IF NOT EXISTS signal_samples_idx ON signal_samples (signame, time);")
            cur.execute("CREATE TABLE IF NOT EXISTS inst_hr (id serial PRIMARY KEY, \
                                                           batchnum int NOT NULL, \
                                                           signame varchar(50) NOT NULL, \
                                                           hr1 float(1) NOT NULL, \
                                                           hr2 float(1) NOT NULL, \
                                                           hr3 float(1) NOT NULL);")
            cur.execute("CREATE INDEX IF NOT EXISTS inst_hr_idx ON inst_hr (batchnum, signame);")
            # print("created inst_hr table")
            conn.commit()
            cur.close()
            conn.close()
            self.logger.warn("Done setting up DB tables")
        except Exception as e:
            self.logger.warn('Exception %s' % e)

    def run(self):
        lines = self.kafkastream.map(lambda x: x[1])
        self.logger.warn('Reading in kafka stream line')

        raw_record = lines.map(lambda line: line.encode('utf-8')). \
            map(lambda line: line.split(','))

        record_interval = raw_record.map(lambda line: (line[0], line[1:])). \
            groupByKey().map(lambda x: (x[0], list(x[1])))

        record_interval.foreachRDD(
            lambda x: process_and_save_sample(self.logger, self.postgres_config, self.s3bucket_config, accum(self.a),
                                              x))

        self.logger.warn('Saved records to DB and S3 and calculated HR for 2s spark stream mini-batch')

        self.ssc.start()
        self.logger.warn('Spark context started')
        self.ssc.awaitTermination()
        self.logger.warn('Spark context terminated')


if __name__ == '__main__':
    spark_config_infile = '../../.config/spark.config'
    postgres_config_infile = '../../.config/postgres.config'
    s3bucket_config_infile = '../../.config/s3bucket.config'
    consumer = SparkConsumer(spark_config_infile, postgres_config_infile, s3bucket_config_infile)
    consumer.run()
