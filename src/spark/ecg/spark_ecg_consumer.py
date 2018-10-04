import sys
import os

sys.path.append('../../python/')
sys.path.append('../../kafka/')

from kafka.producer import KafkaProducer
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition
from pyspark.sql.types import StructType, StructField, StringType
import pyspark.sql.context
from pyspark.sql import SQLContext, Row
import psycopg2
import psycopg2.extras as extras
import helpers
import logging
import json
import pickle
import datetime



def accum(a):
    a.add(1)
    return a.value


def insert_samples(logger, postgres_config, a, record):
    logger.warn('fxn insert_samples')

    def _insert_samples(sqlcmd1, sqlcmd2, signals):
        logger.warn('fxn _insert_samples')
        for signal in signals:
            # print(len(signal))
            _sqlcmd1 = sqlcmd1.format(a, signal[0])
            # print(signal[0], len(signal[1]), signal[1][0])
            try:
                conn = psycopg2.connect(host=postgres_config['host'],
                                        database=postgres_config['database'],
                                        port=postgres_config['port'],
                                        user=postgres_config['user'],
                                        password=postgres_config['password'])
                cur = conn.cursor()
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

    # print(record.take(1))
    sqlcmd1 = "PREPARE inserts AS INSERT INTO signal_samples(batchnum, signame, time, ecg1, ecg2, ecg3) VALUES ({}, '{}', $1, $2, $3, $4) ON CONFLICT DO NOTHING;"
    sqlcmd2 = "EXECUTE inserts (%s, %s, %s, %s);"
    record.foreachPartition(lambda x: _insert_samples(sqlcmd1, sqlcmd2, list(x)))

class SparkConsumer:

    def __init__(self, kafka_config_infile, ecg_spark_config_infile, postgres_config_infile, s3bucket_config_infile):
        if not os.path.exists('./tmp'):
            os.makedirs('./tmp')
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(levelname)s %(message)s',
                            filename='./tmp/spark_consumer.log',
                            filemode='w')
        self.logger = logging.getLogger('py4j')
        self.logger.setLevel(logging.WARN)
        self.ecg_spark_config = helpers.parse_config(ecg_spark_config_infile)
        self.postgres_config = helpers.parse_config(postgres_config_infile)
        self.s3bucket_config = helpers.parse_config(s3bucket_config_infile)
        self.kafka_config = helpers.parse_config(kafka_config_infile)
        self.sc = SparkContext(appName='PythonStreamingDirectKafkaWordCount')
        self.sc.setLogLevel("FATAL")
        self.ssc = StreamingContext(self.sc, 2)
        self.logger.warn('Opened spark Context')
        self.kafkastream = self.connectToKafkaBrokers()
        self.logger.warn('Opened connection to Kafka brokers')
        self.a = self.sc.accumulator(0)

    def start(self):
        self.ssc.start()
        self.logger.warn('Spark context started')
        self.ssc.awaitTermination()
        self.logger.warn('Spark context terminated')

    def connectToKafkaBrokers(self):
        kafkastream = KafkaUtils.createDirectStream(self.ssc, [self.kafka_config["topic"]],
                                                    {"metadata.broker.list": self.kafka_config['ip-addr'],
                                                     "group.id": self.ecg_spark_config['group-id'],
                                                     "num.partitions": str(self.kafka_config['partitions'])})
        # kafkastream = KafkaUtils.createDirectStream(self.ssc, [self.ecg_spark_config['topic']],
        #                                             {'metadata.broker.list': self.ecg_spark_config['ip-addr'],
        #                                              'group.id': self.ecg_spark_config['group-id']})
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

        record_interval = raw_record.map(lambda x: (x[0], x[1:])). \
            groupByKey().map(lambda x: (x[0], list(x[1])))

        record_interval.foreachRDD(lambda x: insert_samples(self.logger, self.postgres_config, accum(self.a), x))

        self.ssc.start()
        self.logger.warn('Spark context started')
        self.ssc.awaitTermination()
        self.logger.warn('Spark context terminated')


if __name__ == '__main__':
    ecg_spark_config_infile = '../../../.config/ecgspark.config'
    kafka_config_infile = '../../../.config/kafka.config'
    postgres_config_infile = '../../../.config/postgres.config'
    s3bucket_config_infile = '../../../.config/s3bucket.config'
    consumer = SparkConsumer(kafka_config_infile, ecg_spark_config_infile, postgres_config_infile,
                             s3bucket_config_infile)
    consumer.run()
