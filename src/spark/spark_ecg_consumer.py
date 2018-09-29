import os
import sys

sys.path.append('../python/')
sys.path.append('../kafka/')

# spark_config = helpers.parse_config('../../.config/spark.config')

# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 consumer.py 52.201.50.203:9092 ecg-data'
from kafka.producer import KafkaProducer
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
import kafka_producer


def accum(a):
    a.add(1)
    return a.value


def insert_samples(logger, postgres_config, s3bucket_config, a, record):
    logger.warn('fxn insert_samples')


    def _insert_samples(sqlcmd1, sqlcmd2, signals):
        logger.warn('fxn _insert_samples')
        for signal in signals:
            #print(len(signal))
            _sqlcmd1 = sqlcmd1.format(a, signal[0])
            #print(signal[0], len(signal[1]), signal[1][0])
            try:
                conn = psycopg2.connect(host=postgres_config['host'],
                                        database=postgres_config['database'],
                                        port=postgres_config['port'],
                                        user=postgres_config['user'],
                                        password=postgres_config['password'])
                cur = conn.cursor()
                #print(_sqlcmd1)
                logger.warn(_sqlcmd1)
                cur.execute(_sqlcmd1)
                extras.execute_batch(cur, sqlcmd2, signal[1])
                cur.execute("DEALLOCATE inserts")
                conn.commit()
                cur.close()
                conn.close()
            except Exception as e:
                logger.warn('Exception %s' % e)



    #print(record.take(1))
    sqlcmd1 = "PREPARE inserts AS INSERT INTO signal_samples(batchnum, signame, time, ecg1, ecg2, ecg3) VALUES ({}, '{}', $1, $2, $3, $4) ON CONFLICT DO NOTHING;"
    sqlcmd2 = "EXECUTE inserts (%s, %s, %s, %s);"
    record.foreachPartition(lambda x: _insert_samples(sqlcmd1, sqlcmd2, list(x)))


    # record.repartition(1).saveAsTextFile(
    #     "s3a://{}:{}@{}/processed/batchnum{:05d}-{}.txt".format(s3bucket_config['aws_access_key_id'],
    #                                                             s3bucket_config['aws_secret_access_key'],
    #                                                             s3bucket_config['bucket'],
    #                                                             a, datetime.now()))


def send_samples(logger, kafka_config, spark_config, a, record):
    logger.warn('fxn send_samples')
    spark_config_infile = '../../.config/spark.config'
    
    def _send_samples(signals):
        print('fxn _send_samples')
        ecg_kafka_producer = KafkaProducer(bootstrap_servers=kafka_config['ip-addr'].split(','),
                                          value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        logger.warn('fxn _send_samples')
        #print('len of signals', len(signals))
        for signal in signals:
            #print('len of signal is ',len(signal))
            grouped_signal_samples = {'batchnum': a, 'signame': signal[0], 'samples': signal[1]}
            print(grouped_signal_samples['batchnum'], grouped_signal_samples['signame'], len(grouped_signal_samples['samples']))
            ecg_kafka_producer.send(spark_config['topic'], grouped_signal_samples)
            logger.warn('in fxn sent samples to topic')
    #print(type(record.take(1)), len(record.take(1)))
    record.foreachPartition(lambda x: _send_samples(list(x)))


class SparkConsumer:

    def __init__(self, kafka_config_infile, spark_config_infile, postgres_config_infile, s3bucket_config_infile):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(levelname)s %(message)s',
                            filename='./tmp/spark_consumer.log',
                            filemode='w')
        self.logger = logging.getLogger('py4j')
        self.logger.setLevel(logging.WARN)
        self.spark_config = helpers.parse_config(spark_config_infile)
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
        self.setupDB()

    def start(self):
        self.ssc.start()
        self.logger.warn('Spark context started')
        self.ssc.awaitTermination()
        self.logger.warn('Spark context terminated')

    def connectToKafkaBrokers(self):
        topic, n = self.kafka_config["topic"], self.kafka_config["partitions"]
        try:
            fromOffsets = {TopicAndPartition(topic, i): long(0) for i in range(n)}
        except:
            fromOffsets = None
        # not exactly sure what fromOffsets does
        print(self.kafka_config)
        kafkastream = KafkaUtils.createDirectStream(self.ssc, [topic],
                                                {"metadata.broker.list": self.kafka_config['ip-addr'],
                                                 "group.id": self.spark_config['group-id'],
                                                 "num.partitions": str(self.kafka_config['partitions'])})
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
                                                       ecg1 float(6) NOT NULL, \
                                                       ecg2 float(6) NOT NULL, \
                                                       ecg3 float(6) NOT NULL);")
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
        #if raw_record is not None:
            #raw_record.pprint()
        #else:
            #print('raw_record is none')
        record_interval = raw_record.map(lambda line: (line[0], line[1:])). \
            groupByKey().map(lambda x: (x[0], list(x[1])))
        #record_interval.pprint(1)
        #record_interval.foreachRDD(lambda x: insert_samples(self.logger, self.postgres_config, self.s3bucket_config, accum(self.a), x))
        self.logger.warn('Saved records to DB')

        record_interval.foreachRDD(lambda x: send_samples(self.logger, self.kafka_config, self.spark_config, self.a.value, x))
        self.logger.warn('Sent samples to kafka topic')


        self.ssc.start()
        self.logger.warn('Spark context started')
        self.ssc.awaitTermination()
        self.logger.warn('Spark context terminated')


if __name__ == '__main__':
    spark_config_infile = '../../.config/spark.config'
    kafka_config_infile = '../../.config/kafka.config'
    postgres_config_infile = '../../.config/postgres.config'
    s3bucket_config_infile = '../../.config/s3bucket.config'
    consumer = SparkConsumer(kafka_config_infile, spark_config_infile, postgres_config_infile, s3bucket_config_infile)
    consumer.run()

