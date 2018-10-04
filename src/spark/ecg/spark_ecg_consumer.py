import sys

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


def insert_samples(logger, postgres_config, s3bucket_config, a, record):
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
        self.setupDB()

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
            cur.execute("CREATE OR REPLACE FUNCTION create_partition_and_insert() RETURNS trigger AS"
                        "$BODY$"
                        "DECLARE"
                        "      partition_date TEXT;"
                        "      partition TEXT;"
                        "      BEGIN"
                        "           partition_date := to_char(NEW.time,'YYYY_MM_DD');"
                        "           partition := TG_RELNAME || '_' || partition_date;"
                        "      IF NOT EXISTS(SELECT relname FROM pg_class WHERE relname=partition) THEN"
                        "           RAISE NOTICE 'A partition has been created %',partition;"
                        "           EXECUTE 'CREATE TABLE ' || partition || ' (check (time = ''' || NEW.time || ''')) INHERITS (' || TG_RELNAME || ');';"
                        "       END IF"
                        "       partition_time := to_char(NEW.time,'YYYY_MM_DD_HH24_MI_SS_MS');"
                        "       EXECUTE 'INSERT INTO ' || partition || ' SELECT(' || TG_RELNAME || ' ' || quote_literal(NEW) || ').*;';"
                        "       RETURN NULL;"
                        "       END;"
                        "       $BODY$"
                        "LANGUAGE plpgsql VOLATILE"
                        "COST 100;")
            cur.execute("CREATE TRIGGER signal_samples_insert_trigger BEFORE INSERT ON signal_samples FOR EACH ROW EXECUTE PROCEDURE create_partition_and_insert();")
            # print("created inst_hr table")
            conn.commit()
            cur.close()
            conn.close()
            self.logger.warn("Done setting up DB tables")
        except Exception as e:
            self.logger.warn('Exception %s' % e)

    def run(self):
        sqlContext = SQLContext(self.sc)

        lines = self.kafkastream.map(lambda x: x[1])
        self.logger.warn('Reading in kafka stream line')

        raw_record = lines.map(lambda line: line.encode('utf-8')). \
            map(lambda line: line.split(','))
        if raw_record is not None:
            raw_record.pprint()
        else:
            print('raw_record is none')

        record_interval = raw_record.map(lambda x: (x[0],
                                                    Row(time=datetime.strptime(x[1], '%Y-%m-%d %H:%M:%S.%f'),
                                                        ecg1=float(x[2]), ecg2=float(x[3]), ecg3=float(x[4])))). \
            groupByKey().map(lambda x: (x[0], x[1]))

        s3bucket_config = self.s3bucket_config
        batchnum = accum(self.a)

        def test(dstream):
            def _test(signals):
                for signal in signals:
                    signame = signal[0]
                    df = sqlContext.createDataFrame(signal[1])
                    df.write.parquet("s3a://{}:{}@{}/processed/batchnum{:05d}-{}.txt".
                                     format(s3bucket_config['aws_access_key_id'],
                                            s3bucket_config['aws_secret_access_key'],
                                            s3bucket_config['bucket'],
                                            batchnum, datetime.now()))

            dstream.foreach(_test)

        record_interval.foreachRDD(test)

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
