import sys
import os

sys.path.append('../../python/')

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import helpers
import logging
import json
import boto3
from spark_helpers import *


class SparkConsumer:
    """
    Class for spark consumer reading from kafka topic which contains the ecg timeseries data.
    """

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
        """
        Starts the streaming context to start subscribing to kafka topic
        """
        self.ssc.start()
        self.logger.warn('Spark context started')
        self.ssc.awaitTermination()
        self.logger.warn('Spark context terminated')

    def connectToKafkaBrokers(self):
        """
        Setup subscription to kafka topic
        """
        kafkastream = KafkaUtils.createDirectStream(self.ssc, [self.kafka_config["topic"]],
                                                    {"metadata.broker.list": self.kafka_config['ip-addr'],
                                                     "group.id": self.ecg_spark_config['group-id'],
                                                     "num.partitions": str(self.kafka_config['partitions'])})
        self.logger.warn('Connected kafka stream to spark context')
        return kafkastream

    def runECG(self):
        """
        Grouping and insertion of ecg samples into database
        :return:
        """
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

        record_interval.foreachRDD(lambda x: insertECGSamples(self.logger, self.postgres_config, accum(self.a), x))

        self.ssc.start()
        self.logger.warn('Spark context started')
        self.ssc.awaitTermination()
        self.logger.warn('Spark context terminated')

    def runHR(self):
        """
        Grouping and calculation of HR for insertion in database
        :return:
        """
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=self.s3bucket_config['bucket'],
                            Key="mgh001_metadata.txt")
        file_content = obj['Body'].read().decode('utf-8')
        meta_data = json.loads(file_content)
        fs = meta_data['fs']
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
            lambda x: processHRSample(self.logger, self.postgres_config, accum(self.a), fs, x))
        self.logger.warn('Saved records to DB')

        self.ssc.start()
        self.logger.warn('Spark context started')
        self.ssc.awaitTermination()
        self.logger.warn('Spark context terminated')
