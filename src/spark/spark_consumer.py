import os
import sys
#sys.path.append('../python/')
#import helpers

#spark_config = helpers.parse_config('../../.config/spark.config')

#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 consumer.py 52.201.50.203:9092 ecg-data'

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__ == '__main__':
    sc = SparkContext(appName='PythonStreamingDirectKafkaWordCount')
    ssc = StreamingContext(sc, 2)
    brokers = 'ec2-52-201-50-203.compute-1.amazonaws.com:9092'
    topic = 'ecg-topic'
    kafkastream = KafkaUtils.createDirectStream(ssc, [topic],{'metadata.broker.list': brokers})
    lines = kafkastream.map(lambda x: x[i] for i in range(len(x)))
    counts = lines.map(lambda line: line.split(' '))
    #need to fix this map/reduce statement cuz I have no idea what it's doing.
    #need to remove logs or save them to s3 bucket
    counts.pprint()
    ssc.start()
    ssc.awaitTermination()