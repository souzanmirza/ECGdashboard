import os
import sys
#sys.path.append('../python/')
#import helpers

#spark_config = helpers.parse_config('../../.config/spark.config')

#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 consumer.py 52.201.50.203:9092 ecg-data'

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from datetime import datetime
from python.ECG import findHR
import numpy as np

if __name__ == '__main__':
    sc = SparkContext(appName='PythonStreamingDirectKafkaWordCount')
    sc.setLogLevel("FATAL")
    ssc = StreamingContext(sc, 2)
    brokers = 'ec2-52-1-201-90.compute-1.amazonaws.com:9092'
    topic = 'ecg-topic'
    kafkastream = KafkaUtils.createDirectStream(ssc, [topic],{'metadata.broker.list': brokers})


    def convertrecord(record):
        convertedrecord = []
        convertedrecord.append(datetime.strptime(record[0], '%Y-%m-%d %H:%M:%S.%f'))
        convertedrecord.append(float(record[1].strip('mv')))
        convertedrecord.append(float(record[2].strip('mv')))
        convertedrecord.append(float(record[3].strip('mv')))
        return convertedrecord

    lines = kafkastream.map(lambda x: x[1])
    raw_record = lines.map(lambda line: line.encode('utf-8')).\
        map(lambda line: line.split(',')).\
        map(lambda line: [line[0], convertrecord(line[1:])])


    record_interval = map(lambda line: (line[0], line[1:])).\
         groupByKey().map(lambda x : (x[0], list(x[1])))


    HR = record_interval.map(findHR(np.array(record_interval[1])[1], np.array(record_interval[1])[2]))


    #need to fix this map/reduce statement cuz I have no idea what it's doing.
    #need to remove logs or save them to s3 bucket
    raw_record.pprint()
    HR.pprint()
    ssc.start()
    ssc.awaitTermination()
