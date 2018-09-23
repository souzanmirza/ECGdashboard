import os
import sys
sys.path.append('../python/')
#import helpers

#spark_config = helpers.parse_config('../../.config/spark.config')

#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 consumer.py 52.201.50.203:9092 ecg-data'

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from datetime import datetime
from detect_peaks import detect_peaks
import numpy as np

def findHR(ts, ecg):
   '''detect HR using avg of R-R intervals'''
   #ecg = filter_ecg(ecg)
   maxpeak = 0.33 * max(ecg)
   if maxpeak > 0:
       locs = detect_peaks(ecg, mph=maxpeak)
       if len(locs) > 1:
           Rpeaks_ts = ts[locs]
           diff_ts = np.diff(Rpeaks_ts)
           bps = np.sum([diff_ts[i].total_seconds() for i in range(len(diff_ts))]) / len(diff_ts)
           bpm = bps * 60
           if bpm > 0:           
              return int(bpm)
           else:
              return None
       else:
           return None
   else:
       return None

def convertrecord(record):
    convertedrecord = []
    convertedrecord.append(datetime.strptime(record[0], '%Y-%m-%d %H:%M:%S.%f'))
    convertedrecord.append(float(record[1].strip('mv')))
    convertedrecord.append(float(record[2].strip('mv')))
    convertedrecord.append(float(record[3].strip('mv')))
    return convertedrecord

def _calculateHR(x):
    # for key in rdd_.keys().collect():
    #     x = rdd_.lookup(key)
    ts_str = x[1][:, 0]
    #print(ts_str)
    if len(ts_str) > 3:
        #print('passed: ', ts_str.shape)
        ts_datetime = [datetime.strptime(ts_str[i], '%Y-%m-%d %H:%M:%S.%f') for i in range(len(ts_str))]
        ts_datetime = np.array(ts_datetime)
        ecg1 = np.array(x[1][:, 1]).astype(float)
        #print(ecg1)
        ecg2 = np.array(x[1][:, 2]).astype(float)
        ecg3 = np.array(x[1][:, 3]).astype(float)
        sampleHR = [x[0], findHR(ts_datetime, ecg1), findHR(ts_datetime, ecg2), findHR(ts_datetime, ecg3)]
        print(sampleHR)
        return sampleHR
    else:
        return None


def calculateHR(rdd):
    HR=[]
    HR.append(rdd.foreach(_calculateHR))
    print('calculateHR result is: ', HR)

if __name__ == '__main__':
    sc = SparkContext(appName='PythonStreamingDirectKafkaWordCount')
    sc.setLogLevel("FATAL")
    ssc = StreamingContext(sc, 5)
    brokers = 'ec2-52-1-201-90.compute-1.amazonaws.com:9092'
    topic = 'ecg-topic'
    kafkastream = KafkaUtils.createDirectStream(ssc, [topic],{'metadata.broker.list': brokers})

    lines = kafkastream.map(lambda x: x[1])
    raw_record = lines.map(lambda line: line.encode('utf-8')).\
        map(lambda line: line.split(','))

    converted_record = raw_record.map(lambda line: [line[0], convertrecord(line[1:])])

    record_interval = raw_record.map(lambda line: (line[0], line[1:])).\
         groupByKey().map(lambda x : (x[0], np.array(list(x[1]))))

    record_interval.foreachRDD(calculateHR)

    #need to fix this map/reduce statement cuz I have no idea what it's doing.
    #need to remove logs or save them to s3 bucket
    #converted_record.pprint()
    #record_interval.pprint()
    ssc.start()
    ssc.awaitTermination()

