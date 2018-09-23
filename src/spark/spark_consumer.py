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
from ECG import findHR
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
        map(lambda line: line.split(','))

    converted_record = raw_record.map(lambda line: [line[0], convertrecord(line[1:])])

    record_interval = raw_record.map(lambda line: (line[0], line[1:])).\
         groupByKey().map(lambda x : (x[0], np.array(list(x[1]))))


    def calculateHR(rdd_):
        HR = []
        for key in rdd_.keys().collect():
	    x=rdd_.lookup(key)
            ts_str = x[0][:,0]
            #print(ts_str)
            if len(ts_str) > 3:
	        print('passed: ', ts_str.shape)
                ts_datetime = [datetime.strptime(ts_str[i], '%Y-%m-%d %H:%M:%S.%f') for i in range(len(ts_str))]
                ts_datetime = np.array(ts_datetime)
                ecg1 = np.array(x[0][:,1]).astype(float)
                print(ecg1)
                ecg2 = np.array(x[0][:, 2]).astype(float)
                ecg3 = np.array(x[0][:, 3]).astype(float)
		sampleHR = [key, findHR(ts_datetime, ecg1), findHR(ts_datetime, ecg2),findHR(ts_datetime, ecg3)]                
		print(sampleHR)
                HR.append(sampleHR)
        return HR
    
    HR = record_interval.foreachRDD(calculateHR)


    #need to fix this map/reduce statement cuz I have no idea what it's doing.
    #need to remove logs or save them to s3 bucket
    #converted_record.pprint()
    #record_interval.pprint()
    ssc.start()
    print(HR)
    ssc.awaitTermination()
