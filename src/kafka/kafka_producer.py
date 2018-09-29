import sys
from kafka.producer import KafkaProducer
import boto3
import time
from datetime import datetime

sys.path.append('../python/')
import helpers
import pickle
import numpy as np
import json

fs = 360


class Producer(object):

    def __init__(self, ip_addr, kafka_config_infile, s3bucket_config_infile):
        self.kafka_config = helpers.parse_config(config_dict['kafka_config'])
        self.s3bucket_config = helpers.parse_config(config_dict['s3bucket_config'])
        self.producer = KafkaProducer(bootstrap_servers=ip_addr)

    def produce_ecg_signal_msgs(self, file_key):
        """
        produces messages and sends them to topic
        to do: tag all samples from same second with same timestamp (fs = 360 ie. 360 samples / second)
        """
        msg_cnt = 0

        while True:

            s3 = boto3.client('s3')
            obj = s3.get_object(Bucket=self.s3bucket_config['bucket'],
                                Key="%s_signals.txt" % file_key)
            # for i in range(fs):
            # time_field = datetime.now().strftime("%Y%m%d-%H%M%S")
            for line in obj['Body'].iter_lines():
                linesplit = line.decode().split(',')
                str_fmt = "{},{},{},{},{}"
                message_info = str_fmt.format(file_key,
                                              linesplit[0],
                                              linesplit[1],
                                              linesplit[2],
                                              linesplit[3]
                                              )
                try:
                    msg = str.encode(message_info)
                except:
                    msg = None
                if msg is not None:
                    self.producer.send(self.kafka_config['topic'], msg)
                    msg_cnt += 1
                print(message_info)
                time.sleep(0.001)

   
if  __name__ == "__main__":
    print('kafka_producer called')
    args = sys.argv
    ip_addr = str(args[1])
    file_key = str(args[2])
    kafka_config_infile = '../../.config/kafka.config'
    s3bucket_config_infile = '../../.config/s3bucket.config'
    prod = Producer(ip_addr, kafka_config_infile, s3bucket_config_infile)
    prod.produce_ecg_signal_msgs(file_key)
