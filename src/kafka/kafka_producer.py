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
import logging

fs = 360


class Producer(object):

    def __init__(self, ip_addr, kafka_config_infile, s3bucket_config_infile):
        if not os.path.exists('./tmp'):
            os.makedirs('./tmp')
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(levelname)s %(message)s',
                            filename='./tmp/kafka_producer.log',
                            filemode='w')
        self.logger = logging.getLogger('py4j')

        self.kafka_config = helpers.parse_config(kafka_config_infile)
        self.s3bucket_config = helpers.parse_config(s3bucket_config_infile)
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
                try:
                    linesplit = line.decode().split(',')
                    str_fmt = "{},{},{},{},{}"
                    message_info = str_fmt.format(file_key,
                                                  linesplit[0],
                                                  linesplit[1],
                                                  linesplit[2],
                                                  linesplit[3]
                                                  )
                except Exception as e:
                    self.logger.error('fxn produce_ecg_signal_msgs error %s'%e)
                try:
                    msg = str.encode(message_info)
                except:
                    msg = None
                    self.logger.debug('empty message')
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
