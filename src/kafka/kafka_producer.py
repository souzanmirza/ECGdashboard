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

    def __init__(self, ip_addr, config_dict):
        try:
            self.kafka_config = helpers.parse_config(config_dict['kafka_config'])
        except Exception as e:
            self.kafka_config = None
            print('No kafka config %s' % e)
        try:
            self.s3bucket_config = helpers.parse_config(config_dict['s3bucket_config'])
        except Exception as e:
            self.s3bucket_config = None
            print('No s3bucket config %s' % e)
        try:
            self.spark_config = helpers.parse_config(config_dict['spark_config'])
        except Exception as e:
            self.spark_config = None
            print('No spark config %s' % e)

        if self.kafka_config is not None:
            self.topic = self.kafka_config['topic']
            self.producer = KafkaProducer(bootstrap_servers=ip_addr)
        elif self.spark_config is not None:
            self.topic = self.spark_config['topic']
            self.producer = KafkaProducer(bootstrap_servers=ip_addr,
                                          value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        else:
            print('No topic provided')
            sys.exit()

    '''
    def produce_msgs_buffer(self, file_key):
        msg_cnt = 0
        while True:

            s3 = boto3.client('s3')
            obj = s3.get_object(Bucket=self.s3bucket_config['bucket'],
                                Key="%s_signals.txt" % file_key)['Body']

            messagebuffer = np.chararray((fs + 2), itemsize=10)
            messagebuffer[0] = file_key
            messagebuffer[1] = datetime.now().strftime("%Y%m%d-%H%M%S")
            for i in range(2, fs + 2):
                # this takes some time to read in...
                messagebuffer[i] = '%0.3f' % (float(obj._raw_stream.readline().decode().split(' ')[0]))
            self.producer.send(self.kafka_config['topic'], value=messagebuffer)
            msg_cnt += 1
            print(messagebuffer[np.newaxis:, ], len(messagebuffer))
            time.sleep(0.001)
    '''
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
                    self.producer.send(self.topic, msg)
                    msg_cnt += 1
                print(message_info)
                time.sleep(0.001)

    def produce_minibatch_msgs(self, grouped_signal_samples):
        msg_cnt = 0

        while True:
            if len(grouped_signal_samples) > 1:
                self.producer.send(self.topic, grouped_signal_samples)
                msg_cnt += 1
            print(grouped_signal_samples)
            time.sleep(0.001)


if __name__ == "__main__":
    print('kafka_producer called')
    args = sys.argv
    ip_addr = str(args[1])
    file_key = str(args[2])
    config_dict = {'kafka_config': '../../.config/kafka.config', 's3bucket_config': '../../.config/s3bucket.config'}
    prod = Producer(ip_addr, config_dict)
    prod.produce_ecg_signal_msgs(file_key)
