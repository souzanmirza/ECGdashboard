import sys
from kafka.client import KafkaClient
from kafka.producer import KafkaProducer
import boto3
import lazyreader
import time
from datetime import datetime
sys.path.append('../python/')
import helpers
import binascii
import numpy as np

fs = 360

class Producer(object):

    def __init__(self, addr):
        self.kafka_config = helpers.parse_config('../../.config/kafka.config')
        self.producer = KafkaProducer(bootstrap_servers=addr)
        # self.producer = KafkaProducer(value_serializer=lambda v: v.encode('utf-8'),
        #                               bootstrap_servers=addr)

    def produce_msgs_buffer(self, file_key):
        msg_cnt = 0
        while True:

            s3 = boto3.client('s3')
            obj = s3.get_object(Bucket=self.kafka_config['bucket'],
                                Key="%s_signals.txt"%file_key)['Body']

            messagebuffer = np.chararray((fs+2), itemsize=10)
            messagebuffer[0] = file_key
            messagebuffer[1] = datetime.now().strftime("%Y%m%d-%H%M%S")
            for i in range(2, fs+2):
                # this takes some time to read in...
                messagebuffer[i] = '%0.3f'%(float(obj._raw_stream.readline().decode().split(' ')[0]))
            self.producer.send(self.kafka_config['topic'], value = messagebuffer)
            msg_cnt += 1
            print(messagebuffer[np.newaxis:,], len(messagebuffer))
            time.sleep(0.001)

    def produce_msgs(self, file_key):
        """
        produces messages and sends them to topic
        to do: tag all samples from same second with same timestamp (fs = 360 ie. 360 samples / second)
        """
        msg_cnt = 0

        while True:

            s3 = boto3.client('s3')
            obj = s3.get_object(Bucket=self.kafka_config['bucket'],
                                Key="%s_signals.txt" %file_key)
            #for i in range(fs):
                #time_field = datetime.now().strftime("%Y%m%d-%H%M%S")
            for line in obj['Body'].iter_lines():
                linesplit = line.decode().split(',')
                str_fmt = "{},{},{}mv,{}mv,{}mv"
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

if __name__ == "__main__":
    print('kafka_producer called')
    args = sys.argv
    ip_addr = str(args[1])
    file_key = str(args[2])
    prod = Producer(ip_addr)
    prod.produce_msgs(file_key)