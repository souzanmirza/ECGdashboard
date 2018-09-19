import sys
from kafka.client import KafkaClient
from kafka.producer import KafkaProducer
import boto3
import lazyreader
import time
from datetime import datetime
sys.path.append('../python/')
import helpers

class Producer(object):

    def __init__(self, addr):
        self.kafka_config = helpers.parse_config('../../.config/kafka.config')
        self.producer = KafkaProducer(bootstrap_servers=addr)

    def produce_msgs(self, file_key):
        """
        produces messages and sends them to topic
        """
        msg_cnt = 0

        while True:

            s3 = boto3.client('s3')
            obj = s3.get_object(Bucket=self.kafka_config['bucket'],
                                Key="%s_signals.txt"%file_key)
            for line in obj['Body'].iter_lines():
                linesplit = line.decode().split(' ')
                time_field = datetime.now().strftime("%Y%m%d %H%M%S")
                str_fmt = "{},{},{}mv"
                message_info = str_fmt.format(file_key,
                                              time_field,
                                              linesplit[0]
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