import sys
sys.path.append('../python')
import helpers
import os

spark_config = helpers.parse_config('../../.config/spark.config')
ipaddr = spark_config['master-ip-addr'].decode().split(',')[0]


os.system("/usr/local/spark/bin/spark-submit --master spark://%s --packages org.apache.spark/spark-streaming-kafka-0-8_2.11/2.3.1 ~/ECGdashboard/src/spark/spark_consumer_class.py"%(ipaddr))
