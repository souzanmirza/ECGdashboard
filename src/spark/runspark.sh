IP_ADDR="$(hostname)"

/usr/local/spark/bin/spark-submit --master $IP_ADDR --packages org.apache.spark/spark-streaming-kafka-0-8_2.11/2.3.1 ~/ECGdashboard/src/spark/spark_consumer_class.py
#Pull repo to node first
