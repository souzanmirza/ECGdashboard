IP_ADDR="$(hostname)"
DEPS="../../python/helpers.py,../../python/detect_peaks.py"
DEPS_ARR=($DEPS)

/usr/local/spark/bin/spark-submit --master local --packages org.apache.spark/spark-streaming-kafka-0-8_2.11/2.3.1 --py-files $DEPS  spark_hr_consumer.py
