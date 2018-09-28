IP_ADDR="$(hostname)"
DEPS="../python/helpers.py,../python/detect_peaks.py"
DEPS_ARR=($DEPS)

/usr/local/spark/bin/spark-submit --master spark://$IP_ADDR:7077 --num-executors 3 --packages org.apache.spark/spark-streaming-kafka-0-8_2.11/2.3.1 --py-files $DEPS  ~/ECGdashboard/src/spark/spark_consumer_class.py
