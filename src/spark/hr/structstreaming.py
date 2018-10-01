import os
import sys
sys.path.append('../../python')

import helpers

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0,org.apache.spark:spark-sql-kafka-0-10_2.11:2.1.0 pyspark-shell'

hr_spark_config_infile = '../../../.config/hrspark.config'
kafka_config_infile = '../../../.config/kafka.config'
postgres_config_infile = '../../../.config/postgres.config'
s3bucket_config_infile = '../../../.config/s3bucket.config'

spark_config = helpers.parse_config(hr_spark_config_infile)
postgres_config = helpers.parse_config(postgres_config_infile)
s3bucket_config = helpers.parse_config(s3bucket_config_infile)
kafka_config = helpers.parse_config(kafka_config_infile)


from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("SSKafka") \
    .getOrCreate()

dsraw = spark.readStream().format("kafka") \
  .option("kafka.bootstrap.servers", kafka_config['ip-addr']) \
  .option("subscribe", spark_config['topic']) \
  .load()
#ds = dsraw.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

#ds = dsraw.selectExpr("CAST(value AS STRING)")

print(type(dsraw))
#print(type(ds))

#dsraw.writeStream \
 #   .format("console") \
  #  .option("truncate","false") \
    #.start() \
    #.awaitTermination()
