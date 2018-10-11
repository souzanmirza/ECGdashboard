import sys
sys.path.append('..')

from spark_consumer import SparkConsumer

if __name__ == '__main__':
    hr_spark_config_infile = '../../../.config/hrspark.config'
    kafka_config_infile = '../../../.config/kafka.config'
    postgres_config_infile = '../../../.config/postgres.config'
    s3bucket_config_infile = '../../../.config/s3bucket.config'
    consumer = SparkConsumer(kafka_config_infile, hr_spark_config_infile, postgres_config_infile,
                             s3bucket_config_infile)
    consumer.run_ecg()
