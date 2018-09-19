import boto3
from config.s3config import S3Config

s3config = S3Config()
file_partition_interval = slice(0,2) #str(args[2])
s3 = boto3.resource('s3', aws_access_key_id=s3config.aws_access_key_id, aws_secret_access_key=s3config.aws_secret_access_key)
obj = s3.Object(s3config.bucket, s3config.records_index)
records = obj.get()['Body'].read().decode().split('\n')
print(records)

objects = [s3.Object("ecgdashboard-bucket", recordname+'_signals.txt') for recordname in records[file_partition_interval]]
while True:
    line=[objects[i].get()['Body']._raw_stream.readline().decode().split(' ')[0:3] for i in range(len(objects))]
    print(line, type(line))