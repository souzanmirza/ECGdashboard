import sys
import boto3
import numpy as np
import datetime

fs = 360
s3 = boto3.client('s3')
obj = s3.get_object(Bucket='ecgdashboard-bucket',
                                Key="RECORDS.txt")['Body']
files = obj.read().decode('utf-8')
files = np.array(files.split('\n'))

fulldate = datetime.datetime.now()

for file in files:
    print(file)
    try:
        obj = s3.get_object(Bucket='ecgdashboard-bucket',
                        Key="%s_signals.txt"%file)
        ecg_signal = []
        timestamps = []
        for line in obj['Body'].iter_lines():
            signal = line.decode('utf-8').split(' ')
            signal_subset = ['%0.6f'%float(signal[0]), '%0.6f'%float(signal[1]), '%0.6f'%float(signal[2])]
            ecg_signal.append(signal_subset)
            fulldate = fulldate + datetime.timedelta(milliseconds=1000/fs)
            timestamps.append(fulldate)
        timestamps = np.array(timestamps,dtype=str)[:,np.newaxis]
        ecg_signal = np.array(ecg_signal)
        x = np.hstack([timestamps, ecg_signal])
        np.savetxt('./mghdata_ts/%s_signals.txt'%file, x, delimiter=',', fmt='%s')
    except:
        continue
