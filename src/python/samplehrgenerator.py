import numpy as np
import datetime

def constantpeaks(bpm, fs, ecg =np.array([]), seconds=6):
    fulldate = datetime.datetime.now()
    length = fs*seconds
    fake_ecg = np.zeros((3, length))
    RR_interval = 1/bpm
    peaks = range(0,length,round(RR_interval * fs))
    timestamps = []
    fake_ecg[:,peaks] = 1
    for i in range(length):
        fulldate = fulldate + datetime.timedelta(milliseconds=1000 / fs)
        timestamps.append(fulldate)
    timestamps = np.array(timestamps, dtype=str)
    fake_ecg = fake_ecg.astype(str)
    timestamps = timestamps[np.newaxis,:].transpose()
    print(timestamps.shape)
    print(fake_ecg.shape)
    fake_ecg = np.hstack([timestamps, fake_ecg.transpose()])
    if len(ecg) > 0:
        fake_ecg = np.vstack([ecg, fake_ecg])
    return fake_ecg

if __name__ == '__main__':
    fakeecg1 = constantpeaks(60, 360, seconds = 10)
    fakeecg2 = constantpeaks(72, 360, seconds = 10, ecg = fakeecg1)
    np.savetxt('../../data/fake003_signals.txt', fakeecg2, delimiter = ',', fmt='%s')
