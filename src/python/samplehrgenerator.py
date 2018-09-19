import numpy as np

def constantpeaks(bpm, fs, ecg =np.array([]), seconds=6):
    length = fs*seconds
    fake_ecg = np.zeros((1, length))
    RR_interval = 1/bpm
    peaks = range(0,length,round(RR_interval * fs))
    fake_ecg[:,peaks] = 1
    if len(ecg) > 0:
        fake_ecg = np.hstack([ecg, fake_ecg])
    return fake_ecg

if __name__ == '__main__':
    fakeecg1 = constantpeaks(60, 360, seconds = 10)
    fakeecg2 = constantpeaks(72, 360, seconds = 10, ecg = fakeecg1)
    np.savetxt('../../data/fake003_signals.txt', fakeecg2, delimiter = ' ', fmt='%d')
