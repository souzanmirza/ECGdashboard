import ECG
import numpy as np
import matplotlib.pyplot as plt

ecg = np.genfromtxt('../../data/fake003_signals.txt')
plt.plot(range(len(ecg)), ecg)
plt.hold('on')
HR, R_peaks = ECG.findHR(360, ecg)
plt.scatter(R_peaks[:,0], R_peaks[:,1], c='r')
plt.show(block=True)
print(HR)