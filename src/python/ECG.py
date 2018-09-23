# -*- coding: utf-8 -*-
"""
Created on Thu Feb 04 13:02:27 2016

@author: Souzan

ECG Processing class. This class extracts the HR, Breathing rate and average R 
wave amplitude of an 8s interval.
This function takes around 0.07-0.13s to run.

"""

import numpy as np
import scipy.signal as signal
from detect_peaks import detect_peaks


def filter_ecg(ecg):
    ''' remove baseline'''
    fftecg=np.fft.fft(ecg)
    step=500.0/len(fftecg) # which is this 500?
    s=int(4./step)
    fftecg[0:s]=0
    ecg_b=(np.fft.ifft(fftecg)).real
    """ highpass filter ecg signal with cutoff frequency """
    b, a = signal.cheby1(2, 1, 2.0/250, 'highpass') # low frequency signals ie gross body movement
    ecg=signal.lfilter(b, a, ecg_b)
    b, a = signal.cheby1(2, 1, 60.0/250, 'lowpass') # 60Hz power lines
    return signal.lfilter(b, a, ecg)

def detect_R_peaks(ecg):
    maxpeak=0.33*max(ecg)
    locs=detect_peaks(ecg, mph=maxpeak)
    return locs, ecg[locs]

def findHR(ts, ecg):
   '''detect HR using avg of R-R intervals'''
   #ecg = filter_ecg(ecg)
   indices, Rpeaks = detect_R_peaks(ecg)
   if indices > 1:
       Rpeaks_ts = ts[indices]
       diff_ts = np.diff(Rpeaks_ts)
       bps = np.sum([diff_ts[i].total_seconds() for i in range(len(diff_ts))]) / len(diff_ts)
       bpm = bps * 60
       return int(bpm) 
   else:
       return None
