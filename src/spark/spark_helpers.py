import sys
import os

sys.path.append('../../python/')

import psycopg2
import psycopg2.extras as extras
from datetime import datetime
import biosppy
import numpy as np

def accum(a):
    a.add(1)
    return a.value


def insert_ecg_samples(logger, postgres_config, a, record):
    logger.warn('fxn insert_samples')

    def _insert_ecg_samples(sqlcmd1, sqlcmd2, signals):
        logger.warn('fxn _insert_samples')
        for signal in signals:
            _sqlcmd1 = sqlcmd1.format(a, signal[0])
            try:
                conn = psycopg2.connect(host=postgres_config['host'],
                                        database=postgres_config['database'],
                                        port=postgres_config['port'],
                                        user=postgres_config['user'],
                                        password=postgres_config['password'])
                cur = conn.cursor()
                print(_sqlcmd1)
                logger.warn(_sqlcmd1)
                cur.execute(_sqlcmd1)
                extras.execute_batch(cur, sqlcmd2, signal[1])
                cur.execute("DEALLOCATE inserts")
                conn.commit()
                cur.close()
                conn.close()
            except Exception as e:
                logger.warn('Exception %s' % e)

    sqlcmd1 = "PREPARE inserts AS INSERT INTO signal_samples(batchnum, signame, time, ecg1, ecg2, ecg3) VALUES ({}, '{}', $1, $2, $3, $4) ON CONFLICT DO NOTHING;"
    sqlcmd2 = "EXECUTE inserts (%s, %s, %s, %s);"
    record.foreachPartition(lambda x: _insert_ecg_samples(sqlcmd1, sqlcmd2, list(x)))

def findHR(ecg, fs, threshold=0.2):
    if max(ecg) < threshold:
        return -1
    try:
        output = biosppy.signals.ecg.ecg(ecg, sampling_rate=fs, show=False).as_dict()
        filtered = output['filtered']
        filtered = filtered[np.where(abs(filtered) > 0.05)]
        rpeaks, = biosppy.signals.ecg.hamilton_segmenter(signal=filtered, sampling_rate=fs)

        # correct R-peak locations
        rpeaks, = biosppy.signals.ecg.correct_rpeaks(signal=filtered,
                                 rpeaks=rpeaks,
                                 sampling_rate=fs,
                                 tol=0.05)

        # extract templates
        templates, rpeaks = biosppy.signals.ecg.extract_heartbeats(signal=filtered,
                                               rpeaks=rpeaks,
                                               sampling_rate=fs,
                                               before=0.2,
                                               after=0.4)

        # compute heart rate
        hr_idx, hr = biosppy.signals.tools.get_heart_rate(beats=rpeaks,
                                       sampling_rate=fs,
                                       smooth=True,
                                       size=3)

        average_hr = np.average(hr)
        output = {'filtered': filtered, 'rpeaks': rpeaks}
        if average_hr > 0:
            return average_hr
        else:
            return -1
    except Exception as e:
        return -1

def process_hr_sample(logger, postgres_config, a, fs, record):
    logger.warn('fxn insert_sample')

    def _insert_hr_sample(sqlcmd1, sqlcmd2, signals):
        logger.warn('fxn _insert_sample')
        for signal in signals:
            _sqlcmd1 = sqlcmd1.format(a, signal[0], datetime.now())
            try:
                conn = psycopg2.connect(host=postgres_config['host'],
                                        database=postgres_config['database'],
                                        port=postgres_config['port'],
                                        user=postgres_config['user'],
                                        password=postgres_config['password'])
                cur = conn.cursor()
                logger.warn(_sqlcmd1)
                cur.execute(_sqlcmd1)
                extras.execute_batch(cur, sqlcmd2, signal[1])
                cur.execute("DEALLOCATE inserts")
                conn.commit()
                cur.close()
                conn.close()
            except Exception as e:
                logger.warn('Exception %s' % e)

    def _calculateHR(signals):
        logger.warn('fxn _calculateHR')
        signals_HR = []
        for x in signals:
            signame = str(x[0])
            signal = np.array(x[1])
            signal[np.argsort(signal[:, 0])]
            ts_str = signal[:, 0]
            print('fs is', fs)
            if len(ts_str) > 3:
                ecg1 = np.array(signal[:, 1]).astype(float)
                ecg2 = np.array(signal[:, 2]).astype(float)
                ecg3 = np.array(signal[:, 3]).astype(float)
                logger.warn("calling findhr")
                sampleHR = (signame, [[findHR(ecg1, fs), findHR(ecg2, fs), findHR(ecg3, fs)]])
                print(sampleHR)
                signals_HR.append(sampleHR)
        else:
            logger.debug('No HR returned')

        sqlcmd3 = "PREPARE inserts AS INSERT INTO inst_hr(batchnum, signame, time, hr1, hr2, hr3) VALUES ({}, '{}', '{}', $1, $2, $3) ON CONFLICT DO NOTHING;"
        sqlcmd4 = "EXECUTE inserts (%s, %s, %s)"
        _insert_hr_sample(sqlcmd3, sqlcmd4, signals_HR)
    record.foreachPartition(_calculateHR)
