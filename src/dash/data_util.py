import sys
import os
import time
import logging
import pandas as pd
import numpy as np
import psycopg2
import helpers

sys.path.append('../python/')


class DataUtil:
    """
    Class to query from database for app streaming.
    """

    def __init__(self, postgres_config_infile):
        if not os.path.exists('./tmp'):
            os.makedirs('./tmp')
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(levelname)s %(message)s',
                            filename='./tmp/website.log',
                            filemode='w')
        self.logger = logging.getLogger('py4j')
        self.postgres_config = helpers.parse_config(postgres_config_infile)
        self.cur = self.connectToDB()
        self.signal_schema = ['batchnum', 'signame', 'time', 'ecg1', 'ecg2', 'ecg3']
        self.hr_schema = ['batchnum', 'signame', 'time', 'hr1', 'hr2', 'hr3']

    def connectToDB(self):
        """
        :return: database cursor
        """
        cur = None
        try:
            conn = psycopg2.connect(host=self.postgres_config['host'],
                                    database=self.postgres_config['database'],
                                    port=self.postgres_config['port'],
                                    user=self.postgres_config['user'],
                                    password=self.postgres_config['password'])
            cur = conn.cursor()
        except Exception as e:
            print(e)
        return cur

    def getLastestECGSamples(self, interval=10):
        """
        Queries signal_samples table to return the latest samples within the given interval.
        :param interval: time in seconds
        :return: dictionary of pandas dataframes containing latest samples within interval for each unique signame.
        """
        sqlcmd = "SELECT batchnum, signame, time, ecg1, ecg2, ecg3 \
                    FROM signal_samples WHERE time > (SELECT MAX(time) - interval '{} second' \
                    FROM signal_samples) \
                    ORDER BY signame;".format(interval)
        self.cur.execute(sqlcmd)
        df = pd.DataFrame(self.cur.fetchall(), columns=self.signal_schema)
        signames = df[self.signal_schema[1]].unique()
        signals_dict = {elem: pd.DataFrame for elem in signames}
        for key in signals_dict.keys():
            signals_dict[key] = df[:][df.signame == key]
            signals_dict[key].sort_values('time', inplace=True)
        return signals_dict.keys(), signals_dict

    def getLastestHR(self):
        """
        Queries inst_hr table to return the most recent heart rate measure for each signame.
        :return: pandas dataframe containing the average heart rate for each signal.
        """
        sqlcmd = "SELECT MAX(b.batchnum) as batchnum, b.signame, b.time, b.hr1, b.hr2, b.hr3 \
                  FROM inst_hr b \
                  INNER JOIN \
                  (SELECT signame, MAX(batchnum) as MaxBatch \
                  FROM inst_hr \
                  GROUP BY signame) a ON a.signame = b.signame AND a.MaxBatch = b.batchnum \
                  GROUP BY b.batchnum, b.signame, b.time, b.hr1, b.hr2, b.hr3;"
        self.cur.execute(sqlcmd)
        df = pd.DataFrame(self.cur.fetchall(), columns=self.hr_schema)
        df.set_index(self.hr_schema[1], inplace=True)
        df.apply(lambda row: self.getAverageHR(row['hr1'], row['hr2'], row['hr3']), axis=1).tolist()
        return df

    def getAverageHR(self, hr1, hr2, hr3, time=None):
        """
        Finds the average heart rate from the heart rate measured on all 3 leads.
        :param hr1, h2, h3: hr measured by lead I, II, III respectively
        :param time: timestamp for the measurement
        :return: list of time and average heart rate or if no time input then just list of average heart rate value.
        """
        heartrates = np.array([hr1, hr2, hr3])
        heartrates[np.where(heartrates == -1)] = 0
        if time:
            return [time, int(np.average(heartrates))]
        return int(np.average(heartrates))

    def getHRSamples(self):
        """
        Query inst_hr table to return HR samples from whole period
        :return: dictionary of pandas dataframes containing average HRs from all time,
        list of signames, dictionary containing most recent average HR measurement for each signame.
        """
        sqlcmd = "SELECT batchnum, signame, time, hr1, hr2, hr3 \
                  FROM inst_hr \
                  ORDER BY signame;"
        self.cur.execute(sqlcmd)
        df = pd.DataFrame(self.cur.fetchall(), columns=self.hr_schema)
        signames = df[self.hr_schema[1]].unique().tolist()
        hr_dict = {elem: pd.DataFrame for elem in signames}
        average_hr_dict = {}
        latest_hr = {}
        for key in hr_dict.keys():
            hr_dict[key] = df[:][df.signame == key]
            hr_dict[key].sort_values('time', inplace=True)
            average_hr_dict[key] = hr_dict[key].apply(
                lambda row: self.getAverageHR(row['hr1'], row['hr2'], row['hr3'], row['time']), axis=1).tolist()
            latest_hr[key] = hr_dict[key].tail(n=1).apply(
                lambda row: self.getAverageHR(row['hr1'], row['hr2'], row['hr3']), axis=1).tolist()
        return average_hr_dict.keys(), average_hr_dict, latest_hr


if __name__ == '__main__':
    # Test the output of the queries.
    postgres_config_infile = '../../.config/postgres.config'
    datautil = DataUtil(postgres_config_infile)
    while True:
        keys_ecg, signals_dict = datautil.getLastestECGSamples()
        keys, hrvar, latesthr = datautil.getHRSamples()
        for key in keys_ecg:
            print('ecg samples: ', key, len(signals_dict[key].index))
        print(latesthr)
        time.sleep(1)
