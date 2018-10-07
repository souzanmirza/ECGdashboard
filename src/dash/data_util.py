import sys
import os

sys.path.append('../python/')

import psycopg2
import helpers
import logging
import pandas as pd
import numpy as np

class DataUtil:
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

    def getLastestECGSamples(self, duration = 10):
        sqlcmd = "SELECT batchnum, signame, time, ecg1, ecg2, ecg3 \
                    FROM signal_samples WHERE time > (SELECT MAX(time) - interval '{} second' \
                    FROM signal_samples) \
                    ORDER BY signame;".format(duration)
        self.cur.execute(sqlcmd)
        df = pd.DataFrame(self.cur.fetchall(), columns=self.signal_schema)
        UniqueNames = df.signame.unique()
        DataFrameDict = {elem : pd.DataFrame for elem in UniqueNames}
        signames=df[self.signal_schema[1]].unique().tolist()
        for key in DataFrameDict.keys():
            DataFrameDict[key] = df[:][df.signame == key]
            DataFrameDict[key].sort_values('time', inplace=True)
        return DataFrameDict.keys(), DataFrameDict


    def getLastestHR(self):
        sqlcmd = "SELECT MAX(b.batchnum) as batchnum, b.signame, b.time, b.hr1, b.hr2, b.hr3 \
                  FROM inst_hr b \
                  INNER JOIN \
                  (SELECT signame, MAX(batchnum) as MaxBatch \
                  FROM inst_hr \
                  GROUP BY signame) a ON a.signame = b.signame AND a.MaxBatch = b.batchnum \
                  GROUP BY b.batchnum, b.signame, b.time, b.hr1, b.hr2, b.hr3;"
        self.cur.execute(sqlcmd)
        #print(self.cur.fetchall())
        df = pd.DataFrame(self.cur.fetchall(), columns=self.hr_schema)
        df.set_index(self.hr_schema[1], inplace=True)
        df.apply(lambda row: self.getAverageHR(row['hr1'], row['hr2'], row['hr3']), axis=1).tolist()
        return df


    def getTimestampBounds(self, df):
        maxTime = str(df[self.signal_schema[2]].max())
        minTime = str(df[self.signal_schema[2]].min())
        timestamps = [minTime] + ['.'] * df[self.signal_schema[2]].count() + [maxTime]


    def getAverageHR(self, hr1, hr2, hr3, time=None):
        heartrates = np.array([hr1, hr2, hr3])
        heartrates[np.where(heartrates==-1)]=0
        if time:
            return [time, int(np.average(heartrates))]
        return int(np.average(heartrates))

    def getHRSamples(self):
        sqlcmd = "SELECT batchnum, signame, time, hr1, hr2, hr3 \
                  FROM inst_hr \
                  ORDER BY signame;"
        self.cur.execute(sqlcmd)
        df = pd.DataFrame(self.cur.fetchall(), columns=self.hr_schema)
        UniqueNames = df.signame.unique()
        DataFrameDict = {elem: pd.DataFrame for elem in UniqueNames}
        AverageHRDict = {}
        signames = df[self.signal_schema[1]].unique().tolist()
        LastestHR = {}
        for key in DataFrameDict.keys():
            DataFrameDict[key] = df[:][df.signame == key]
            DataFrameDict[key].sort_values('time', inplace=True)
            AverageHRDict[key] = DataFrameDict[key].apply(lambda row: self.getAverageHR(row['hr1'], row['hr2'], row['hr3'], row['time']), axis=1).tolist()
            LastestHR[key] = DataFrameDict[key].tail(n=1).apply(lambda row: self.getAverageHR(row['hr1'], row['hr2'], row['hr3']), axis=1).tolist()
        return AverageHRDict.keys(), AverageHRDict, LastestHR


if __name__ == '__main__':
    import time
    postgres_config_infile = '../../.config/postgres.config'
    datautil = DataUtil(postgres_config_infile)
    while True:
        keys, DataFrameDict= datautil.getLastestECGSamples()
        keys, hrvar, latesthr  = datautil.getHRSamples()
        try:
            print(DataFrameDict['mghdata_ts/mgh001'])
        except:
            print('no mghdata_ts/mgh001')
            pass
        print(latesthr)
        time.sleep(0.2)