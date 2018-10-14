import sys
import unittest
import numpy as np
from pyspark import SparkContext

sys.path.append('../src/spark')
sys.path.append('../src/python')

import spark_helpers

class sparkHelpersTestClass(unittest.TestCase):
    def test_accum(self):
        sc = SparkContext(appName='PythonStreamingDirectKafkaWordCount')
        a = sc.accumulator(0)
        self.assertEqual(spark_helpers.accum(a), 1)
        sc.stop()

    def test_findHR(self):
        ecg = np.zeros((100, 1))
        fs=100
        self.assertEqual(spark_helpers.findHR(ecg, fs),-1)
        ecg = ecg*0.1
        fs=100
        self.assertEqual(spark_helpers.findHR(ecg, fs),-1)

if __name__ == '__main__':
    unittest.main()



