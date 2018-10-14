import sys
import unittest
import numpy as np

sys.path.append('../src/dash')
sys.path.append('../src/python')

from data_util import DataUtil

class dashDataUtilTestClass(unittest.TestCase):
    def test_getAverageHR(self):
        datautil = DataUtil('../.config/postgres.config')
        self.assertEqual(datautil.getAverageHR(0,0,0), 0)
        self.assertEqual(datautil.getAverageHR(0, -1, 0), 0)
        self.assertEqual(datautil.getAverageHR(45, -1, 45), 30)
        self.assertEqual(datautil.getAverageHR(45, 45, 45), 45)

if __name__ == '__main__':
    unittest.main()