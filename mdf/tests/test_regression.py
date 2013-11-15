"""
Unit tests for regression testing
"""
import unittest
import os
import pandas as pa
from datetime import datetime
import mdf.regression
from mdf import evalnode

@evalnode
def pid_test():
    return os.getpid()

class RemoteTest(unittest.TestCase):

    def test_regression_contexts(self):
        """
        simple test that creates two subprocesses and checks the
        pids are different
        """ 
        lhs, rhs = mdf.regression.get_contexts(None, None)

        # test the pids for the two contexts are different
        lhs_pid = lhs.get_value(pid_test)
        rhs_pid = rhs.get_value(pid_test)
    
        self.assertNotEqual(lhs_pid, rhs_pid)
        
    def test_df_differ(self):
        """
        tests the DataFrameDiffer
        """
        date_range = pa.bdate_range(datetime.now(), periods=10)
        df_differ = mdf.regression.DataFrameDiffer([pid_test])

        diffs = mdf.regression.run(date_range, [df_differ], lhs=None, rhs=None)

        self.assertTrue(diffs[0][0])

