"""
Tests for scenario analysis using multprocessing
"""
import unittest
from mdf.builders.basic import DataFrameBuilder
from mdf import MDFContext, run, varnode, evalnode, now
import pandas as pd
from datetime import datetime
import time

A = varnode()
B = varnode()

@evalnode
def X():
    return A() + B() + now().day


class RemoteTest(unittest.TestCase):

    def setUp(self):
        self.ctx = MDFContext()

    def test_multiprocess_run(self):
        # create a few shifts
        shifts = []
        for i in range(10):
            shifts.append({A: i, B: i * 2})

        date_range = pd.bdate_range(datetime(1970, 1, 1), periods=5)
        df_builder = DataFrameBuilder([X])
        num_processes = 4

        # process the scenarios in parallel
        start_time = time.clock()
        shifted_ctxs = run(date_range,
                           [df_builder],
                           shifts=shifts,
                           ctx=self.ctx,
                           num_processes=num_processes)
        end_time = time.clock()

        # do the same thing in a single process
        sync_df_builder = DataFrameBuilder([X])
        
        sync_start_time = time.clock()
        sync_shifted_ctxs = run(date_range, [sync_df_builder], shifts=shifts, ctx=self.ctx)
        sync_end_time = time.clock()

        # the contexts should be returned in the same order
        self.assertEquals(shifted_ctxs, sync_shifted_ctxs)

        for ctx in shifted_ctxs:
            df = df_builder.get_dataframe(ctx)
            sync_df = sync_df_builder.get_dataframe(ctx)
            assert df.equals(sync_df)

        print ("Took %fs using %d processes" % ((end_time - start_time), num_processes))
        print ("Took %fs using 1 process" % (sync_end_time - sync_start_time))
