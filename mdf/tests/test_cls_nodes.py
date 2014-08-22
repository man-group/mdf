from mdf import (
    MDFContext,
    varnode,
    evalnode,
    queuenode,
    nansumnode,
    cumprodnode,
    filternode,
    datanode,
)

from datetime import datetime
from numpy.testing.utils import assert_almost_equal
import pandas as pd
import numpy as np
import unittest
import logging

# this is necessary to stop namespace from looking
# too far up the stack as it looks for the first frame
# not in the mdf package

__package__ = None

_logger = logging.getLogger(__name__)

class _TestNodes(object):
    C = varnode()
    D = varnode()

    @queuenode
    def queue_output(cls):
        return cls.A() + cls.B()

    @nansumnode
    def nansum_output(cls):
        return cls.A() + cls.sometimes_nan_B()

    @cumprodnode
    def cumprod_output(cls):
        return cls.A() + cls.B()

    @evalnode
    def sometimes_nan_B(cls):
        b = cls.B()
        return np.nan if b % 2 else b

    @evalnode
    def A(cls):
        return cls.C() * cls.D()

    @evalnode
    def B(cls):
        accum = 0

        while True:
            yield accum
            accum += 1

    @queuenode(size=2)
    def queue_size_test(cls):
        return 0

    #
    # check that evalnodes passed as arguments to nodes get
    # bound correctly
    #
    _queue_size = 3
    @evalnode
    def get_queue_size(cls):
        return cls._queue_size

    @queuenode(size=get_queue_size)
    def queue_size_test2(cls):
        return 0

    @evalnode
    def dataframe(cls):
        return pd.DataFrame({"A": range(100)},
                            index=pd.bdate_range(datetime(1970, 1, 1), periods=100),
                            dtype=float)

    df_data = datanode(data=dataframe)
    df_filter = filternode(data=dataframe)


class SubClass(_TestNodes):
    @evalnode
    def A(cls):
        return cls.C() + cls.D()

    _queue_size = 4


class ClsNodeTest(unittest.TestCase):

    def setUp(self):
        self.daterange = pd.bdate_range(datetime(1970, 1, 1), datetime(1970, 1, 10))
        self.ctx = MDFContext()
        self.ctx[_TestNodes.C] = 10
        self.ctx[_TestNodes.D] = 20

    def test_queuenode(self):
        self._run(_TestNodes.queue_output)
        queue = self.ctx[_TestNodes.queue_output]
        self.assertEqual(len(queue), len(self.daterange))

    def test_nansumnode(self):
        self._run(_TestNodes.nansum_output)
        nansum = self.ctx[_TestNodes.nansum_output]
        self.assertEqual(nansum, 812)

    def test_cumprodnode(self):
        self._run(_TestNodes.cumprod_output)
        cumprod = self.ctx[_TestNodes.cumprod_output]
        self.assertEqual(cumprod, 14201189062704000)

    def test_subclass(self):
        base_a = self.ctx[_TestNodes.A]
        sub_a = self.ctx[SubClass.A]
        self.assertEqual(base_a, 200)
        self.assertEqual(sub_a, 30)

        self._run(_TestNodes.cumprod_output, SubClass.cumprod_output)
        base_cumprod = self.ctx[_TestNodes.cumprod_output]
        sub_cumprod = self.ctx[SubClass.cumprod_output]
        self.assertEqual(base_cumprod, 14201189062704000)
        self.assertEqual(sub_cumprod, 42072307200)

    def test_nodenames(self):
        self.assertNotEqual(SubClass.A.name, _TestNodes.A.name)

    def test_queue_size(self):
        self._run(_TestNodes.queue_size_test,  # check basic queue size
                  _TestNodes.queue_size_test2, # check evalnode queue size
                  SubClass.queue_size_test2)  # check overriding of queue size
        
        self.assertEqual(len(self.ctx[_TestNodes.queue_size_test]), 2)
        self.assertEqual(len(self.ctx[_TestNodes.queue_size_test2]), 3)
        self.assertEqual(len(self.ctx[SubClass.queue_size_test2]), 4)

    def test_datanode(self):
        self._run(_TestNodes.df_data, _TestNodes.df_filter)

        self.assertEqual(self.ctx[_TestNodes.df_data]["A"], len(self.daterange) - 1)
        self.assertTrue(self.ctx[_TestNodes.df_filter])

        self.ctx.set_date(datetime(1990, 1, 1))
        self.assertFalse(self.ctx[_TestNodes.df_filter])

    def _run(self, *nodes):
        for t in self.daterange:
            self.ctx.set_date(t)
            for node in nodes:
                self.ctx[node]
