from mdf import (
    MDFContext,
    evalnode,
    queuenode,
    nansumnode,
    cumprodnode,
    delaynode,
    ffillnode,
    vargroup,
    datanode,
    run,
    DataFrameBuilder
)

from datetime import datetime
from numpy.testing.utils import assert_almost_equal
import pandas as pd
import numpy as np
import unittest
import logging
import operator

# this is necessary to stop namespace from looking
# too far up the stack as it looks for the first frame
# not in the mdf package

__package__ = None

_logger = logging.getLogger(__name__)

params = vargroup(C=None, D=None)

@queuenode
def queue_output():
    return A() + B()

@queuenode
def queue_yield():
    while True:
        yield A() + B()

@evalnode
def queue_filter():
    yield False
    while True:
        yield True

@queuenode(filter=queue_filter)
def queue_filter_test():
    yield "THIS SHOULD NOT BE IN THE QUEUE"
    while True:
        yield 0

@nansumnode()
def nansum_output():
    return A() + sometimes_nan_B()

@cumprodnode()
def cumprod_output():
    return A() + B()

@evalnode
def sometimes_nan_B():
    b = B()
    return np.nan if b % 2 else b

@evalnode
def A():
    return params.C() * params.D()

@evalnode
def B():
    accum = 0

    while True:
        yield accum
        accum += 1

@evalnode
def A_plus_B():
    return A() + B()

@evalnode
def Counter():
    accum = -2.0
    while True:
        if accum != 0.0:
            yield accum
        accum += 0.5

class DelayNodeTest(object): 

    @evalnode
    def initial_value(cls):
        return 0

    @delaynode(periods=1, initial_value=initial_value)
    def delayed_node(cls):
        i = 1
        while True:
            yield i
            i += 1

    @delaynode(periods=1, initial_value=initial_value, lazy=True)
    def delayed_node_lazy(cls):
        return cls.delay_test()[-1]

    @queuenode
    def delay_test(cls):
        return 1 + cls.delayed_node()

    @queuenode
    def delay_test_lazy(cls):
        return 1 + cls.delayed_node_lazy()

@queuenode
def ffill_queue():
    return ffill_test()

@ffillnode
def ffill_test():
    i = 0
    while True:
        yield np.nan if i % 2 else float(i)
        i += 1

@ffillnode
def ffill_array_test():
    return ffill_array_test_not_filled()

@evalnode
def ffill_array_test_not_filled():
    i = 0
    array = np.ndarray((5,), dtype=float)
    array.fill(10.0)
    yield array
    
    array.fill(np.nan)
    while True:
        yield array

class NodeTest(unittest.TestCase):

    def setUp(self):
        self.daterange = pd.bdate_range(datetime(1970, 1, 1), datetime(1970, 1, 10))
        self.ctx = MDFContext()
        self.ctx[params.C] = 10
        self.ctx[params.D] = 20

    def test_queuenode(self):
        self._run(queue_output)
        queue = self.ctx[queue_output]
        self.assertEqual(len(queue), len(self.daterange))

    def test_queueyield(self):
        self._run(queue_yield)
        queue = self.ctx[queue_yield]
        self.assertEqual(len(queue), len(self.daterange))

    def test_queue_filter(self):
        self._run(queue_filter_test)
        queue = self.ctx[queue_filter_test]
        self.assertEqual(list(queue), [0] * (len(self.daterange) - 1))
        
    @staticmethod
    def diff_dfs(lhs_df, rhs_df, tolerance):
        diffs = np.abs(lhs_df - rhs_df)
        mask = (diffs > tolerance).values
        mask &= ~(np.isnan(lhs_df) and np.isnan(rhs_df)).values
        mask |= np.isnan(lhs_df).values & ~np.isnan(rhs_df).values
        mask |= np.isnan(rhs_df).values & ~np.isnan(lhs_df).values
        return mask.any()

    def test_nansumnode(self):
        self._run(nansum_output)
        nansum = self.ctx[nansum_output]
        self.assertEqual(nansum, 812)

    def test_cumprodnode(self):
        self._run(cumprod_output)
        cumprod = self.ctx[cumprod_output]
        self.assertEqual(cumprod, 14201189062704000)

    def test_delaynode(self):
        self._run(DelayNodeTest.delay_test, DelayNodeTest.delay_test_lazy)
        value =  self.ctx[DelayNodeTest.delay_test]
        value_lazy =  self.ctx[DelayNodeTest.delay_test_lazy]
        self.assertEqual(list(value), list(range(1, len(self.daterange)+1)))
        self.assertEqual(list(value_lazy), list(range(1, len(self.daterange)+1)))

    def test_ffillnode(self):
        self._run(ffill_queue)
        value =  self.ctx[ffill_queue]
        self.assertEqual(tuple(value), (0.0, 0.0, 2.0, 2.0, 4.0, 4.0, 6.0))

    def test_ffill_array(self):
        self._run(ffill_array_test)
        value =  self.ctx[ffill_array_test]
        unfilled_value = self.ctx[ffill_array_test_not_filled]
        self.assertTrue(np.isnan(unfilled_value).all())
        self.assertEquals(value.tolist(), [10., 10., 10., 10., 10.])

    def test_datanode_ffill(self):
        data = pd.Series(range(len(self.daterange)), self.daterange, dtype=float)
        data = data[[bool(i % 2) for i in range(len(data.index))]]

        expected = data.reindex(self.daterange, method="ffill")
        expected[np.isnan(expected)] = np.inf

        node = datanode("test_datanode_ffill", data, ffill=True, missing_value=np.inf)
        qnode = node.queuenode()

        self._run(qnode)
        value = self.ctx[qnode]

        self.assertEquals(list(value), expected.values.tolist())

    def test_lookahead_node(self):
        B_queue = B.queuenode()
        B_lookahead = B.lookaheadnode(periods=len(self.daterange))

        self.ctx.set_date(self.daterange[0])
        actual = self.ctx[B_lookahead]

        self._run(B_queue)
        expected = self.ctx[B_queue]

        self.assertEquals(actual.values.tolist(), list(expected))
        self.assertEquals(actual.index.tolist(), list(self.daterange))
    
    def test_apply_node(self):
        actual_node = A.applynode(func=operator.add, args=(B,)).queuenode()
        expected_node = A_plus_B.queuenode()

        self._run(actual_node, expected_node)
        actual = self.ctx[actual_node]
        expected = self.ctx[expected_node]

        self.assertEquals(actual, expected)

    def test_binary_operators_with_constant(self):
        self._test(Counter,            [-2.0, -1.5, -1.0, -0.5,  0.5,  1.0,  1.5])
        self._test(Counter + 0.2,      [-1.8, -1.3, -0.8, -0.3,  0.7,  1.2,  1.7])
        self._test(Counter - 0.2,      [-2.2, -1.7, -1.2, -0.7,  0.3,  0.8,  1.3])
        self._test(Counter * 2.0,      [-4.0, -3.0, -2.0, -1.0,  1.0,  2.0,  3.0])
        self._test(Counter / 0.5,      [-4.0, -3.0, -2.0, -1.0,  1.0,  2.0,  3.0])

        self._test(0.2 + Counter,      [-1.8, -1.3, -0.8, -0.3,  0.7,  1.2,  1.7])
        self._test(1.0 - Counter,      [ 3.0,  2.5,  2.0,  1.5,  0.5,  0.0, -0.5])
        self._test(2.0 * Counter,      [-4.0, -3.0, -2.0, -1.0,  1.0,  2.0,  3.0])
        self._test(12 / (Counter+.25), [-6.8571428571428568, -9.6000000000000014, -16.0,
                                        -48.0, 16.0, 9.6000000000000014, 6.8571428571428568])

    def test_binary_operators_with_node(self):
        self._test(Counter + Counter,  [-4.0, -3.0, -2.0, -1.0,  1.0,  2.0,  3.0])
        self._test(Counter - Counter,  [ 0.0,  0.0,  0.0,  0.0,  0.0,  0.0,  0.0])
        self._test(Counter * Counter,  [ 4.0,  2.25, 1.0,  0.25, 0.25, 1.0,  2.25])
        self._test(Counter / Counter,  [ 1.0,  1.0,  1.0,  1.0,  1.0,  1.0,  1.0])
        
    def _test(self, node, expected_values):
        values = node.queuenode()
        self._run(values)
        actual = self.ctx[values]
        self.assertEquals(list(actual), expected_values)
        
    def _run_for_daterange(self, date_range, *nodes):
        for t in date_range:
            self.ctx.set_date(t)
            for node in nodes:
                self.ctx[node] 
                
    def _run(self, *nodes):
        self._run_for_daterange(self.daterange, *nodes)


