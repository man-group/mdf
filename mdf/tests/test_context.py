from datetime import datetime
from mdf import (
    MDFContext,
    varnode,
    nansumnode,
    evalnode,
    now,
    shift,
)
from numpy.testing.utils import assert_array_almost_equal
from pandas.core import datetools

import pandas as pd
import unittest

A = varnode()

@nansumnode
def B():
    return A() + now().year - 1970

@evalnode
def C():
    while True:
        yield shift(B, A, [1, 2, 3])

@evalnode
def D():
    while True:
        yield shift(B, shift_sets=[{A : 1}, {A : 2}, {A : 3}])

class ContextTest(unittest.TestCase):
    def setUp(self):
        self.daterange = pd.bdate_range(datetime(1970, 1, 1), periods=3, freq=datetools.yearEnd)
        self.ctx = MDFContext()

    def test_shift(self):
        # C yields a value based on shifted contexts
        res = []
        for t in self.daterange:
            self.ctx.set_date(t)
            res.append(self.ctx[C])

        assert_array_almost_equal(res, [(1,2,3), (3,5,7), (6,9,12)])

    def test_shift2(self):
        # C yields a value based on shifted contexts
        res = []
        for t in self.daterange:
            self.ctx.set_date(t)
            res.append(self.ctx[D])

        assert_array_almost_equal(res, [(1,2,3), (3,5,7), (6,9,12)])
