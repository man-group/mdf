from mdf import (
    MDFContext,
    varnode,
    evalnode,
    shift,
    now
)

from datetime import datetime, timedelta
from numpy.testing.utils import assert_almost_equal
import pandas as ps
import numpy as np
import unittest
import logging

# this is necessary to stop namespace from looking
# too far up the stack as it looks for the first frame
# not in the mdf package
__package__ = None
_logger = logging.getLogger(__name__)

B = varnode()
C = varnode()

num_calls_A = 0
num_calls_A_override = 0

@evalnode
def A():
    global num_calls_A
    num_calls_A += 1
    return B()

@evalnode
def A_override():
    global num_calls_A_override
    num_calls_A_override += 1
    return C()

@evalnode
def B_override():
    return C() * 3

@evalnode
def D():
    return A() * 2

@evalnode
def D_override():
    return A() + C() 

@evalnode
def shift_test():
    values = shift(D, A, [A_override])
    return values[0]

@evalnode
def shift_test2():
    # varnodes can be overridden with evalnodes
    values = shift(D, B, [B_override])
    return values[0]

class OverrideTest(unittest.TestCase):

    def setUp(self):
        self.ctx = MDFContext()

        global num_calls_A, num_calls_A_override
        num_calls_A = 0
        num_calls_A_override = 0

    def test_override(self):
        b = self.ctx[B] = 1
        c = self.ctx[C] = 2
        self.assertEqual(self.ctx[A], b)
        self.assertEqual(num_calls_A, 1)
        self.assertEqual(num_calls_A_override, 0)

        # override A
        self.ctx[A] = A_override
        self.assertEqual(self.ctx[A], c)
        self.assertEqual(num_calls_A, 1)
        self.assertEqual(num_calls_A_override, 1)

        # check the dependencies are working
        c = self.ctx[C] = 100
        self.assertEqual(self.ctx[A], c)
        self.assertEqual(num_calls_A, 1)
        self.assertEqual(num_calls_A_override, 2)

        # check A doesn't still depend on B
        b = self.ctx[B] = 500
        self.assertEqual(self.ctx[A], c)
        self.assertEqual(num_calls_A, 1)
        self.assertEqual(num_calls_A_override, 2)

    def test_shift_override(self):
        b = self.ctx[B] = 1
        c = self.ctx[C] = 2
        
        # D = A * 2
        # A = B
        self.assertEqual(self.ctx[D], b * 2)
        self.assertEqual(num_calls_A, 1)
        self.assertEqual(num_calls_A_override, 0)

        # shift_test = D [ A := A_override ]
        # A_override = C 
        self.assertEqual(self.ctx[shift_test], c * 2)
        self.assertEqual(num_calls_A, 1)
        self.assertEqual(num_calls_A_override, 1)        

    def test_shift_override2(self):
        c = self.ctx[C] = 10

        # shift_test = D [ B := B_override ]
        # D = A * 2
        # A = B
        # B_override = C * 3
        self.assertEqual(self.ctx[shift_test2], c * 6)      

    def test_shift_override3(self):
        # overriding a node in the root context and then
        # evaluating it in a shifted context should work
        self.ctx[D] = D_override # = A + C
        self.ctx[A] = 10
        self.ctx[C] = 20

        shifted_ctx = self.ctx.shift({A : 100})

        # - if D is used instead of D_override A will equal 2 * 100 = 200
        # - if D_override is used but in the root context the
        #   result will be A + C = 30
        # - if D_override is used and the correct context is used
        #   the results will be A + C = 100 + 20 = 120        
        self.assertEqual(shifted_ctx[D], 120)
