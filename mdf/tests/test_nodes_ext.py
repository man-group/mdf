from mdf import (
    MDFContext,
    varnode,
    evalnode,
    now
    )
from datetime import datetime, timedelta
import unittest
import logging

# this is necessary to stop namespace from looking
# too far up the stack as it looks for the first frame
# not in the mdf package
__package__ = None

_logger = logging.getLogger(__name__)

#
# the following nodes define this graph:
#
#               A       = B + C
#              / \
#  D * E =    B   C     = years since 1970
#            / \   \
#           D   E  now
#

D = varnode()
E = varnode()
F = varnode()

@evalnode
def A():
    return B() + C()

@evalnode
def B():
    return D() * E()

@evalnode
def C():
    accum = 0
    cur_year = 1970

    while True:
        next_year = now().year
        accum += next_year - cur_year
        yield accum
        cur_year = next_year

class NodeExtensionsTest(unittest.TestCase):

    def setUp(self):
        self.ctx = MDFContext(datetime.now())
        self.ctx[D] = 10
        self.ctx[E] = 20

    def test_node_names(self):
        self.assertEqual(A.short_name, "A")
        self.assertEqual(B.short_name, "B")
        self.assertEqual(C.short_name, "C")
        self.assertEqual(D.short_name, "D")
        self.assertEqual(E.short_name, "E")
        self.assertEqual(F.short_name, "F")

    def test_eval(self, ctx=None):
        ctx = ctx or self.ctx

        # get the calculated value A
        a = ctx[A]

        # calcualte the expected value
        b = ctx[D] * ctx[E]
        c = ctx[now].year - 1970
        expected_a = b + c

        self.assertEqual(a, expected_a)

    def test_update(self, ctx=None):
        ctx = ctx or self.ctx
    
        orig_b = ctx[B]
        ctx[D] *= 2
        self.assertEqual(orig_b * 2, ctx[B])

        b = ctx[D] * ctx[E]
        c = ctx[now].year - 1970
        expected_a = b + c

        self.assertEqual(ctx[A], expected_a)

    def test_clone(self):
        # get A before doing anything
        orig_a = self.ctx[A]

        # clone the context and run the eval and update checks
        new_date = self.ctx[now] + timedelta(days=365)
        shifted_ctx = self.ctx.shift({now : new_date})
        self.test_eval(shifted_ctx)

        # shifted contexts are read-only        
        self.assertRaises(AttributeError,
                          lambda: self.test_update(shifted_ctx))

        # check the original hasn't been affected
        self.assertEqual(orig_a, self.ctx[A])

    def test_shift(self):
        # get A before doing anything
        orig_a = self.ctx[A]

        # shift the context and run the eval check
        shifted_ctx = self.ctx.shift({E : self.ctx[E] * 2})
        self.assertEqual(shifted_ctx[E], self.ctx[E] * 2)
        self.test_eval(shifted_ctx)

        # check the original hasn't been affected
        self.assertEqual(orig_a, self.ctx[A])

    def test_set_evalnode(self):
        # setting an evalnode should fix its value
        self.ctx[A] = 100
        self.assertEqual(self.ctx[A], 100)

        # changing its dependencies shouldn't affect it
        self.ctx[B] *= 2
        self.ctx[C] *= 2
        self.assertEqual(self.ctx[A], 100)
