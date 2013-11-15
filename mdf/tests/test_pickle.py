from mdf import (
    MDFContext,
    varnode,
    evalnode,
    queuenode,
    shift,
    now
)
import numpy as np
import pandas as pa
import unittest
import logging
import tempfile
import shutil
import os
import random
import pickle

A = varnode()

_b_num_calls = 0

@evalnode
def B():
    global _b_num_calls
    _b_num_calls += 1
    return A() * 2

@queuenode(size=5)
def C():
    return B()

# this is necessary to stop namespace from looking
# too far up the stack as it looks for the first frame
# not in the mdf package
__package__ = None
_logger = logging.getLogger(__name__)

class PickleTest(unittest.TestCase):

    def setUp(self):
        self.ctx = MDFContext()

    def test_pickle(self):
        a = self.ctx[A] = 10
        b = self.ctx[B]
        c = self.ctx[C]

        num_calls = _b_num_calls

        x = pickle.dumps(self.ctx)
        new_ctx = pickle.loads(x)

        self.assertEquals(new_ctx[A], a)
        self.assertEquals(new_ctx[B], b)
        self.assertEquals(new_ctx[C], c)

        # unpickling the context shouldn't re-evaluate anything
        self.assertEquals(num_calls, _b_num_calls)

    def test_node_method_pickle(self):
        # get another node via a nodetype method
        # use nan in the args as there were problems pickling/unpicking nan
        node = A.samplenode(initial_value=np.nan, offset=pa.datetools.BMonthEnd())
        
        # test text and binary pickle formats
        for protocol in (0, pickle.HIGHEST_PROTOCOL):
            x = pickle.dumps(node, protocol)
            new_node = pickle.loads(x)
    
            # unpickling a node constructed via a notetype method call should retrieve
            # the same node instance 
            self.assertTrue(node is new_node)

    def test_shifted_pickle(self):
        shifted_ctx = self.ctx.shift({A : 100})
        a = shifted_ctx[A]
        b = shifted_ctx[B]
        c = shifted_ctx[C]
        num_calls = _b_num_calls

        x = pickle.dumps(self.ctx)
        new_ctx = pickle.loads(x)

        # pickling preserves shifted contexts so this should return
        # an existing shifted context
        new_shifted_ctx = new_ctx.shift({A : a})
        self.assertNotEquals(new_shifted_ctx, shifted_ctx) 

        self.assertEquals(new_shifted_ctx[A], a)
        self.assertEquals(new_shifted_ctx[B], b)
        self.assertEquals(new_shifted_ctx[C], c)

        # unpickling the context shouldn't re-evaluate anything
        self.assertEquals(num_calls, _b_num_calls)

    def test_dynamic_varnode_pickle(self):
        # shift B by a temporary varnode with a value set in the root context
        temp_varnode = varnode(default="temp_varnode_default")
        shifted_ctx = self.ctx.shift({B : temp_varnode})
        self.ctx[temp_varnode] = "temp_varnode"
        self.assertEquals(shifted_ctx[B], "temp_varnode")

        # pickle the context, unregister the node and recreate the context
        x = pickle.dumps(self.ctx)
        MDFContext.unregister_node(temp_varnode)
        new_ctx = pickle.loads(x)

        # find the shifted context (can't reshift as temp_varnode was unregistered)
        self.assertEquals(len(new_ctx.get_shifted_contexts()), 1)
        shifted_ctx = new_ctx.get_shifted_contexts()[0]

        # check that B in the shifted context is still set
        self.assertEquals(shifted_ctx[B], "temp_varnode")
        
        # check that temp_varnode refers to the original node, not the new one
        # that had to be recreated when un-pickled.
        self.assertEquals(shifted_ctx[temp_varnode], "temp_varnode_default")

    def test_save(self):
        tmpdir = tempfile.mkdtemp()
        try:
            for ext in (".dag", ".zip", ".bz2", ".gz"):
                filename = os.path.join(tmpdir, "ctx" + ext)

                a = self.ctx[A] = random.randint(0, 100)
                self.assertEquals(self.ctx[A], a)

                self.ctx.save(filename)
                _logger.info("context file '%s' is %db" % (os.path.basename(filename),
                                                           os.stat(filename).st_size))
                new_ctx = MDFContext.load(filename)
    
                self.assertEquals(new_ctx[A], a)

                os.unlink(filename)
        finally:
            shutil.rmtree(tmpdir, True)
