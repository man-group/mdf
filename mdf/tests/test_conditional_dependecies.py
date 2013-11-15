import unittest
from ..nodes import ConditionalDependencyError
from datetime import datetime, timedelta
from mdf import (
    MDFContext,
    varnode,
    evalnode,
    now, 
    shift
    )

# this is necessary to stop namespace from looking
# too far up the stack as it looks for the first frame
# not in the mdf package
__package__ = None

x = varnode()
y = varnode()
z = varnode()
s = varnode()

@evalnode
def wrap_y():
    return y() ** 2

@evalnode
def deeper_dependency_node():   
    yield x() 
    while True:
        yield wrap_y()

@evalnode
def simple_dependency_node():
    yield x()
    while True:
        yield y()**2
    
@evalnode
def shift_simple():
    return shift(simple_dependency_node,y,[s])[0]

@evalnode
def shift_deeper():
    return shift(deeper_dependency_node,y,[s])[0]

class ConditionalDependencyTests(unittest.TestCase):
    def setUp(self):
        self.ctx = MDFContext(datetime.now())
        self.ctx[x] = 1
        self.ctx[y] = 2
        self.ctx[z] = 3
    
    def test_no_shift(self, ctx=None):
        ctx = ctx or self.ctx
        
        value = ctx[simple_dependency_node]
        
        self.assertEqual(value,1)
        
    def test_shift_raises(self, ctx=None):
        ctx = ctx or self.ctx
        ctx[s] = [1,2,3,4] 
        #all fine the first time   
        value = ctx[shift_simple]  
        self.assertEqual(value, 1)
        value = ctx[shift_deeper] 
        self.assertEqual(value, 1)
        
        #advance the simulation time
        ctx.set_date(ctx[now] + timedelta(seconds=1))        
        #y is now detected as a dependency and we have an error
        self.assertRaises(ConditionalDependencyError,ctx.get_value,shift_simple)    
        self.assertRaises(ConditionalDependencyError,ctx.get_value,shift_deeper)