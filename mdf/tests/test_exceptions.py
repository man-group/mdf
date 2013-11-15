from mdf import MDFContext, varnode, evalnode, now
from datetime import datetime
import unittest

class TestException(Exception):
    pass

A = varnode()

@evalnode
def B():
    if A():
        return True
    raise TestException("A not set")

@evalnode
def C():
    if now() < datetime(2001, 1, 1):
        raise TestException("Prior to 2001")
    return True

class ExceptionTest(unittest.TestCase):
    def setUp(self):
        self.ctx = MDFContext()

    def test_exception_1(self):
        # A should raise an exception if A is not True
        self.ctx[A] = False
        
        self.assertRaises(TestException, self.ctx.get_value, B)

        # when called again the exception should be re-raised
        self.assertRaises(TestException, self.ctx.get_value, B)  

        # setting A should cause B to return True
        self.ctx[A] = True
        self.assertTrue(self.ctx[B])

    def test_exception_2(self):
        # C should raise an exception while 'now' is before 2001
        self.ctx.set_date(datetime(2000, 12, 31))
        self.assertRaises(TestException, self.ctx.get_value, C)

        self.ctx.set_date(datetime(2001, 1, 1))
        self.assertTrue(self.ctx[C])
