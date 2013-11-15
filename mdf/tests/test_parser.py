"""
Unit tests for the pylab magic function parser
"""
import unittest
from mdf.parser import tokenize

class PylabParserTest(unittest.TestCase):

    def test_simple_parse(self):
        test = "2001-01-01 T node_a node_b"
        expected = ["2001-01-01", "T", "node_a", "node_b"]
        actual = tokenize(test)
        self.assertEqual(actual, expected)

    def test_func(self):
        test = "2001-01-01 T node_a node_b.queuenode(a, size=c)"
        expected = ["2001-01-01", "T", "node_a", "node_b.queuenode(a, size=c)"]
        actual = tokenize(test)
        self.assertEqual(actual, expected)

    def test_index(self):
        test = "2001-01-01 T nodes[0], nodes[1]"
        expected = ["2001-01-01", "T", "nodes[0]", "nodes[1]"]
        actual = tokenize(test)
        self.assertEqual(actual, expected)
        
    def test_index_func(self):
        test = "2001-01-01 T nodes[0], nodes[1].delaynode(periods=1)"
        expected = ["2001-01-01", "T", "nodes[0]", "nodes[1].delaynode(periods=1)"]
        actual = tokenize(test)
        self.assertEqual(actual, expected)

    def test_func_index(self):
        test = "2001-01-01 T nodes[0], get_nodes()[0].delaynode(periods=1)"
        expected = ["2001-01-01", "T", "nodes[0]", "get_nodes()[0].delaynode(periods=1)"]
        actual = tokenize(test)
        self.assertEqual(actual, expected)

    def test_dict(self):
        test = "2001-01-01 T nodes[0], nodes[1] {a=foo(), b=hello[0], c=100}"
        expected = ["2001-01-01", "T", "nodes[0]", "nodes[1]", "{a=foo(), b=hello[0], c=100}"]
        actual = tokenize(test)
        self.assertEqual(actual, expected)

    def test_list(self):
        test = "2001-01-01 T nodes[0], nodes[1] [a, b, c]"
        expected = ["2001-01-01", "T", "nodes[0]", "nodes[1]", "[a, b, c]"]
        actual = tokenize(test)
        self.assertEqual(actual, expected)
        
    def test_multiproc(self):
        test = "2001-01-01 T nodes[0], nodes[1] [a, b, c] || 2"
        expected = ["2001-01-01", "T", "nodes[0]", "nodes[1]", "[a, b, c]", "|| 2"]
        actual = tokenize(test)
        self.assertEqual(actual, expected)
