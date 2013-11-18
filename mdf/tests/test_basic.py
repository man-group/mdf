import unittest
from mdf.builders.basic import CSVWriter, DataFrameBuilder
import sys

if sys.version_info[0] > 2:
    from io import StringIO
else:
    from cStringIO import StringIO

class BasicTest(unittest.TestCase):
    nodes = [1,2,3,4]
    labels = ['a','b',None,'d']
    no_lables = [None] * 4

    def test_create_csv_writer_with_nodes(self):
        csv_writer = CSVWriter(StringIO(), [1, 2, 3, 4])
        self.assertEqual(csv_writer.nodes, self.nodes)
        self.assertEqual(csv_writer.columns, self.no_lables)

    def test_create_dataframe_builder_with_nodes(self):
        dframe_builder = DataFrameBuilder([1, 2, 3, 4])
        self.assertEqual(dframe_builder.nodes, self.nodes)

    def test_create_csv_writer_with_node_label_pairs(self):
        # Mix lists and tuples to ensure that all work
        csv_writer = CSVWriter(StringIO(), [(1,'a'), [2,'b'], (3, None), [4, 'd']])
        self.assertEqual(csv_writer.nodes, self.nodes)
        self.assertEqual(csv_writer.columns, self.labels)
  