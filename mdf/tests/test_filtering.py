"""
Test the various nodetypes all work with a filter.
"""
from mdf import (
    MDFContext,
    varnode,
    evalnode,
    now
)
import mdf
from datetime import datetime, timedelta
import unittest
import logging
import pandas as pa
import numpy as np

# this is necessary to stop namespace from looking
# too far up the stack as it looks for the first frame
# not in the mdf package
__package__ = None

_logger = logging.getLogger(__name__)

index = pa.Index([datetime(1991, 12, 31),
                  datetime(1992, 1, 1),
                  datetime(1992, 1, 2),
                  datetime(1992, 1, 3),
                  datetime(1992, 1, 6),
                  datetime(1992, 1, 7),
                  datetime(1992, 1, 8),
                  datetime(1992, 1, 9),
                  datetime(1992, 1, 10),
                  datetime(1992, 1, 13)])

data = pa.Series([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], index=index, dtype=float)
datanode = mdf.datanode("data", data)

df = pa.DataFrame({"A" : [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                   "B" : [np.nan, 9, 8, np.nan, 6, 5, 4, 3, 2, 1]},
                        index=index, dtype=float)
dfnode = mdf.datanode("df", df)

filter = pa.Series([False, True, False, True, False,
                    True, False, True, False, True], index=index)
filternode = mdf.datanode("filter", filter)

queue_expected = [data[data.index <= d].tolist() for d in index]
queue_filtered_expected = [data[filter][data[filter].index <= d].tolist() for d in index]

delay_expected = data.shift(1)
delay_filtered_expected = data[filter].shift(1).reindex(index, method="ffill")

delay_df_expected = df.shift(1)
delay_df_filtered_expected = df[filter].shift(1).reindex(index, method="ffill")

nansum_expected = data.cumsum(skipna=True).ffill()
nansum_filtered_expected = data[filter].cumsum(skipna=True).reindex(index).ffill()

nansum_df_expected = df.cumsum(skipna=True).ffill()
nansum_df_filtered_expected = df[filter].cumsum(skipna=True).reindex(index).ffill()

cumprod_expected = data.cumprod().ffill()
cumprod_filtered_expected = data[filter].cumprod().reindex(index).ffill()

cumprod_df_expected = df.cumprod().ffill()
cumprod_df_filtered_expected = df[filter].cumprod().reindex(index).ffill()


def _normalize(x):
    """try and get a vector/dataframe in a format unittest can compare them"""
    if isinstance(x, float):
        if np.isnan(x):
            return -np.inf
        return x

    if isinstance(x, (pa.DataFrame, pa.Series)):
        x = x.values

    if x.ndim == 1:
        return tuple([_normalize(y) for y in x])

    r = x.copy()
    r[np.isnan(r)] = -np.inf
    return tuple(r.tolist())

class NodeFilterTest(unittest.TestCase):

    def setUp(self):
        self.ctx = MDFContext(index[0])

    def test_queue(self):
        actual = self._run(datanode.queuenode(as_list=True))
        self.assertEqual(actual.tolist(), queue_expected)

        actual = self._run(datanode.queuenode(filter=filternode, as_list=True))
        self.assertEqual(actual.tolist(), queue_filtered_expected)

    def test_delay(self):
        actual = self._run(datanode.delaynode(periods=1, initial_value=np.nan))
        self.assertEqual(_normalize(actual), _normalize(delay_expected))

        actual = self._run(datanode.delaynode(periods=1, initial_value=np.nan, filter=filternode))
        self.assertEqual(_normalize(actual), _normalize(delay_filtered_expected))

    def test_delay_df(self):
        actual = self._run(dfnode.delaynode(periods=1, initial_value=np.nan))
        self.assertEqual(_normalize(actual), _normalize(delay_df_expected))

        actual = self._run(dfnode.delaynode(periods=1, initial_value=np.nan, filter=filternode))
        self.assertEqual(_normalize(actual), _normalize(delay_df_filtered_expected))

    def test_nansum(self):
        actual = self._run(datanode.nansumnode())
        self.assertEqual(_normalize(actual), _normalize(nansum_expected))

        actual = self._run(datanode.nansumnode(filter=filternode))
        self.assertEqual(_normalize(actual), _normalize(nansum_filtered_expected))

    def test_nansum_df(self):
        actual = self._run(dfnode.nansumnode())
        self.assertEqual(_normalize(actual), _normalize(nansum_df_expected))

        actual = self._run(dfnode.nansumnode(filter=filternode))
        self.assertEqual(_normalize(actual), _normalize(nansum_df_filtered_expected))

    def test_cumprod(self):
        actual = self._run(datanode.cumprodnode())
        self.assertEqual(_normalize(actual), _normalize(cumprod_expected))

        actual = self._run(datanode.cumprodnode(filter=filternode))
        self.assertEqual(_normalize(actual), _normalize(cumprod_filtered_expected))

    def test_cumprod_df(self):
        actual = self._run(dfnode.cumprodnode())
        self.assertEqual(_normalize(actual), _normalize(cumprod_df_expected))

        actual = self._run(dfnode.cumprodnode(filter=filternode))
        self.assertEqual(_normalize(actual), _normalize(cumprod_df_filtered_expected))

    def _run(self, node):
        result = []
        for t in index:
            self.ctx.set_date(t)
            result.append(self.ctx[node])
        if isinstance(result[0], pa.Series):
            return pa.DataFrame(result, index=index)
        return pa.Series(result, index, dtype=object)
