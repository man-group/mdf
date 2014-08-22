"""
Classes for collecting data and determining differences,
for use with mdf.regression.run.
"""
import sys
import os
from ..builders import DataFrameBuilder
from ..nodes import MDFNode
import numpy as np
import pandas as pa
import xlwt
import logging

from datetime import datetime

_log = logging.getLogger(__name__)

if sys.version_info[0] > 2:
    basestring = str

def _to_range(row, col, sheet=None):
    """returns an Excel range string, e.g. 0, 0 => A1"""
    cell = ""
    while col >= 26:
        cell = "%s%s" % (chr(ord("A") + (col % 26)), cell)
        col = (col // 26) - 1
    cell = "%s%s" % (chr(ord("A") + (col % 26)), cell) 
    cell += "%d" % (row + 1)
    if sheet is not None:
        cell = "%s!%s" % (sheet.name, cell)
    return cell

class Differ(object):
    """
    Differ objects are the same a mdf builders with the
    addition that they have the ability to diff result
    sets.
    
    When mdf.regression.run is called, each differ
    object is called for each date in the regression
    with the context being evaluated.
    
    When finished, lhs.diff(rhs, lhs_ctx, rhs_ctx) is
    called to diff the data collected in the regression
    differ object.
    """

    def diff(self, rhs, lhs_ctx, rhs_ctx):
        """
        returns a tuple:

            (is_different, brief_description, long_description, filename)

        brief_description should be a one line human readable string
        that describes any differences found, suitable for inclusion
        in a diff report.

        long_description describes any difference found in more detail
        and may be included in the details section of a regression
        report.
        
        filename may be None or the name of a file containing a more
        detailed diff report.
        """
        raise NotImplementedError

class DataFrameDiffer(DataFrameBuilder, Differ):
    """
    Subclass of Differ and DataFrameBuilder to be used
    for collecting a dataframe of node values and then
    comparing with another.
    """
    
    def __init__(self, nodes, dtype=object, sparse_fill_value=None, xls_filename=None):
        self.nodes_or_names = nodes
        nodes = self._get_nodes(nodes)
        DataFrameBuilder.__init__(self, nodes, dtype=dtype, sparse_fill_value=sparse_fill_value)
        Differ.__init__(self)
        self.__tolerances = {}
        self.__xls_filename = xls_filename

    #
    # This is so nodes can be passed in as a list of names (including module/package/class)
    # and the nodes will be found using that, instead of passing a node instance in.
    #
    # When the differ is pickled the node may exist in the target environment but the pickled
    # node format remembers what class the node was implemented on as well as the class it's
    # bound too. If that's different in two instances it won't find the node and so the
    # regression will fail. By always getting the nodes by name it will get the correct node.
    # 
    def _get_nodes(self, nodes_or_names):
        nodes = []
        for n in nodes_or_names:
            if isinstance(n, basestring) and "." in n:
                name = n
                # import the module if necessary
                components = name.split(".")
                modulename = components[0]
                try:
                    __import__(modulename)
                except ImportError:
                    pass

                module = sys.modules.get(modulename, None)
                while modulename in sys.modules and len(components) > 1:
                    module = sys.modules[modulename]
                    components.pop(0)
                    modulename = ".".join((modulename, components[0]))
                    try:
                        __import__(modulename)
                    except ImportError:
                        pass

                if not components or not module:
                    raise Exception("Node not found: '%s'" % name)

                # get the class and then the node from the module
                obj = module
                while components:
                    attr = components.pop(0)
                    obj = getattr(obj, attr)
                n = obj

            # check by this point we have a node
            if not isinstance(n, MDFNode):
                raise Exception("Node not found: %s" % n)

            nodes.append(n)

        return nodes

    def __setstate__(self, state):
        # fix up the nodes again from their names
        state["nodes"] = self._get_nodes(state["nodes_or_names"])
        self.__dict__.update(state)

    def __getstate__(self):
        # don't pickle the nodes - get them again when unpickling
        state = dict(self.__dict__)
        state.pop("nodes", None)
        return state

    def set_tolerance(self, tolerance, abs=True, node=None):
        """
        Sets the tolerance for comparing values.
        
        When abs is True a difference is considered
        significant when::

            abs(lhs - rhs) > tolerance 
            
        Or if abs is False a difference is considered
        significant when::

            abs((lhs / rhs) - 1.0) > tolerance


        If node is None the tolerance applies to all values,
        othereise the tolerance only applies to values
        derived from that specific node.
        """
        assert node is None or node in self.nodes
        self.__tolerances[node] = (tolerance, abs)

    def get_tolerance(self, node=None):
        """
        returns the tolerance set using set_tolerance
        as a tuple (tolerance, abs)
        """
        tolerance = self.__tolerances.get(node, None)
        if tolerance is None:
            tolerance = self.__tolerances.get(None, (0.0, True))
        return tolerance

    def diff(self, other, ctx, other_ctx):
        """
        Returns a tuple (is_different, brief_description, long_description, detail_filename)
        that describes the difference between self and other.
        
        If applicable and if a path was passed to the ctor then additional details
        describing differences will be written to a file, and that filename is
        returned as part of the diff.
        """
        lhs_data = self.get_dataframe(ctx)
        rhs_data = other.get_dataframe(other_ctx)

        # Coerce python datetime indexes to pandas DatetimeIndex
        # TODO: Remove this once pandas 0.7.3 compatibility is no longer needed
        def _coerce_dt_index(index):
            if len(index) > 0 and (not isinstance(index, pa.DatetimeIndex)):
                # If first and last index entries are python datetimes, assume that the index contains only datetimes
                if isinstance(index[0], datetime) and isinstance(index[-1], datetime):
                    return pa.DatetimeIndex(index)

            # Return the original index if no modifications were done
            return index

        lhs_data.index = _coerce_dt_index(lhs_data.index)
        rhs_data.index = _coerce_dt_index(rhs_data.index)

        # diff each node's values individually
        is_different = False
        brief_description = ""
        long_description = ""
        different_nodes = []
        details_filename = None

        def _cols_are_similar(lhs_col, rhs_col):
            lhs_col, rhs_col = str(lhs_col), str(rhs_col) 
            if "." in lhs_col:
                unused, lhs_col = lhs_col.rsplit(".", 1)
            if "." in rhs_col: 
                unused, rhs_col = rhs_col.rsplit(".", 1)
            return lhs_col.lower() == rhs_col.lower()

        for node in self.nodes:
            lhs_columns = sorted(self.get_columns(node, ctx))
            rhs_columns = sorted(other.get_columns(node, other_ctx))

            # check the columns are the same
            if len(lhs_columns) != len(rhs_columns) \
            or (np.array(lhs_columns) != np.array(rhs_columns)).any():
                is_different = True

                description = "%s has column differences" % node.name
                description += "\n" + "-" * len(description) + "\n\n"

                max_columns = max(len(lhs_columns), len(rhs_columns))
                lhs_tmp_cols = list(lhs_columns) + [None] * (max_columns - len(lhs_columns))
                rhs_tmp_cols = list(rhs_columns) + [None] * (max_columns - len(rhs_columns))
                cols_are_similar = len(lhs_columns) == len(rhs_columns)
                for i, (lhs_col, rhs_col) in enumerate(zip(lhs_tmp_cols, rhs_tmp_cols)):
                    if lhs_col != rhs_col:
                        description += "%d: %s != %s\n" % (i, lhs_col, rhs_col)
                        if not _cols_are_similar(lhs_col, rhs_col):
                            cols_are_similar = False

                long_description += description + "\n\n"

                # if the cols aren't even similar skip the rest of the checks
                if not cols_are_similar:
                    long_description += "**Not diffing data because of column differences**\n\n"
                    different_nodes.append(node)
                    continue

            lhs_df = lhs_data[lhs_columns]
            rhs_df = rhs_data[rhs_columns]

            # check the indices are the same
            if (np.array(lhs_df.index) != np.array(rhs_df.index)).any():
                is_different = True
                different_nodes.append(node)

                mask = np.array(rhs_data.index) != np.array(lhs_data.index)
                lhs_diff_dates = lhs_data.index[mask]
                rhs_diff_dates = rhs_data.index[mask]

                description = "%s has index differences" % node.name
                description += "\n" + "-" * len(description) + "\n\n"
                description += "indexes are different starting at %s != %s" % (
                                            lhs_diff_dates[0],
                                            rhs_diff_dates[0])

                long_description += description + "\n\n"
                continue

            #
            # columns and indices are the same so check the contents
            #
            try:
                lhs_df = lhs_df.astype(float)
            except TypeError:
                pass

            try:
                rhs_df = rhs_df.astype(float)
            except TypeError:
                pass

            tolerance, is_abs = self.get_tolerance(node)
            if is_abs:
                diffs = np.abs(lhs_df - rhs_df)
                mask = (diffs > tolerance).values
            else:
                diffs = np.abs((lhs_df / rhs_df) - 1.0)
                mask = (diffs > tolerance).values

            # don't include differences where both sides are NaN or 0.0
            try:
                mask &= ~((lhs_df == 0.0) & (rhs_df == 0.0)).values
                mask &= ~(np.isnan(lhs_df) & np.isnan(rhs_df)).values
            except TypeError:
                pass

            # do include differences where one side is NaN but the other isn't
            try:
                mask |= np.isnan(lhs_df).values & ~np.isnan(rhs_df).values
                mask |= np.isnan(rhs_df).values & ~np.isnan(lhs_df).values
            except TypeError:
                pass

            if mask.any():
                is_different = True
                different_nodes.append(node)

                row_mask = np.apply_along_axis(np.any, 1, mask)
                diffs = diffs[row_mask]

                description = "%s has %d differences" % (node.name, len(diffs.index))
                description += "\n" + "-" * len(description) + "\n\n"
                description += "tolerance = %f%s\n\n" % (
                                    tolerance if is_abs else tolerance * 100.0,
                                    "%" if not is_abs else "")

                lhs_diffs = lhs_df[row_mask]
                rhs_diffs = rhs_df[row_mask]

                # convert the lhs and rhs to strings
                lhs_lines = lhs_diffs.to_string().splitlines()
                rhs_lines = rhs_diffs.to_string().splitlines()

                # pad so they're the same length
                lhs_lines += ["" * max(len(rhs_lines) - len(lhs_lines), 0)]
                rhs_lines += ["" * max(len(lhs_lines) - len(rhs_lines), 0)]
                
                max_lines = 10
                mid = min(len(lhs_lines), max_lines) // 2

                # format them on the same lines
                lines = []
                fmt = "%%-%ds     %%-2s     %%s" % max([len(x) for x in lhs_lines])
                for i, (l, r) in enumerate(zip(lhs_lines, rhs_lines)):
                    if i == mid:
                        lines.append(fmt % (l, "!=", r))
                    else:
                        lines.append(fmt % (l, "  ", r))

                description += "\n".join(lines[:max_lines])
                if len(lines) > max_lines:
                    description += "\n..."

                long_description += description + "\n\n"

        if is_different:
            node_names = [x.short_name for x in different_nodes]
            _log.debug("Differences found in nodes: %s" % ", ".join(node_names))

            if len(different_nodes) == 0:
                brief_description = "No data differences"
                long_description += "No data differences\n\n"
            elif len(different_nodes) == 1:
                brief_description = "%s has differences" % node_names[0]
            else:
                brief_description = ", ".join(node_names[:-1])
                brief_description += " and %s have differences" % node_names[-1] 

            if self.__xls_filename and len(different_nodes) > 0:
                _log.debug("Writing differences to Excel file '%s'" % self.__xls_filename)
                details_filename = self.__xls_filename
                self.__write_xls(other, different_nodes, lhs_data, rhs_data, details_filename, ctx, other_ctx)

        return (is_different, brief_description, long_description, details_filename)

    def __write_xls(self, rhs_differ, different_nodes, lhs_data, rhs_data, filename, lhs_ctx, rhs_ctx):
        """write the diffs to a spreadsheet"""
        wb = xlwt.Workbook()
        date_style = xlwt.easyxf(num_format_str='YYYY-MM-DD')
        nsheets = 0

        for node in different_nodes:
            lhs_columns = sorted(self.get_columns(node, lhs_ctx))
            lhs_df = lhs_data[lhs_columns]

            rhs_columns = sorted(rhs_differ.get_columns(node, rhs_ctx))
            rhs_df = rhs_data[rhs_columns]

            if len(lhs_df.columns) > 255 or len(rhs_df.columns) > 255: # xlwt has a limit of 256 columns
                # just dump data into two separate CSV if its too big for a nice XLS report
                fname = "%s__%s" % (node.short_name, os.path.splitext(os.path.basename(filename))[0])
                csv_fpath =  os.path.join(os.path.dirname(filename), fname)
                                      
                _log.info("Node %s has mare than 255 columns, can't use xlwt, writing CSV to "
                          "%s[_LHS|_RHS].csv" % (node.name, csv_fpath))
                lhs_df.to_csv(csv_fpath+"_LHS.csv")
                rhs_df.to_csv(csv_fpath+"_RHS.csv")
            else:    
                _log.info("Writing Excel sheet for %s" % node.name)
                nsheets += 1
                diffs_ws = wb.add_sheet(("%s_DIFFS" % node.short_name)[-31:])
                lhs_ws = wb.add_sheet(("%s_LHS" % node.short_name)[-31:])
                rhs_ws = wb.add_sheet(("%s_RHS" % node.short_name)[-31:])
    
                for ws, df in ((lhs_ws, lhs_df), (rhs_ws, rhs_df)):
                    for row, value in enumerate(df.index):
                        ws.write(row + 1, 0, value, date_style)
    
                    for col_i, col_name in enumerate(df.columns):
                        ws.write(0, col_i + 1, str(col_name))
    
                        col = df[col_name]
                        for row_i, value in enumerate(col):
                            if np.isnan(value):
                                ws.row(row_i + 1).set_cell_error(col_i + 1, "#NUM!")
                            else:
                                ws.write(row_i + 1, col_i + 1, value)
    
                max_cols = max(len(lhs_columns), len(rhs_columns))
                max_rows = max(len(lhs_df.index), len(rhs_df.index))
                tolerance, is_abs = self.get_tolerance(node)
    
                for row, value in enumerate(lhs_df.index):
                    diffs_ws.write(row + 1, 0,
                        xlwt.Formula("IF(EXACT(%(l)s,%(r)s),%(l)s,\"ERROR\")" % {
                                        "l" : _to_range(row + 1, 0, lhs_ws),
                                        "r" : _to_range(row + 1, 0, rhs_ws)}),
                        date_style)
    
                for col_i, col_name in enumerate(lhs_df.columns):
                    diffs_ws.write(0, col_i + 1,
                        xlwt.Formula("IF(EXACT(%(l)s,%(r)s),%(l)s,\"ERROR\")" % {
                                        "l" : _to_range(0, col_i + 1, lhs_ws),
                                        "r" : _to_range(0, col_i + 1, rhs_ws)}))
    
                for col_i in xrange(1, max_cols + 1):
                    for row_i in xrange(1, max_rows + 1):
                        if is_abs:
                            diffs_ws.write(row_i,
                                           col_i,
                                           xlwt.Formula("ABS(%s-%s)" % (_to_range(row_i, col_i, lhs_ws),
                                                                        _to_range(row_i, col_i, rhs_ws))))
                        else:
                            diffs_ws.write(row_i,
                                           col_i,
                                           xlwt.Formula("ABS((%s/%s)-1)" % (_to_range(row_i, col_i, lhs_ws),
                                                                            _to_range(row_i, col_i, rhs_ws))))                                                                 
        if nsheets:
            wb.save(filename)
