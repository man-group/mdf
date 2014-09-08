"""
Basic commonly used builder classes
"""
import numpy as np
import pandas as pa
from ..nodes import MDFNode, MDFEvalNode
from collections import deque, defaultdict
import datetime
import operator
import csv
import matplotlib.pyplot as pp
import sys
import types

if sys.version_info[0] > 2:
    basestring = str

def _get_labels(node, label=None, value=None):
    """
    returns a list of lables the same length as value, if value is
    a list (or of length 1 if value is not a list)

    If label is supplied that will be used as the base (eg x.0...x.N)
    or if it's a list it will be padded to the correct length and returned.
    """
    # if there's a value return enough labels for the value, if it's a list
    if value is not None:
        if label is None:
            label = _get_labels(node)[0]

        # if value is a list return a label for each element
        if isinstance(value, (tuple, list, np.ndarray, pa.core.generic.NDFrame, pa.Index)):
            # if the label is a list already pad it to the right size
            if isinstance(label, (tuple, list, np.ndarray, pa.core.generic.NDFrame, pa.Index)):
                label = list(label)
                if len(label) < len(value):
                    label += ["%s.%d" % (label, i) for i in xrange(len(label), len(value))]
                return label[:len(value)]

            # otherwise create a list using the value's index
            if isinstance(value, pa.Series):
                return ["%s.%s" % (label, c) for c in value.index]
            return ["%s.%d" % (label, i) for i in xrange(len(value))]

        # if value is not a list return a single label
        if isinstance(label, (tuple, list, np.ndarray, pa.core.generic.NDFrame)):
            return list(label[:1])
        return [label]

    # if there's no value but a label, assume the value is a scalar and return a single label
    if label is not None:
        if isinstance(label, (tuple, list, np.ndarray, pa.core.generic.NDFrame)):
            return list(label[:1])
        return [label]

    # if there's no value and no node, assume the value is a scalar and return the name of the node
    if isinstance(node, MDFNode):
        return [node.name.split(".").pop()]
    return [str(node)]

def _relabel(columns, node_names, short_names, ctx_ids):
    """"
    return list of new column names that don't overlap
    columns is a list of columns lists for each node.
    """
    assert len(columns) == len(node_names) == len(short_names) == len(ctx_ids)

    def _get_overlapping(columns):
        col_count = {}
        overlapping = set()
        for cols in columns:
            for col in cols:
                col_count.setdefault(col, 0)
                col_count[col] += 1
                if col_count[col] > 1:
                    overlapping.add(col)
        return overlapping

    overlap = _get_overlapping(columns)
    if not overlap:
        return columns

    # take a copy as this will be updated in-place
    columns = [list(cols) for cols in columns]

    # collect the node names and contexts for all overlapping columns
    overlap_node_names = {}
    for i, (cols, node_name, short_name, ctx_id) \
    in enumerate(zip(columns, node_names, short_names, ctx_ids)):
        for j, col in enumerate(cols):
            if col in overlap:
                overlap_node_names.setdefault(col, [])\
                                    .append((i, j, node_name, short_name, ctx_id))

    for col, details in overlap_node_names.iteritems():
        is_, js_, node_names, short_names, ctx_ids = zip(*details)

        # prefix with the node short names if they're unique
        unique_short_names = np.unique(short_names)
        if unique_short_names.size == len(short_names):
            for i, j, node_name, short_name, ctx_id in details:
                columns[i][j] = "%s.%s" % (col, short_name)
            continue

        # otherwise try prefixing with the full names
        unique_node_names = np.unique(node_names)
        if unique_node_names.size == len(node_names):
            # if the short name is common replace it with the long name
            if unique_short_names.size == 1 \
            and col.startswith(unique_short_names[0]):
                for i, j, node_name, short_name, ctx_id in details:
                    columns[i][j] = col.replace(short_name, node_name)
            else:
                for i, j, node_name, short_name, ctx_id in details:
                    columns[i][j] = "%s.%s" % (col, node_name)
            continue

        # otherwise if the contexts are unique use a context id suffix
        unique_ctx_ids = np.unique(ctx_ids)
        if unique_ctx_ids.size == len(ctx_ids):
            for i, j, node_name, short_name, ctx_id in details:
                columns[i][j] = "%s.ctx-%s" % (col, ctx_id)
            continue

        # If none of those are unique use a numeric suffix.
        # This should be quite unlikely.
        for x in xrange(len(details)):
            columns[i][j] = "%s.%d" % (col, x)

    return columns

def _pairs_to_node_label_lists(node_label_pairs):
    results = []
    for node_or_node_label_pair in node_label_pairs:
        if isinstance(node_or_node_label_pair, (tuple, list)):
            # it's a tuple/list (node, label)
            results.append(node_or_node_label_pair)
        else:
            # it's just a node - use None as the label and a default
            # will be selected in _get_labels
            results.append((node_or_node_label_pair, None))

    # return ([node,...], [label,...])
    return map(list, zip(*results))

class CSVWriter(object):
    """
    callable object that appends values to a csv file
    For use with mdf.run
    """

    def __init__(self, fh, nodes, columns=None):
        """
        Writes node values to a csv file for each date.
        
        'fh' may be a file handle, or a filename, or a node.

        If fh is a node it will be evaluated for each context used
        and is expected to evaluate to the filename or file handle
        to write the results to.
        """
        # keep track of any file handles opened by this instance so they
        # can be closed.
        self.fh = fh
        self.open_fhs = []

        # fh may be either a file handle, a filename or a node
        # that evaluates to a file handle or name.
        self.writers = {}
        if not isinstance(fh, MDFNode):
            # if fh isn't a node use the same writer for all contexts
            if isinstance(fh, basestring):
                fh = open(fh, "wb")
                self.open_fhs.append(fh)
            writer = csv.writer(fh)
            self.writers = defaultdict(lambda: writer)

        self.handlers = None

        if isinstance(nodes, MDFNode):
            nodes = [nodes]

        if len(nodes) > 1 and columns is None:
            self.nodes, self.columns = _pairs_to_node_label_lists(nodes)
        else:
            self.nodes = nodes

            self.columns = list(columns or [])[:len(nodes)]
            self.columns += [None] * (len(nodes) - len(self.columns))

    def __del__(self):
        self.close()

    def close(self):
        """closes any file handles opened by this writer"""
        while self.open_fhs:
            fh = self.open_fhs.pop()
            fh.close()
        self.writers.clear()

    def __call__(self, date, ctx):
        # get the node values from the context
        values = [ctx.get_value(node) for node in self.nodes]

        # get the writer from the context, or create it if it's not been
        # created already.
        ctx_id = ctx.get_id()
        try: 
            writer = self.writers[ctx_id]
        except KeyError:
            fh = self.fh
            if isinstance(fh, MDFNode):
                fh = ctx.get_value(fh)
            if isinstance(fh, basestring):
                fh = open(fh, "wb")
                self.open_fhs.append(fh)
            writer = self.writers[ctx_id] = csv.writer(fh)

        # figure out how to handle them and what to write in the header
        if self.handlers is None:
            header = ["date"]
            self.handlers = []
            for node, value, column in zip(self.nodes, values, self.columns):
                if isinstance(column, MDFNode):
                    column = ctx.get_value(column)
                header.extend(_get_labels(node, column, value))

                if isinstance(value, (basestring, int, float, bool, datetime.date)):
                    self.handlers.append(self._write_basetype)
                elif isinstance(value, (list, tuple, np.ndarray, pa.Index, pa.core.generic.NDFrame)):
                    self.handlers.append(self._write_list)
                elif isinstance(value, pa.Series):
                    self.handlers.append(self._write_series)
                else:
                    raise Exception("Unhandled type %s for node %s" % (type(value), node))

            # write the header
            writer.writerow(header)

        # format the values and write the row
        row = [date]
        for handler, value in zip(self.handlers, values):
            handler(value, row)
        writer.writerow(row)

    def _write_basetype(self, value, row):
        row.append(value)

    def _write_list(self, value, row):
        row.extend(value)
        
    def _write_series(self, value, row):
        row.extend(value)

class NodeTypeHandler(object):
    """
    Base class for NodeData handling in DataFrameBuilder. Sub-classes
    should override _handle(). Callers should call handle()
    """
    def __init__(self, node, filter=False):
        self._name = node.short_name
        self._filter = node.get_filter() if filter and isinstance(node, MDFEvalNode) else None
        self._index = []
        self._labels = set()
        self._data = dict()

    def handle(self, date, ctx, value):
        """
        Stashes the date and then handles the data
        in the sub-class
        """
        if self._filter is None \
        or ctx[self._filter]:
            self._index.append(date)
            self._handle(date, value)

    def _handle(self, date, value):
        raise NotImplementedError("_handle must be implemented in the subclass")

    def get_dataframe(self, dtype=object):
        """
        Returns a DataFrame containing the values accumulated
        for each column for a node.
        """
        columns = self.get_columns()
        df = pa.DataFrame(data={}, index=self._index, columns=columns, dtype=dtype)
        for (d, l), value in self._data.items():
            df[l][d] = value
        return df

    def get_columns(self):
        """
        returns the columns used to construct the dataframe
        in get_dataframe
        """
        return sorted(self._labels)

class NodeListTypeHandler(NodeTypeHandler):
    def __init__(self, node, filter=False):
        super(NodeListTypeHandler, self).__init__(node, filter=filter)

    def _handle(self, date, value):
        # the set of labels is fixed on the first callback
        # and is of the form node.name.X for int X
        self._labels = self._labels or [self._name + "." + str(i) for i in range(len(value))]
        assert len(self._labels) == len(value)
        for l, v in zip(self._labels, value):
            self._data[(date, l)] = v

class NodeDictTypeHandler(NodeTypeHandler):
    def __init__(self, node, filter=False):
        super(NodeDictTypeHandler, self).__init__(node, filter=filter)
        
    def _handle(self, date, value):
        # the set of labels can grow over time
        # and they reflect the big union of the dict keys
        self._labels = self._labels.union(map(str, value.keys()))
        for k, v in value.items():
            self._data[(date, str(k))] = v

class NodeSeriesTypeHandler(NodeTypeHandler):
    def __init__(self, node, filter=False):
        super(NodeSeriesTypeHandler, self).__init__(node, filter=filter)
        
    def _handle(self, date, value):
        # the set of labels can grow over time
        # and they reflect the big union of the row labels in the
        # node value Series
        self._labels = self._labels.union(map(str, value.index))
        for l in value.index:
            self._data[(date, str(l))] = value[l]

class NodeBaseTypeHandler(NodeTypeHandler):
    def __init__(self, node, filter=False):
        super(NodeBaseTypeHandler, self).__init__(node, filter=filter)
        self._labels.add(self._name)

    def _handle(self, date, value):
        self._data[(date, self._name)] = value

class DataFrameBuilder(object):
    # version number to provide limited backwards compatibility
    __version__ = 2

    def __init__(self, nodes, contexts=None, dtype=object, sparse_fill_value=None, filter=False,
                 start_date=None):
        """
        Constructs a new DataFrameBuilder.
        
        dtype and sparse_fill_value can be supplied as hints to the
        data type that will be constructed and whether or not to try
        and create a sparse data frame.
        
        If `filter` is True and the nodes are filtered then only values
        where all the filters are True will be returned.
        
        NB. the labels parameter is currently not supported
        """
        self.context_handler_dict = {}
        self.filter = filter
        self.dtype = object
        self.sparse_fill_value = None
        self.start_date = start_date

        self._finalized = False
        self._cached_dataframes = {}
        self._cached_columns = {}

        if isinstance(nodes, MDFNode):
            nodes = [nodes]
        
        self.nodes = nodes
        
        if contexts:
            assert len(contexts) == len(nodes)
        else:
            contexts = []

        self.contexts = contexts
        self._last_ctx = None

    def __call__(self, date, ctx):
        # copy the version to this instance (this isn't done in the ctor as the regression
        # testing works by creating the builders in the main process and then sending them
        # to the remote processes - so the version is snapped when the builder is actually
        # called).
        self._version_ = self.__version__

        self._last_ctx = ctx.get_id()

        ctx_list = self.contexts or ([ctx] * len(self.nodes))
        for ctx_, node in zip(ctx_list, self.nodes):
            node_value = ctx_.get_value(node)
            handler_dict = self.context_handler_dict.setdefault(ctx.get_id(), {})

            key = (node.name, node.short_name, ctx_.get_id())
            handler = handler_dict.get(key)
            
            if not handler:
                if isinstance(node_value, (basestring, int, float, bool, datetime.date)) \
                or isinstance(node_value, tuple(np.typeDict.values())):
                    handler = NodeBaseTypeHandler(node, filter=self.filter)
                elif isinstance(node_value, dict):
                    handler = NodeDictTypeHandler(node, filter=self.filter)
                elif isinstance(node_value, pa.Series):
                    handler = NodeSeriesTypeHandler(node, filter=self.filter)
                elif isinstance(node_value, (list, tuple, deque, np.ndarray, pa.Index, pa.core.generic.NDFrame)):
                    handler = NodeListTypeHandler(node, filter=self.filter)
                else:
                    raise Exception("Unhandled type %s for node %s" % (type(node_value), node))

                handler_dict[key] = handler

            if (self.start_date is None) or (date >= self.start_date):
                handler.handle(date, ctx_, node_value)

    def clear(self):
        self.context_handler_dict.clear()
        self._cached_columns.clear()
        self._cached_dataframes.clear()

    def get_dataframe(self, ctx, dtype=None, sparse_fill_value=None):
        ctx_id = ctx if isinstance(ctx, int) else ctx.get_id()
        
        # if the builder's been finalized and there's a dataframe cached
        # return that without trying to convert it to a sparse dataframe
        # or changing the dtypes (dtype and sparse_fill_value are only
        # hints).
        try:
            return self._cached_dataframes[ctx_id]
        except KeyError:
            pass

        if dtype is None:
            dtype = self.dtype

        result_df = self._build_dataframe(ctx_id, dtype)

        if sparse_fill_value is None:
            sparse_fill_value = self.sparse_fill_value

        if sparse_fill_value is not None:
            # this doesn't always work depending on the actual dtype
            # the dataframe ends up being
            try:
                result_df = result_df.to_sparse(fill_value=sparse_fill_value)
            except TypeError:
                pass

        # try and infer types for any that are currently set to object
        return result_df.convert_objects()

    def _build_dataframe(self, ctx, dtype):
        """builds a dataframe from the collected data"""
        ctx_id = ctx if isinstance(ctx, int) else ctx.get_id()
        handler_dict = self.context_handler_dict[ctx_id]

        if len(handler_dict) == 1:
            # if there's only one handler simply get the dataframe from it
            handler = next(iter(handler_dict.values()))
            result_df = handler.get_dataframe(dtype=dtype)
        else:
            # otherwise do an outer join of all the handlers' dataframes
            result_df = pa.DataFrame(dtype=dtype)
            handler_keys, handlers = zip(*handler_dict.items())
            dataframes = [h.get_dataframe(dtype=dtype) for h in handlers]

            # relabel any overlapping columns
            all_columns = [df.columns for df in dataframes]
            node_names, short_names, ctx_ids = zip(*handler_keys)
            new_columns = _relabel(all_columns, node_names, short_names, ctx_ids)
            for df, cols in zip(dataframes, new_columns):
                df.columns = cols

            # join everything into a single dataframe
            for df in dataframes:
                result_df = result_df.join(df, how="outer")
            result_df = result_df.reindex(columns=sorted(result_df.columns))

        return result_df

    def get_columns(self, node, ctx):
        """
        returns the sub-set of columns in the dataframe returned
        by get_dataframe that relate to a particular node
        """
        ctx_id = ctx if isinstance(ctx, int) else ctx.get_id()

        try:
            return self._cached_columns[(ctx_id, node)]
        except KeyError:
            pass

        handler_dict = self.context_handler_dict[ctx_id]

        # the ctx is the root context passed to __call__, which may be
        # different from the shifted contexts that the node was actually
        # evaluated in.
        # Get all the columns for this node in all sub-contexts.
        columns = []
        ctx_ids = []
        for (node_name, short_name, sub_ctx_id), handler in handler_dict.items():
            if node_name == node.name \
            and short_name == node.short_name:
                columns.append(handler.get_columns())
                ctx_ids.append(sub_ctx_id)

        # re-label in case the same node was evaluated in multiple sub-contexts
        columns = _relabel(columns,
                            [node.name] * len(columns),
                            [node.short_name] * len(columns),
                            ctx_ids)

        return reduce(operator.add, columns, [])

    @property
    def dataframes(self):
        """all dataframes created by this builder (one per context)"""
        return [self.get_dataframe(ctx) for ctx in self.context_handler_dict.keys()]

    @property
    def dataframe(self):
        return self.get_dataframe(self._last_ctx) if self._last_ctx is not None else None

    def plot(self, show=True, **kwargs):
        """plots all collected dataframes and shows, if show=True"""
        for df in self.dataframes:
            df.plot(**kwargs)
            legend = sorted(df.columns)
    
            pp.legend(legend, loc='upper center', bbox_to_anchor=(0.5, -0.17), fancybox=True, shadow=True)
        if show:
            pp.show()

    def finalize(self):
        """
        Throw away intermediate structures and just retain any dataframes
        and columns.

        It's not possible to add more data to the builder after this has
        been called.
        """
        assert not self._finalized

        # cache all dataframes and column sets
        for ctx_id in list(self.context_handler_dict.keys()):
            for node in self.nodes:
                self._cached_columns[(ctx_id, node)] = self.get_columns(node, ctx_id)
            self._cached_dataframes[ctx_id] = self.get_dataframe(ctx_id)
            
            # delete the data for that context in case we're low on memory
            del self.context_handler_dict[ctx_id]

        # this should be empty now
        assert len(self.context_handler_dict) == 0

        # snap the version number if it's not already been taken (see __call__)
        if not hasattr(self, "_version_"):
            self._version_ = self.__version__

        self._finalized = True

    def combine_result(self, other, other_ctx, ctx):
        """
        Adds a result from another df builder to this one.

        If not already finalized this method will call finalize and
        so no more data can be collected after this is called.
        """
        ctx_id = ctx.get_id()
        other_ctx_id = other_ctx.get_id()

        # only the caches will be updated so make sure self has been
        # finalized
        if not self._finalized:
            self.finalize()

        # update self.nodes with any nodes from the other
        nodes = set(self.nodes)
        other_nodes = set(other.nodes)
        additional_nodes = other_nodes.difference(nodes)
        self.nodes += list(additional_nodes)

        # copy the finalized data
        for node in other.nodes:
            self._cached_columns[(ctx_id, node)] = other.get_columns(node, other_ctx_id)

        self._cached_dataframes[ctx_id] = other.get_dataframe(other_ctx_id)

class FinalValueCollector(object):
    """
    callable object that collects the final values for a set of nodes.
    For use with mdf.run
    """

    def __init__(self, nodes):
        if isinstance(nodes, MDFNode):
            nodes = [nodes]
        self.__nodes = nodes
        self.__values = {}
        self.__contexts = []

    def __call__(self, date, ctx):
        ctx_id = ctx.get_id()
        self.__values[ctx_id] = [ctx.get_value(node) for node in self.__nodes]
        if ctx_id not in self.__contexts:
            self.__contexts.append(ctx_id)

    def clear(self):
        """clears all previously collected values"""
        self.__values.clear()
        self.__contexts = []

    def get_values(self, ctx=None):
        """returns the collected values for a context"""
        if not self.__values:
            return None

        if ctx is None:
            ctx = self.__contexts[-1]

        ctx_id = ctx if isinstance(ctx, int) else ctx.get_id()
        return self.__values.get(ctx_id, None)

    def get_dict(self, ctx=None):
        """returns the collected values as a dict keyed by the nodes"""
        values = self.get_values(ctx)
        if values is None:
            return None

        return dict(zip(self.__nodes, values))

    @property
    def values(self):
        """returns the values for the last context"""
        if not self.__contexts:
            return None

        ctx_id = self.__contexts[-1]
        return self.get_values(ctx_id)

class NodeLogger(object):
    """
    callable object for use with mdf run that logs a message
    each time a node value changes.
    """

    def __init__(self, nodes, fh=sys.stdout):
        """
        ``nodes`` is the list of node values to watch
        ``fh`` is a file like object to write to when changes are observed
        """
        self.nodes = nodes
        self.fh = fh

    def __call__(self, date, ctx):
        # get the max length of the node names for formatting nicely
        max_len = max((len(node.name) for node in self.nodes))
        fmt = "%%-%ds = %%s\n" % max_len

        # get the initial values in the root context and any shifted contexts
        root_ctx = ctx
        values = [None] * len(self.nodes)

        for i, node in enumerate(self.nodes):
            values[i] = ctx[node]

        # log the initial values
        self.fh.write("%s:\n" % ctx)
        for node, value in zip(self.nodes, values):
                self.fh.write(fmt % (node.name, value))
        self.fh.write("\n")

        while True:
            prev_values = list(values)
            yield

            # get the new values
            for i, node in enumerate(self.nodes):
                if node.has_value(ctx):
                    values[i] = ctx[node]

            if values != prev_values:
                self.fh.write("%s *changed*:\n" % ctx)
                for node, value, prev_value in zip(self.nodes, values, prev_values):
                    if value != prev_value:
                        self.fh.write(fmt % (node.name, value))
                self.fh.write("\n")
