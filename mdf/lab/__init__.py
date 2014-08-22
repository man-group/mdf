import re
import pandas as pd
import numpy as np
import operator
import logging

from functools import partial
from datetime import datetime
from collections import namedtuple, Sequence
from pandas.core import datetools
from pandas.core.datetools import BDay

from mdf.nodes import MDFNode, MDFVarNode, now
from mdf.runner import plot
from mdf.parser import tokenize
from mdf.context import _get_current_context, MDFContext
from mdf.lab.progress import ProgressBar
from IPython.core.magic import Magics, line_magic, magics_class

# include everything from mdf in this module's namespace
from mdf import __all__ as __mdf_all__
from mdf import *

import sys
if sys.version_info[0] > 2:
    basestring = str
    from functools import reduce

__all__ = list(set(__mdf_all__).difference(["plot"]).union(["mdf_plot"]))

mdf_plot = plot

_log = logging.getLogger(__name__)

# and pull in some bits from the viewer, if possible
_viewer_imported = False
try:
    from mdf import viewer
    from mdf.viewer import excel
    from mdf.viewer import get_selected, get_dataframes
    from mdf.viewer.excel import export_dataframe
    import wx

    __all__.extend(["get_selected",
                    "get_dataframes",
                    "export_dataframe"])

    _viewer_imported = True
except ImportError:
    pass


class ShiftedResultsTuple(Sequence):
    def __init__(self, field_names, *values):
        # Use the namedtuple construction to apply validation and correction to field names
        tp = namedtuple("_ShiftedResults", field_names, rename=True)
        fdict = tp(*values)._asdict()
        for k, v in fdict.items():
            self.__setattr__(k, v)

        self.__items = fdict.values()
        itemstext = ', '.join('%s=%s' % (name, value) for name, value in fdict.items())
        self.__reprtext = type(self).__name__ + ("(%s)" % itemstext)

    def __len__(self):
        return len(self.__items)

    def __getitem__(self, index):
        return self.__items[index]

    def __repr__(self):
        return self.__reprtext


class DataFrameWithShiftSet(pd.DataFrame):
    """
    Subclass of pandas.DataFrame with two additional
    properties::

        shift_set_name
        shift_set
    """

    def __init__(self, df, shift_set_name, shift_set):
        pd.DataFrame.__init__(self, df)
        self.__shift_set_name = shift_set_name
        self.__shift_set = dict(shift_set)

    @property
    def shift_set_name(self):
        """name of the shift set used to construct this dataframe"""
        return self.__shift_set_name

    @property
    def shift_set(self):
        """dictionary of shifted nodes used to construct this dataframe"""
        return self.__shift_set


class WidePanelWithShiftSet(pd.WidePanel):
    """
    Subclass of pandas.WidePanel with two additional
    properties::

        shift_set_name
        shift_set
    """

    def __init__(self, wp, shift_set_name, shift_set):
        pd.WidePanel.__init__(self,
                              items=wp,
                              major_axis=wp.major_axis,
                              minor_axis=wp.minor_axis)
        self.__shift_set_name = shift_set_name
        self.__shift_set = dict(shift_set)

    @property
    def shift_set_name(self):
        """name of the shift set used to construct this widepanel"""
        return self.__shift_set_name

    @property
    def shift_set(self):
        """dictionary of shifted nodes used to construct this widepanel"""
        return self.__shift_set

@magics_class
class MDFMagics(Magics):
    """A component to manage the mdf magic functions"""
    __timestep = BDay(1)

    @line_magic
    def mdf_help(self, _=""):
        """Show the mdf ipython help"""
        mdf_pylab_help()

    @line_magic
    def mdf_ctx(self, parameter_s=""):
        """
        Gets or sets the current context.

        %mdf_ctx [new_ctx]
        """
        cur_ctx = _get_current_context()
        if parameter_s:
            ctx = eval(parameter_s, self.shell.user_global_ns, self.shell.user_ns)
            assert isinstance(ctx, MDFContext)
            ctx._activate_ctx()
            cur_ctx = ctx
        return cur_ctx

    @line_magic
    def mdf_now(self, parameter_s=""):
        """
        Gets or sets the date of the current context.

        %mdf_now [date]
        """
        curr_ctx = _get_current_context()
        if parameter_s:
            now = _parse_datetime(parameter_s, self.shell.user_global_ns, self.shell.user_ns)
            root_ctx = curr_ctx.get_parent() or curr_ctx
            root_ctx.set_date(now)
        return curr_ctx.get_date()

    @line_magic
    def mdf_reset(self, parameter_s=""):
        """
        Resets the current mdf context, and optionally sets the current date.
        
        %mdf_reset [date]
        
        eg: %mdf_reset
        or: %mdf_reset 2010-01-01
        """
        if parameter_s:
            now = _parse_datetime(parameter_s, self.shell.user_global_ns, self.shell.user_ns)
        else:
            now = datetools.normalize_date(datetime.now())
        ctx = MDFContext(now)
        ctx._activate_ctx()

    @line_magic
    def mdf_timestep(self, parameter_s=""):
        """
        Gets/sets the timestep used to advance the date when calling
        %mdf_advance or %mdf_evalto.

        %mdf_timestep [offset]

        eg: %mdf_timestep
        or: %mdf_timestep WEEKDAY
        """
        if parameter_s:
            self.__timestep = datetools.getOffset(parameter_s)
        return self.__timestep

    @line_magic
    def mdf_evalto(self, parameter_s=""):
        """
        Advances the current context to the end date and return a pandas
        dataframe of nodes evaluated on each timestep.

        %mdf_evalto <end_date> [nodes...]

        eg: %mdf_evalto 2020-01-01 <my node 1> <my node 2>
        """
        args = tokenize(parameter_s)

        cur_ctx = _get_current_context()
        root_ctx = cur_ctx.get_parent() or cur_ctx
        end_date, nodes = args[0], args[1:]
        end_date = _parse_datetime(end_date, self.shell.user_global_ns, self.shell.user_ns)
        nodes = map(lambda x: eval(x, self.shell.user_global_ns, self.shell.user_ns), nodes)

        df_ctx = root_ctx
        if len(nodes) > 0 and isinstance(nodes[-1], (dict, list, tuple)):
            shift_sets = _get_shift_sets(args[-1], nodes.pop())
            assert len(shift_sets) <= 1, "Only one shift set allowed for %mdf_evalto"
            if shift_sets:
                unused, shift_set = shift_sets[0]
                df_ctx = df_ctx.shift(shift_set=shift_set)

        df_builder = DataFrameBuilder(nodes, filter=True)
        date_range = pd.DatetimeIndex(start=cur_ctx.get_date(), end=end_date, freq=self.__timestep)
        for dt in date_range:
            root_ctx.set_date(dt)
            df_builder(dt, df_ctx)
        return df_builder.get_dataframe(df_ctx)

    @line_magic
    def mdf_advance(self, parameter_s=""):
        """
        Advance the current context one timestep (see %mdf_timestep).

        %mdf_advance [nodes...]

        If node is specified the value of node after the time has
        been advanced is returned.

        eg: %mdf_advance mdf.now
        """
        args = tokenize(parameter_s)

        nodes = []
        if args:
            nodes = map(lambda x: eval(x, self.shell.user_global_ns, self.shell.user_ns), args)
            for node in nodes:
                assert isinstance(node, MDFNode)

        cur_ctx = _get_current_context()
        root_ctx = cur_ctx.get_parent() or cur_ctx
        root_ctx.set_date(root_ctx.get_date() + self.__timestep)

        if len(nodes) > 0:
            if len(nodes) == 1:
                return cur_ctx[nodes[0]]
            return [cur_ctx[node] for node in nodes]

    @line_magic
    def mdf_show(self, parameter_s=""):
        """
        Opens a new mdf viewer and adds nodes to it, or adds the nodes
        to an existing viewer if one is open.

        %mdf_show [nodes...]
        """
        args = tokenize(parameter_s)
        nodes = map(lambda x: eval(x, self.shell.user_global_ns, self.shell.user_ns), args)
        ctx = _get_current_context()
        viewer.show(nodes, ctx=ctx)

    @line_magic
    def mdf_selected(self, parameter_s=""):
        """
        Return tuples of (ctx, node) for the currently selected nodes in
        the mdf viewer.
        
        %mdf_selected
        """
        return viewer.get_selected()

    def _magic_dataframe(self, parameter_s, widepanel=False, single_df=True):
        """Implementation for magic_dataframe and magic_widepanel"""
        # the first two arguments are dates, and after that it's a list of nodes
        # with some optional keyword args, ie %mdf_df <start> <end> node, node, node, shifts=[{x:1}, {x:2}]
        args = arg_names = tokenize(parameter_s)
        args = [_try_eval(x, self.shell.user_global_ns, self.shell.user_ns) for x in args]
        args = list(zip(arg_names, args))

        start = None
        if len(args) > 0:
            arg_name, arg = args.pop(0)
            start = _parse_datetime(arg_name, self.shell.user_global_ns, self.shell.user_ns)

        end = None
        if len(args) > 0:
            arg_name, arg = args.pop(0)
            end = _parse_datetime(arg_name, self.shell.user_global_ns, self.shell.user_ns)

        # the final argument can be the number of processes to use
        num_processes = 0
        if len(args) > 0:
            arg_name, arg = args[-1]
            if isinstance(arg, basestring) and arg.startswith("||"):
                arg_name, arg = args.pop()
                num_processes = int(arg[2:])

        # the next to last parameter may be a shift set or list of
        # shift sets.
        has_shifts = False
        shift_sets = [{}] # always have at least one empty shift set
        shift_names = ["_0"]
        arg_name, arg = args[-1] if len(args) > 0 else (None, None)
        if not isinstance(arg, MDFNode):
            arg_name, arg = args.pop()
            named_shift_sets = _get_shift_sets(arg_name, arg)
            if named_shift_sets:
                shift_names, shift_sets = zip(*named_shift_sets)
                has_shifts = True

        # any remaining arguments are the nodes
        nodes = []
        node_var_names = []
        for arg_name, node in args:
            assert isinstance(node, MDFNode), "%s is not a node" % arg_name
            nodes.append(node)
            node_var_names.append(arg_name)

        curr_ctx = _get_current_context()
        ctxs = [None] * len(nodes)

        if not nodes:
            # get the selected nodes from the viewer
            if _viewer_imported:
                selected = viewer.get_selected()
                ctxs, nodes = zip(*selected)
                for i, (ctx, node) in enumerate(selected):
                    assert ctx.is_shift_of(curr_ctx), \
                        "selected node '%s' is not in the current context" % node.name

                    # replace any contexts that are simply the current context with None
                    # so that shifting works correctly
                    if ctx is curr_ctx:
                        ctxs[i] = None

        # if there are shifts then all the contexts have to be None otherwise the
        # shifts won't work correctly. This could be relaxed later if it causes problems,
        # but for now this makes the code simpler.
        if has_shifts:
            assert np.array([x is None for x in ctxs]).all(), \
                "Can't apply shifts when contexts are explicitly specified"

        # list df_builders, one per node or group of nodes
        callbacks = []
        df_builders = []
        if widepanel or not single_df:
            # build multiple dataframes
            for node, ctx in zip(nodes, ctxs):
                if ctx is None:
                    df_builder = DataFrameBuilder([node], filter=True)
                else:
                    df_builder = DataFrameBuilder([node], contexts=[ctx], filter=True)
                df_builders.append(df_builder)
        else:
            # build a single dataframe
            if np.array([x is None for x in ctxs]).all():
                df_builder = DataFrameBuilder(nodes, filter=True)
            else:
                df_builder = DataFrameBuilder(nodes, contexts=ctxs, filter=True)
            df_builders.append(df_builder)

        # add all the dataframe builders to the callbacks
        callbacks.extend(df_builders)

        root_ctx = curr_ctx.get_parent() or curr_ctx
        date_range = pd.DatetimeIndex(start=start, end=end, freq=self.__timestep)

        # Add a progress bar to the callbacks
        callbacks.append(ProgressBar(date_range[0], date_range[-1]))

        shifted_ctxs = run(date_range,
                           callbacks,
                           ctx=root_ctx,
                           shifts=shift_sets,
                           num_processes=num_processes)

        if not has_shifts:
            shifted_ctxs = [root_ctx]

        # when returning a list of results because multiple shifts have been specified
        # use a named tuple with the items being the names of the shifts
        tuple_ctr = tuple
        if has_shifts:
            # Currying hell yeah
            tuple_ctr = partial(ShiftedResultsTuple, shift_names)

        if widepanel:
            wps = []
            for shift_name, shift_set, shifted_ctx in zip(shift_names, shift_sets, shifted_ctxs):
                wp_dict = {}
                for node_var_name, df_builder in zip(node_var_names, df_builders):
                    wp_dict[node_var_name] = df_builder.get_dataframe(shifted_ctx)
                wp = pd.WidePanel.from_dict(wp_dict)

                if has_shifts:
                    wp = WidePanelWithShiftSet(wp, shift_name, shift_set)
                wps.append(wp)

            if len(wps) == 1:
                return wps[0]
            return tuple_ctr(*wps)

        # list a list of lists of dataframes
        # [[dfs for one shift set], [dfs for next shift set], ...]
        df_lists = []
        for shift_name, shift_set, shifted_ctx in zip(shift_names, shift_sets, shifted_ctxs):
            dfs = []
            for df_builder in df_builders:
                df = df_builder.get_dataframe(shifted_ctx)
                if has_shifts:
                    df = DataFrameWithShiftSet(df, shift_name, shift_set)
                dfs.append(df)
            df_lists.append(dfs)

        if single_df:
            # flatten into a single list (there should be one dataframe per shift)
            dfs = reduce(operator.add, df_lists, [])
            if len(dfs) == 1:
                return dfs[0]
            return tuple_ctr(*dfs)

        if len(df_lists) == 1:
            return df_lists[0]
        return tuple_ctr(*df_lists)

    @line_magic
    def mdf_df(self, parameter_s=""):
        """
        Return a pandas dataframe of nodes evaluated over a date range.

        %mdf_df <start_date> <end_date> [nodes...] [[node=shift,...]]
        
        If no nodes are specified and the viewer is active
        the currently selected nodes are used.
        """
        return self._magic_dataframe(parameter_s, widepanel=False, single_df=True)

    @line_magic
    def mdf_dfs(self, parameter_s=""):
        """
        Return a list of pandas dataframes of nodes evaluated over a date range.

        %mdf_dfs <start_date> <end_date> [nodes...] [[node=shift,...]]
        
        If no nodes are specified and the viewer is active
        the currently selected nodes are used.
        """
        return self._magic_dataframe(parameter_s, widepanel=False, single_df=False)

    @line_magic
    def mdf_wp(self, parameter_s=""):
        """
        Return a pandas widepanel of nodes evaluated over a date range.

        %mdf_wp <start_date> <end_date> [nodes...] [[node=shift,...]]
        
        If no nodes are specified and the viewer is active
        the currently selected nodes are used.
        """
        return self._magic_dataframe(parameter_s, widepanel=True)

    @line_magic
    def mdf_xl(self, parameter_s=""):
        """
        Export to excel a list of nodes evaluated over a date range, or DataFrames.

        %mdf_xl <start_date> <end_date> [nodes...]

        If no nodes are specified and the viewer is active
        the currently selected nodes are used.
        
        Alternatively, export one or more DataFrames directly:
        %mdf_xl df1 [, dfN ...]
        """

        args = tokenize(parameter_s)
        args = [_try_eval(x, self.shell.user_global_ns, self.shell.user_ns) for x in args]
        if not args:
            raise AssertionError("Usage: %mdf_xl <start> <end> nodes...")

        dfs = []
        # if there is at least one DataFrame at the beginning, export them directly
        if args and isinstance(args[0], pd.DataFrame):
            dfs.extend((x for x in args if isinstance(x, pd.DataFrame)))

        else:
            # create one DataFrame of nodes evaluated over a date range
            dfs.append(self.mdf_df(self, parameter_s))

        excel.export_dataframe(dfs)

        # return the DataFrame if there is only 1, the complete list otherwise. can be [].
        if len(dfs) == 1:
            return dfs[0]
        return dfs

    @line_magic
    def mdf_plot(self, parameter_s=""):
        """
        Plot list of nodes evaluated over a date range

        %mdf_xl <start_date> <end_date> [nodes...]

        If no nodes are specified and the viewer is active
        the currently selected nodes are used.
        """
        df = self.mdf_df(parameter_s)
        df.plot()

    @line_magic
    def mdf_vars(self, parameter_s=""):
        """
        Print the values of varnodes a node or list of nodes
        are dependent on.

        %mdf_vars [<node>] [[category,...]]

        If no nodes are specified all nodes that are currently
        known about will be examined.
        """
        categories = None
        if parameter_s.strip().endswith("]") and "[" in parameter_s:
            parameter_s, categories = parameter_s.rstrip("]").rsplit("[", 1)
            categories = [x.strip() for x in categories.strip().split(",")]

        parameter_s = parameter_s.strip()
        nodes = parameter_s.strip().split(" ") if parameter_s else []
        nodes = map(lambda x: eval(x, self.shell.user_global_ns, self.shell.user_ns), nodes)

        curr_ctx = _get_current_context()

        for node in nodes:
            if not (node.has_value(curr_ctx) or node.was_called(curr_ctx)):
                _log.warn("%s has not yet been evaluated" % node.name)

        # get all the varnode values
        varnode_values = {}

        def vistor(node, ctx):
            if isinstance(node, MDFVarNode) \
                and ctx is curr_ctx \
                and node is not now:
                varnode_values[node] = ctx[node]
            return True

        curr_ctx.visit_nodes(vistor,
                             root_nodes=nodes or None,
                             categories=categories or None)

        # put the results in a dataframe with the ctx ids as columns
        nodes = sorted(varnode_values.keys(), key=lambda x: (sorted(x.categories), x.short_name))
        df = pd.DataFrame(data={}, index=nodes, columns=["Value", "Category"], dtype=object)
        for node, value in varnode_values.items():
            df["Value"][node] = value
            df["Category"][node] = ",".join(["%s" % (c or "") for c in sorted(node.categories)])

        if df.index.size == 0:
            print ("No matching dependencies found - has the node been evaluated?")
            return

        df.index = [n.short_name for n in df.index]
        print (df.to_string(float_format=lambda x: "%.3f" % x))


def _parse_datetime(arg, global_ns={}, local_ns={}):
    # If arg is an int smaller than 1e10 then it is likely a date
    if isinstance(arg, int) and arg < 30000000:
        return datetime(arg / 10000, (arg / 100) % 100, arg % 100)

    # Attempt to use the standard datetime parsing
    parsed = datetools.to_datetime(arg)
    if isinstance(parsed, datetime):
        return parsed

    # if the parsing failed, try to evaluate the string
    try:
        dt = eval(arg, global_ns, local_ns)
        return datetools.to_datetime(dt)
    except NameError:
        pass

    raise TypeError("Supplied parameter value '%s' can not be converted to datetime." % arg)


def mdf_pylab_help():
    print ("""
    === MDF Pylab ===

    MDF supports interactive evaluation by creating an 'ambient' context.
    All the standard mdf functions continue to work and it is possible to
    evaluate nodes without explicitly specifying a context.
    In addition, there are several 'magic functions' provided as shortcuts:

    %mdf_ctx [new_ctx] : gets the current context or makes another context active

    %mdf_now [date] : gets the current context date or sets it

    %mdf_df <start> <end> [nodes...] : return a dataframe of nodes evaluated over a date range

    %mdf_wp <start> <end> [nodes...] : return a widepanel of nodes evaluated over a date range

    %mdf_dfs <start> <end> [nodes...] : return a list of dataframes of nodes evaluated
                                        over a date range

    %mdf_plot <start> <end> [nodes...] : plot nodes evaluated over a date range

    %mdf_xl <start> <end> [nodes...]: export to excel nodes evaluated over a date range 
                                      
    %mdf_xl dataframe1 [dataframeN...]: export to excel one or more dataframes

    %mdf_evalto <end> node : evaluate the given node between the current time and the
                             specified end date. Returns the results as a numpy.array

    %mdf_reset [date] : resets the current context, replacing it with a blank context set to
                        the optional date.

    %mdf_timestep [offset] : sets and/or displays the current time step to use when advancing
                             time for the current context. offset can be a string like 'EOM'

    %mdf_advance [node] : advance the current date by timestep and return value of node

    %mdf_vars [nodes...] [[category,...] : show any varnode dependencies

    %mdf_help() : displays this help text.

    --- MDF Viewer Magic functions ---
    
    %mdf_show [nodes...] : show nodes in an mdf viewer window

    %mdf_selected : return list of (ctx, node) pairs for each node selected in the mdf viewer

    Example
    --------
    from mdf.lab import *
    import random

    @evalnode
    def random_value():
        while True:
            yield random.random()

    # get a dataframe of values
    %mdf_df 2001-01-01 2012-01-01 random_value
    df = _

    # plot some values
    %mdf_plot 2001-01-01 2012-01-01 random_value

    # open the mdf viewer
    %mdf_show random_value

    # reset
    %mdf_reset "1999-5-1"
    # Context reset to <ctx 1: 1991-05-01>

    %mdf_ctx
    # <ctx 1: 1991-05-01>
    """)


_loaded = False


def load_ipython_extension(ip):
    global _loaded
    if not _loaded:
        ip.register_magics(MDFMagics)

        # create the ambient context
        today = datetools.normalize_date(datetime.now())
        ctx = MDFContext(today)
        ctx._activate_ctx()

        _loaded = True

        print ("""Use the magic function %mdf_help for a list of commands""")


def _clean_varname(s):
    """get a valid variable name from a string"""
    # Remove invalid characters
    s = re.sub("[^0-9a-zA-Z_]", "_", s)

    # Remove leading characters until we find a letter or underscore
    s = re.sub("^[^a-zA-Z_]+", "", s)

    return s


def _try_eval(x, globals_, locals_):
    """return eval(x) or x"""
    try:
        return eval(x, globals_, locals_)
    except:
        return x


def _get_shift_sets(arg_name, shifts):
    """
    takes a string passed to a magic function,
        eg "[shift_set_1,shift_set_2,{a:2}"

    and it's evaluated form and returns a list of
    shift sets: (name, shift_dict)
    """
    if not isinstance(shifts, (dict, tuple, list)):
        raise Exception("Couldn't convert %s to a shift set" % arg_name)

    shift_sets = []

    if isinstance(shifts, dict):
        # the shifts parameter can be a single dict
        shift_sets.append((_clean_varname(arg_name), shifts))
        return shift_sets

    # or a collection of dicts

    # try and get the names of the shifts
    shift_names = [None] * len(shifts)
    if arg_name.startswith("[") and arg_name.endswith("]"):
        tokens = tokenize(arg_name.strip("[]"))
        if len(tokens) == len(shift_names):
            shift_names = map(_clean_varname, tokens)

    for shift_name, shift in zip(shift_names, shifts):
        # normally the shifts are a list of dicts
        if isinstance(shift, dict):
            if shift_name is None:
                shift_name = "_%d" % len(shift_sets)
            shift_sets.append((shift_name, shift))

        # but we also allow lists of lists of dicts
        elif isinstance(shift, (list, tuple)):
            for i, x in enumerate(shift):
                inner_shift_name = shift_name
                if inner_shift_name is None:
                    inner_shift_name = "_%d" % len(shift_sets)
                else:
                    inner_shift_name = "%s_%d" % (shift_name, i)

                if not isinstance(x, dict):
                    raise AssertionError("shift %s was expected to be a dict, "
                                         "but it's a %s (%s)" % (
                                             inner_shift_name, type(x), x))

                shift_sets.append((inner_shift_name, x))

        # anything else we can't deal with
        else:
            if shift_name is None:
                shift_name = "_%d" % len(shift_sets)
            raise AssertionError("shift %s was expected to be a dict, but it's a %s (%s)" % (
                shift_name, type(shift), shift))

    return shift_sets


# add a hook for ipython to automatically register the mdf ipython
# plugin when mdf.lab is imported
def _ipython_active():
    """return true if in an active IPython session"""
    try:
        import IPython
    except ImportError:
        return False

    try:
        if IPython.__version__ >= "0.12":
            return __IPYTHON__
        return __IPYTHON__active
    except NameError:
        return False

if _ipython_active():
    ip = get_ipython()
    load_ipython_extension(ip)
