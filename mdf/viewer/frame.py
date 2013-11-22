"""
The main wx frame for the mdf viewer
"""
from ..nodes import MDFNode
from ..context import MDFContext
import wx
import wx.py
import wx.lib.agw.aui as aui
import wx.gizmos as gizmos
import os
import sys
import logging
import pandas as pd
import numpy as np
import matplotlib.pyplot
from datetime import datetime, timedelta
from collections import deque
from ..runner import run
from ..builders import DataFrameBuilder
from ..nodes import MDFVarNode
import excel
import traceback

_logger = logging.getLogger(__name__)

from panels import (
    NodeProperties,
    NodeValueViewer,
    CodeViewer,
    DAGViewer,
    PlotPanel
)

from dialogs import (
    RunToDialog,
    DAGViewerOptions,
    FiltersDialog
)

from common import to_wxdate, load_icon, load_bitmap

# locals dict for new pycrust sessions
_default_pycrust_locals = {
    "datetime"          : datetime,
    "pa"                : pd,
    "pandas"            : pd,
    "np"                : np,
    "numpy"             : np,
    "pyplot"            : matplotlib.pyplot,
    "export_dataframe"  : excel.export_dataframe,
}

_pycrust_intro = """
MDF Viewer Python Shell

MDF Viewer specific functions:

get_selected     : returns the context and nodes selected
get_dataframes   : build dataframes from a list of contexts and nodes
get_ctx          : get the root mdf context
export_dataframe : export a dataframe to Excel
show             : add an mdf node to the root of the displayed tree

use help(...) for more help on any functions
use ctrl+up/down to scroll through your command history
""".lstrip()


class ViewerRootNode(MDFVarNode):
    """
    specialized node for refreshing the tree view when it's dirtied
    """

    def __init__(self, frame, name, default=None):
        MDFVarNode.__init__(self, name, default=default)
        self.frame = frame

        if frame:
            self.timer = wx.Timer(frame)
            frame.Bind(wx.EVT_TIMER, frame.RefreshAll, self.timer)

    def on_set_dirty(self, ctx, flags):
        if self.frame:
            # refresh the tree when the event loop continues to allow
            # all other nodes to get marked as dirty before refreshing
            try:
                self.timer.Start(oneShot=True)
            except wx.PyDeadObjectError:
                self.frame = None
        self.touch(ctx)


class TreeNode(object):
    """
    TreeNodes represent a point in the (virtual) tree, produced
    as a result of traversing the DAG.
    
    Each node has an id which uniquely describes its location in
    the tree.
    
    The same (node, ctx) pair may appear at multiple locations
    in the tree.

    This class is only used for searching in the tree.
    """

    @classmethod
    def create_from_item(cls, tree, item, ctx_filter, category_filter):
        # create the parent
        parent = None
        parent_item = tree.GetItemParent(item)
        if parent_item.IsOk():
            parent = cls.create_from_item(tree, parent_item, ctx_filter, category_filter) 

        # get the sibling id
        sibling_id = 0
        prev_sibling = tree.GetPrevSibling(item)
        while prev_sibling.IsOk():
            sibling_id += 1
            prev_sibling = tree.GetPrevSibling(prev_sibling)

        # return the node
        ctx, node = tree.GetPyData(item) 
        return cls(node, ctx, parent, sibling_id, ctx_filter, category_filter)

    def __init__(self, node, ctx, parent, sibling_id, ctx_filter, category_filter):
        """call create_from_item instead"""
        self.node = node
        self.ctx = ctx
        self.parent = parent
        self.ctx_filter = ctx_filter
        self.category_filter = category_filter
        self.id = (parent.id if parent else []) + [sibling_id]

    @property
    def children(self):
        """yields children TreeNodes"""
        dependencies = self.node.get_dependencies(self.ctx)
        dependencies = _apply_filters(dependencies, self.ctx_filter, self.category_filter)
        dependencies = sorted(dependencies, key=lambda (node, ctx): (node.name, ctx.get_id()))
        for i, (node, ctx) in enumerate(dependencies):
            yield TreeNode(node, ctx, self, i, self.ctx_filter, self.category_filter)

    @property
    def next_sibling(self):
        """returns the next sibling to this one, or None"""
        if not self.parent:
            return None

        g = self.parent.children
        try:
            while True:
                child = g.next()
                if child == self:
                    return g.next()
        except StopIteration:
            return None

    def __cmp__(self, other):
        return cmp(self.id, other and other.id)

    def __eq__(self, other):
        return other and self.id == other.id

    def __ne__(self, other):
        return not other or self.id != other.id


def _apply_filters(nodes_and_ctxs, ctx_filter, category_filter):
    """
    takes a list of nodes and contexts and removes nodes that don't match the
    filters and adds any children of those nodes that do match.
    """
    if ctx_filter or category_filter:
        unfiltered = list(nodes_and_ctxs)
        nodes_and_ctxs = []
        seen = set()
        while unfiltered:
            node, ctx = unfiltered.pop()
            if (node, ctx) in seen:
                continue
            seen.add((node, ctx))

            shift_set = ctx.get_shift_set()
            categories = node.categories
            include = True

            # check if context is included
            if ctx_filter is not None:
                if not shift_set:
                    if None not in ctx_filter:
                        include = False
                else:
                    # check if shift set is included
                    for key, value in shift_set.iteritems():
                        if key in ctx_filter and value in ctx_filter[key]:
                            break
                    else:
                        include = False

            # check at least one of the node categories is in the filter
            if include and category_filter is not None:
                if not categories and None not in category_filter:
                    include = False
                else:
                    for category in categories:
                        if category in category_filter:
                            break
                        else:
                            include = False

            # if both filters passed add it to the list and continue to the next one
            if include:
                nodes_and_ctxs.append((node, ctx))
                continue

            # add this nodes dependencies to see if any of those match the filter
            unfiltered.extend(node.get_dependencies(ctx))

    return nodes_and_ctxs


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


def _pydev_attach():
    """attempt to connect or re-connect to the pydev debugger"""
    import threading
    import time
    import glob

    try:
        import pydevd
        import pydevd_tracing
    except ImportError:
        # look for a PyDev src folder
        program_files = os.environ.get("PROGRAMFILES", r"C:\Program Files (x86)")
        pattern = r"%s\Eclipse\plugins\org.python.pydev.debug_*\pysrc" % program_files
        folders = sorted(glob.glob(pattern), reverse=True)
        if folders:
            if folders[0] not in sys.path:
                sys.path.append(folders[0])

        # try to import again
        import pydevd
        import pydevd_tracing

    if not _ipython_active():
        # remove any redirection
        if getattr(sys, "_pydev_orig_stdout", None) is None:
            sys._pydev_orig_stdout = sys.stdout
        if getattr(sys, "_pydev_orig_stderr", None) is None:
            sys._pydev_orig_stderr = sys.stderr

        sys.stdout = sys._pydev_orig_stdout
        sys.stderr = sys._pydev_orig_stderr

    # stop any existing debugger
    dbg = pydevd.GetGlobalDebugger()
    if dbg:
        dbg.FinishDebuggingSession()
        time.sleep(0.1)
        pydevd_tracing.SetTrace(None)

        # remove any additional info for the current thread
        try:
            del threading.currentThread().__dict__["additionalInfo"]
        except KeyError:
            pass

        pydevd.SetGlobalDebugger(None)
        pydevd.connected = False
        time.sleep(0.1)

    # do this a couple of times as when re-connecting the first doesn't work
    _logger.info("Attempting to attach to the pydev debugger")
    if not ipython_active():
        pydevd.settrace(stdoutToServer=True, stderrToServer=True, suspend=False)
        pydevd.settrace(stdoutToServer=True, stderrToServer=True, suspend=False)
    else:
        pydevd.settrace(stdoutToServer=False, stderrToServer=False, suspend=False)
        pydevd.settrace(stdoutToServer=False, stderrToServer=False, suspend=False)
    _logger.info("Attached to PyDev")


class MDFViewerFrame(wx.Frame):

    _columns = [
        "__reserved__",
        "Context",
        "Value",
        "Type",
        "Category",
        "Module",
    ]

    class UpdatingGuard(object):
        """object that stops a viewer frame updating"""
        def __init__(self, frame):
            self._frame = frame
            
        def __enter__(self):
            self._frame._updating = True
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            self._frame._updating = False
            self._frame.RefreshAll()

        def enable_updates(self, enable=True):
            if enable:
                self._frame._updating = False
                self._frame.RefreshAll()
            else:
                self._frame._updating = True

    def __init__(self, parent, id=wx.ID_ANY, title="", pos= wx.DefaultPosition,
                 size=wx.DefaultSize, style=wx.DEFAULT_FRAME_STYLE):
        wx.Frame.__init__(self, parent, id, title, pos, size, style)

        icon = load_icon("frame.ico")
        self.SetIcon(icon)

        self._root_node = ViewerRootNode(self, "_mdf_viewer_root_")
        self._filename = None
        self._dag_viewer = None
        self._properties = None
        self._code_viewer = None
        self._value_viewer = None
        self._date_picker = None
        self._updating = False
        self._last_search_item = None
        self._ctx_filter = None
        self._cat_filter = None

        self._pycrust = None
        self._pycrust_locals = {
            "__mdf_frame__"     : self,
            "get_selected"      : self.GetSelectedContextsAndNodes,
            "get_ctx"           : self.GetRootContext,
            "get_dataframes"    : self.GetDataFrames,
            "show"              : self.AddNodeToRoot,
        }
        self._pycrust_locals.update(_default_pycrust_locals)

        self._last_selected_start_date = datetime.now()
        self._last_selected_end_date = datetime.now()

        # tell AuiManager to manage this frame
        self._mgr = aui.AuiManager()
        self._mgr.SetManagedWindow(self)

        self.LoadImages()
        self.BuildToolBars()
        self.BuildPanes()
        self.CreateMenuBar()

        self.CreateStatusBar()
        self.GetStatusBar().SetStatusText("Ready")

    def SetDefaultDateRange(self, start_date=None, end_date=None):
        """
        Sets the defaults for the date range control.
        These are also set when the graph is loaded.
        """
        if start_date is not None:
            self._last_selected_start_date = start_date
        if end_date is not None:
            self._last_selected_end_date = end_date

    def CreateMenuBar(self):
        mb = wx.MenuBar()
        self.menu_ids = {}

        file_menu = wx.Menu()
        open = file_menu.Append(wx.NewId(), "&Open...")
        save = file_menu.Append(wx.NewId(), "&Save")
        save_as = file_menu.Append(wx.NewId(), "Save As...")
        file_menu.AppendSeparator()
        exit = file_menu.Append(wx.ID_EXIT, "&Exit")
        mb.Append(file_menu, "&File")

        self.Bind(wx.EVT_MENU, self.OnOpen, id=open.Id)
        self.Bind(wx.EVT_MENU, self.OnSave, id=save.Id)
        self.Bind(wx.EVT_MENU, self.OnSaveAs, id=save_as.Id)
        self.Bind(wx.EVT_MENU, self.OnExit, id=wx.ID_EXIT)

        tools_menu = wx.Menu()
        pydev_attach = tools_menu.Append(wx.NewId(), "Attach to PyDev")
        pycrust = tools_menu.Append(wx.NewId(), "Open PyCrust")
        mb.Append(tools_menu, "&Tools")

        self.Bind(wx.EVT_MENU, self.OnPyDevAttach, id=pydev_attach.Id)
        self.Bind(wx.EVT_MENU, self.OnOpenPyCrust, id=pycrust.Id)

        windows_menu = wx.Menu()
        node_props = windows_menu.AppendCheckItem(wx.NewId(), "Properties")
        node_source = windows_menu.AppendCheckItem(wx.NewId(), "Source")
        node_value = windows_menu.AppendCheckItem(wx.NewId(), "Value")
        mb.Append(windows_menu, "&Windows")

        mb.Check(node_props.Id, True)
        mb.Check(node_source.Id, True)
        mb.Check(node_value.Id, True)

        self.Bind(wx.EVT_MENU, self.OnWindowsProperties, id=node_props.Id)
        self.Bind(wx.EVT_MENU, self.OnWindowsSource, id=node_source.Id)
        self.Bind(wx.EVT_MENU, self.OnWindowsValue, id=node_value.Id)

        self.menu_ids = {
            "file_open"             : open.Id,
            "file_exit"             : exit.Id,
            "windows_properties"    : node_props.Id,
            "windows_source"        : node_source.Id,
            "windows_value"         : node_value.Id,
            "pydev_attach"          : pydev_attach.Id,
        }
        
        self.SetMenuBar(mb)

    def BuildPanes(self):
        self.Bind(aui.EVT_AUI_PANE_CLOSE, self.OnPaneClose)

        self._mgr.AddPane(self.CreateTreeCtrl(), aui.AuiPaneInfo().
                                                    Name("DAG Viewer").
                                                    CenterPane().
                                                    PaneBorder(False))

        self.CreatePropertiesPanel()
        self.CreateCodePanel()
        self.CreateValuePanel()
        self._mgr.Update()

    def BuildToolBars(self):
        # create the toolbar
        tb = aui.AuiToolBar(self, -1, wx.DefaultPosition, wx.DefaultSize, style=wx.EXPAND)
        tb.SetToolBitmapSize(wx.Size(48, 48))

        # add the file controls
        open = tb.AddSimpleTool(wx.NewId(),
                                    "Open",
                                    wx.ArtProvider.GetBitmap(wx.ART_FILE_OPEN),
                                    "Open...")

        save = tb.AddSimpleTool(wx.NewId(),
                                    "Save",
                                    wx.ArtProvider.GetBitmap(wx.ART_FILE_SAVE),
                                    "Save")

        save_as = tb.AddSimpleTool(wx.NewId(),
                                    "Save As...",
                                    wx.ArtProvider.GetBitmap(wx.ART_FILE_SAVE_AS),
                                    "Save As...")

        self.Bind(wx.EVT_MENU, self.OnOpen, id=open.id)
        self.Bind(wx.EVT_MENU, self.OnSave, id=save.id)
        self.Bind(wx.EVT_MENU, self.OnSaveAs, id=save_as.id)

        # add the run controls
        tb.AddSpacer(5)
        tb.AddSeparator()
        tb.AddSpacer(5)
        self._date_picker = wx.DatePickerCtrl(tb,
                                        size=(120, -1),
                                        style=wx.DP_DROPDOWN|wx.DP_SHOWCENTURY|wx.DP_ALLOWNONE)

        date = tb.AddControl(self._date_picker)
        tb.SetToolShortHelp(date, "Current evaluation date ('now')")
    
        next_icon = load_bitmap("step.ico", 16, 16)
        next = tb.AddSimpleTool(wx.NewId(),
                                    "Next",
                                    next_icon,
                                    "Step to next date")
        
        run_icon = load_bitmap("run.ico", 16, 16)
        run = tb.AddSimpleTool(wx.NewId(),
                                "Run To...",
                                run_icon,
                                "Run to a specific date...")

        refresh = tb.AddSimpleTool(wx.NewId(),
                                    "Recalculate",
                                    wx.ArtProvider.GetBitmap(wx.ART_REDO),
                                    "Evaluate all nodes for current date")
        self.refreshId = refresh.id

        self.Bind(wx.EVT_DATE_CHANGED, self.OnDateChanged, id=self._date_picker.Id)
        self.Bind(wx.EVT_MENU, self.OnRecalculate, id=refresh.id)
        self.Bind(wx.EVT_MENU, self.OnNext, id=next.id)
        self.Bind(wx.EVT_MENU, self.OnRun, id=run.id)

        # add the filter icon
        filter_icon = load_bitmap("filter.ico", 16, 16)
        filter = tb.AddSimpleTool(wx.NewId(),
                                    "Filter...",
                                    filter_icon,
                                    "Set filters...",
                                    aui.ITEM_CHECK)
        self.Bind(wx.EVT_MENU, self.OnSetFilters, id=filter.id)

        # add the search control with a spacer so it's on the right
        tb.SetToolBitmapSize(wx.Size(48, 48))
        search = wx.SearchCtrl(tb, size=(200, -1))
        tb.AddStretchSpacer()
        tb.AddControl(search)

        self.Bind(wx.EVT_SEARCHCTRL_SEARCH_BTN, self.OnSearch, id=search.Id)
        self.Bind(wx.EVT_TEXT_ENTER, self.OnSearch, id=search.Id)

        tb.Realize()
        self._mgr.AddPane(tb, aui.AuiPaneInfo()
                                    .Name("toolbar")
                                    .ToolbarPane()
                                    .Top()
                                    .Resizable())

        self._toolbar = tb
        self._mgr.Update()

    def LoadImages(self):
        self.imglist = wx.ImageList(16, 16, True, 2)
        self.FOLDER = self.imglist.Add(wx.ArtProvider.GetBitmap(wx.ART_FOLDER, wx.ART_OTHER, wx.Size(16, 16)))
        self.NORMAL_FILE = self.imglist.Add(wx.ArtProvider.GetBitmap(wx.ART_NORMAL_FILE, wx.ART_OTHER, wx.Size(16, 16)))

    def CreateTreeCtrl(self):
        tree = gizmos.TreeListCtrl(self, -1, style=wx.TR_DEFAULT_STYLE|wx.TR_MULTIPLE)

        imglist = wx.ImageList(16, 16, True, 2)
        imglist.Add(wx.ArtProvider.GetBitmap(wx.ART_FOLDER, wx.ART_OTHER, wx.Size(16, 16)))
        imglist.Add(wx.ArtProvider.GetBitmap(wx.ART_NORMAL_FILE, wx.ART_OTHER, wx.Size(16, 16)))
        tree.AssignImageList(imglist)

        tree.AddColumn("Name")
        for c in self._columns[1:]:
            tree.AddColumn(c)
        tree.SetMainColumn(0)
        tree.SetColumnWidth(0, 250)

        root = tree.AddRoot("No DAG loaded", 0)            
        tree.Expand(root)

        self._dag_tree = tree

        self.__collapsing = False
        self.Bind(wx.EVT_TREE_ITEM_EXPANDING, self.OnTreeItemExpanding, id=tree.Id)
        self.Bind(wx.EVT_TREE_ITEM_COLLAPSING, self.OnTreeItemCollapsing, id=tree.Id) 
        self.Bind(wx.EVT_TREE_SEL_CHANGED, self.OnTreeSelectionChanged, id=tree.Id)
        self.Bind(wx.EVT_TREE_ITEM_RIGHT_CLICK, self.OnTreeItemMenu, id=tree.Id)

        return tree

    def CreatePropertiesPanel(self):
        self._properties = NodeProperties(self)
        self._mgr.AddPane(self._properties, aui.AuiPaneInfo().
                                                Caption("Node Properties").
                                                Right())

        return self._properties


    def CreateValuePanel(self):
        self._value_viewer = NodeValueViewer(self)
        self._mgr.AddPane(self._value_viewer, aui.AuiPaneInfo().
                                                Caption("Node Value").
                                                Right())

        return self._value_viewer

    def CreateCodePanel(self):
        self._code_viewer = CodeViewer(self)
        self._mgr.AddPane(self._code_viewer, aui.AuiPaneInfo().
                                                Caption("Node Source").
                                                Bottom())
        return self._code_viewer

    def CreateDAGViewer(self):
        self._dag_viewer = DAGViewer(self)
        self._mgr.AddPane(self._dag_viewer, aui.AuiPaneInfo().
                                                Caption("DAG Viewer").
                                                Center().
                                                Float().
                                                FloatingSize((800, 600)))
        return self._dag_viewer

    def SetNode(self, node, ctx):
        if node is self._root_node:
            return

        if self._properties:
            self._properties.SetNode(node, ctx)
        if self._code_viewer:
            self._code_viewer.SetNode(node)
        if self._value_viewer:
            self._value_viewer.SetNode(node, ctx)
            pane = self._mgr.GetPane(self._value_viewer)
            caption = "Node Value (%s)" % self._value_viewer.GetCurrentValueType()
            if caption != pane.caption:
                pane.caption = caption
                self._mgr.Update()

    def SetNodeToCurrentSelection(self):
        items = self._dag_tree.GetSelections()
        if len(items) != 1:
            return
        data = self._dag_tree.GetPyData(items[0])
        if data is None:
            return
        ctx, node = data
        self.SetNode(node, ctx)

    def GetSelectedContextsAndNodes(self):
        """
        returns a list of (ctx, node)
        for use from PyCrust as 'get_selected()'
        """
        items = self._dag_tree.GetSelections()
        ctxs_and_nodes = []
        for item in items:
            data = self._dag_tree.GetPyData(item)
            if not data:
                return
            ctx, node = data
            if (ctx, node) not in ctxs_and_nodes:
                ctxs_and_nodes.append((ctx, node))
        return ctxs_and_nodes

    def GetRootContext(self):
        """
        returns the root context
        for use from PyCrust as 'get_ctx()'
        """
        tree = self._dag_tree
        data = tree.GetPyData(tree.GetRootItem())
        if not data:
            return
        ctx, node = data
        return ctx

    def GetDataFrames(self, ctxs_and_nodes, start=None, end=None):
        """
        compute a dataframe for each of the (ctx, node) pairs between
        start and end.
        If start or end are None the currently selected dates will be used.
        for use from PyCrust as 'get_dataframes()'
        """
        if isinstance(ctxs_and_nodes, tuple) \
        and len(ctxs_and_nodes) == 2:
            ctxs_and_nodes = [ctxs_and_nodes]

        contexts, nodes = zip(*ctxs_and_nodes)
        df_builder = DataFrameBuilder(nodes, contexts=contexts, filter=True)

        if start is None:
            start = self._last_selected_start_date
        self._last_selected_start_date = start

        if end is None:
            end = self._last_selected_end_date
        self._last_selected_end_date = end

        date_range = pd.bdate_range(start, end)
        root_ctx = self.GetRootContext()

        # create a progress dialog
        maximum = max(1, (end - start).days)
        progress_dlg = wx.ProgressDialog("Please wait...",
                                         "Calculating nodes",
                                         maximum=maximum,
                                         style=wx.PD_CAN_ABORT
                                                | wx.PD_ELAPSED_TIME
                                                | wx.PD_REMAINING_TIME
                                                | wx.PD_AUTO_HIDE)

        # callback called by 'run' to update the progress dialog
        def callback(date, ctx):
            days_left = (date - start).days
            keep_going, skip = progress_dlg.Update(days_left)
            if not keep_going:
                raise StopIteration

        try:
            with self.UpdatingGuard(self):
                run(date_range, [callback, df_builder], ctx=root_ctx)
        finally:
            progress_dlg.Update(maximum)

        return df_builder.dataframe

    def OnDateChanged(self, event):
        # update now in the context
        tree = self._dag_tree
        data = tree.GetPyData(tree.GetRootItem())
        if not data:
            return

        # get the date from the event
        date = event.GetDate()
        try:
            date = datetime(date.GetYear(), date.GetMonth()+1, date.GetDay())
        except ValueError:
            return

        # if the date is a weekend don't set the date and disable the
        # calculate button
        is_weekday = date.weekday() <= 4
        enabled = self._toolbar.GetToolEnabled(self.refreshId)
        if enabled != is_weekday:
            self._toolbar.EnableTool(self.refreshId, is_weekday)
            self._toolbar.Refresh()

        # and set it on the context
        ctx, node = data
        
        self.GetStatusBar().SetStatusText("Setting date...")
        try:
            ctx.set_date(date)
            self.GetStatusBar().SetStatusText("Ready")
        except Exception, e:
            traceback.print_tb(sys.exc_traceback)
            self.GetStatusBar().SetStatusText("An error occurred: %s" % e)
            raise

    def OnOpenPyCrust(self, event):
        if self._pycrust is None:
            self._pycrust = wx.py.crust.Crust(self,
                                                locals=self._pycrust_locals,
                                                intro=_pycrust_intro)

            self._mgr.AddPane(self._pycrust, aui.AuiPaneInfo().
                                                    Caption("PyCrust").
                                                    Center().
                                                    Float().
                                                    FloatingSize((1000, 800)))
        self._mgr.Update()

    def OnPyDevAttach(self, event):
        try:
            _pydev_attach()
        except Exception, e:
            wx.MessageBox("Unable to attach to PyDev: %s" % e)

    def OnPaneClose(self, event):
        # don't do anything if a panel is being minimized
        if event.GetEventType() == aui.wxEVT_AUI_PANE_MINIMIZE:
            return

        windows = [event.pane.window]
        if event.pane.IsNotebookControl():
            windows = list(event.pane.window.Children)

        for window in windows:
            if window == self._properties:
                self._properties = None
                self.GetMenuBar().Check(self.menu_ids["windows_properties"], False)
            elif window == self._code_viewer:
                self._code_viewer = None
                self.GetMenuBar().Check(self.menu_ids["windows_source"], False)
            elif window == self._value_viewer:
                self._value_viewer = None
                self.GetMenuBar().Check(self.menu_ids["windows_value"], False)
            elif window == self._dag_viewer:
                self._dag_viewer = None
            elif window == self._pycrust:
                self._pycrust = None

    def OnWindowsProperties(self, event):
        self.GetMenuBar().Check(event.GetId(), True)

        if not self._properties:
            self.CreatePropertiesPanel()
            self.SetNodeToCurrentSelection()
            self._mgr.Update()

    def OnWindowsSource(self, event):            
        self.GetMenuBar().Check(event.GetId(), True)

        if not self._code_viewer:
            self.CreateCodePanel()
            self.SetNodeToCurrentSelection()
            self._mgr.Update()

    def OnWindowsValue(self, event):
        self.GetMenuBar().Check(event.GetId(), True)

        if not self._value_viewer:
            self.CreateValuePanel()
            self.SetNodeToCurrentSelection()
            self._mgr.Update()

    def ShowDag(self, nodes, ctx):
        dlg = DAGViewerOptions(self)
        res = dlg.ShowModal()
        if res != wx.ID_OK:
            return

        self.GetStatusBar().SetStatusText("Rendering Graph...")
        if not self._dag_viewer:
            self.CreateDAGViewer()

        try:
            self._dag_viewer.SetNodes(nodes,
                                      ctx,
                                      max_depth=dlg.max_depth,
                                      layout_style=dlg.layout_style)
            self._mgr.Update()
            self.GetStatusBar().SetStatusText("Ready")
        except Exception, e:
            traceback.print_tb(sys.exc_traceback)
            self._dag_viewer.Close()
            msg = wx.MessageDialog(self, str(e), "An error occurred",  wx.OK | wx.ICON_ERROR)
            msg.ShowModal()
            msg.Destroy()
            self.GetStatusBar().SetStatusText("An error occurred: %s" % e)
            raise

    def Calculate(self):
        tree =self._dag_tree
        if tree is None:
            return

        # get the root node and context
        data = tree.GetPyData(tree.GetRootItem())
        if data is None:
            return
        root_ctx, root_node = data

        self.GetStatusBar().SetStatusText("Calculating...")
        errors = []
        try:
            # evaluate all the direct dependencies
            for node, ctx in root_node.get_dependencies(root_ctx):
                try:
                    ctx[node]
                except:
                    errors.append((node, ctx))
    
            # and mark the root node as clean
            root_ctx[self._root_node] = None
    
            # re-raise any exceptions
            for node, ctx in errors:
                ctx[node]
    
            self.GetStatusBar().SetStatusText("Ready")
        except Exception, e:
            traceback.print_tb(sys.exc_traceback)
            self.GetStatusBar().SetStatusText("An error occurred: %s" % e)
            self.RefreshAll()
            raise

        self.RefreshAll()

    def RefreshAll(self, event=None):
        tree =self._dag_tree
        if tree is None:
            return

        # get the root node and context
        data = tree.GetPyData(tree.GetRootItem())
        if data is not None:
            root_ctx, root_node = data
            date = root_ctx.get_date()
            self._date_picker.SetValue(to_wxdate(date))

            # if the date is a weekend don't set the date and disable the
            # calculate button
            if not self._updating:
                is_weekday = date.weekday() <= 4
                enabled = self._toolbar.GetToolEnabled(self.refreshId)
                if enabled != is_weekday:
                    self._toolbar.EnableTool(self.refreshId, is_weekday)
                    self._toolbar.Refresh()

        # if running or plotting don't update anything else yet
        if self._updating:
            return

        # reset any previous search item
        self._last_search_item = None

        tree.Freeze()
        try:
            items = [tree.GetRootItem()]
            while items:
                item = items.pop()
                self.RefreshItem(tree, item)
                if tree.IsExpanded(item):
                    child, cookie = tree.GetFirstChild(item)
                    while child.IsOk():
                        items.append(child)
                        child, cookie = tree.GetNextChild(item, cookie)
    
            self.SetNodeToCurrentSelection()
        finally:
            tree.Thaw()

    def RefreshItem(self, tree, item):
        # the root item is a dummy node and never needs to be refreshed
        if item == tree.GetRootItem():
            return

        data = tree.GetPyData(item)
        if data is None:
            return
        ctx, node = data

        if "Context" in self._columns:
            tree.SetItemText(item, str(ctx), self._columns.index("Context"))

        if "Module" in self._columns:
            tree.SetItemText(item, node.modulename or "", self._columns.index("Module"))

        if "Category" in self._columns:
            category = ", ".join([str(x) for x in node.categories if x])
            tree.SetItemText(item, category, self._columns.index("Category"))

        value_index = self._columns.index("Value")
        type_index = self._columns.index("Type")

        if node.is_dirty(ctx):
            tree.SetItemTextColour(item, wx.LIGHT_GREY) 

            if type_index >= 0:
                tree.SetItemText(item, "<stale value>", type_index)
            if value_index >= 0:
                tree.SetItemText(item, "<stale value>", value_index)
        else:
            tree.SetItemTextColour(item, wx.BLACK)

            if type_index >= 0:
                tree.SetItemText(item, str(type(ctx[node])), type_index)
            if value_index >= 0:
                tree.SetItemText(item, repr(ctx[node]), value_index)

        has_children = len(node.get_dependencies(ctx)) > 0
        tree.SetItemHasChildren(item, has_children)

    def ExportToExcel(self, nodes, ctxs):
        progress_dlg = None
        start_date = None
        end_date = None

        assert len(nodes) == len(ctxs)

        # check to see if there's a value, and if that value's a dataframe
        # then export that as it is
        if len(nodes) == 1 and not nodes[0].is_dirty(ctxs[0]):
            value = ctxs[0][nodes[0]]
            if isinstance(value, pd.DataFrame):
                try:
                    excel.export_dataframe(value)
                    return
                except Exception, e:
                    traceback.print_tb(sys.exc_traceback)
                    msg = wx.MessageDialog(self, str(e), "An error occurred",  wx.OK | wx.ICON_ERROR)
                    msg.ShowModal()
                    msg.Destroy()
                    raise

        # callback called by 'run'
        def callback(date, ctx):
            days_left = (date - start_date).days
            keep_going, skip = progress_dlg.Update(days_left)
            if not keep_going:
                raise StopIteration

        dlg = RunToDialog(self,
                            "Export to Excel...",
                            start_date=self._last_selected_start_date,
                            end_date=self._last_selected_end_date)

        dlg.CenterOnScreen()
        if wx.ID_OK == dlg.ShowModal():
            # create a date range inclusive of the end date
            start_date, end_date = dlg.GetDates()
            date_range = pd.bdate_range(start_date, end_date)

            self._last_selected_start_date = start_date
            self._last_selected_end_date = end_date

            # create a progress dialog
            maximum = max(1, (end_date - start_date).days)
            progress_dlg = wx.ProgressDialog("Please wait...",
                                             "Calculating nodes",
                                             maximum=maximum,
                                             style=wx.PD_CAN_ABORT
                                                    | wx.PD_ELAPSED_TIME
                                                    | wx.PD_REMAINING_TIME
                                                    | wx.PD_AUTO_HIDE)

            # create the dataframe builder
            df_builder = DataFrameBuilder(nodes, contexts=ctxs, filter=True)

            # and run through that date range using the builder
            self.GetStatusBar().SetStatusText("Calculating...")
            try:
                with self.UpdatingGuard(self):
                    run(date_range, [callback, df_builder], ctx=ctxs[0].get_parent() or ctxs[0])

                # export to Excel
                excel.export_dataframe(df_builder.dataframe)

            except StopIteration:
                pass
            except Exception, e:
                traceback.print_tb(sys.exc_traceback)
                msg = wx.MessageDialog(self, str(e), "An error occurred",  wx.OK | wx.ICON_ERROR)
                msg.ShowModal()
                msg.Destroy()

            progress_dlg.Update(maximum)
            self.GetStatusBar().SetStatusText("Ready")

        dlg.Destroy()
        self.RefreshAll()

    def Plot(self, nodes, ctxs):
        progress_dlg = None
        start_date = None
        end_date = None

        assert len(nodes) == len(ctxs)

        # callback called by 'run'
        def callback(date, ctx):
            days_left = (date - start_date).days
            keep_going, skip = progress_dlg.Update(days_left)
            if not keep_going:
                raise StopIteration

        dlg = RunToDialog(self,
                            "Plot...",
                            start_date=self._last_selected_start_date,
                            end_date=self._last_selected_end_date)

        dlg.CenterOnScreen()
        if wx.ID_OK == dlg.ShowModal():
            # create a date range inclusive of the end date
            start_date, end_date = dlg.GetDates()
            date_range = pd.bdate_range(start_date, end_date)

            self._last_selected_start_date = start_date
            self._last_selected_end_date = end_date

            # create a progress dialog
            maximum = max(1, (end_date - start_date).days)
            progress_dlg = wx.ProgressDialog("Please wait...",
                                             "Calculating nodes",
                                             maximum=maximum,
                                             style=wx.PD_CAN_ABORT
                                                    | wx.PD_ELAPSED_TIME
                                                    | wx.PD_REMAINING_TIME
                                                    | wx.PD_AUTO_HIDE)


            # create the dataframe builder
            df_builder = DataFrameBuilder(nodes, contexts=ctxs, filter=True)

            # and run through that date range using the builder
            self.GetStatusBar().SetStatusText("Calculating...")
            try:
                with self.UpdatingGuard(self):
                    run(date_range, [callback, df_builder], ctx=ctxs[0].get_parent() or ctxs[0])

                # create the new plot panel
                df = df_builder.dataframe.fillna(method="ffill")
                panel = PlotPanel(self, [df])
                self._mgr.AddPane(panel, aui.AuiPaneInfo().
                                            Caption("Plot").
                                            Center().
                                            Float().
                                            FloatingSize((800,600)))
                self._mgr.Update()
            except StopIteration:
                pass
            except Exception, e:
                traceback.print_tb(sys.exc_traceback)
                msg = wx.MessageDialog(self, str(e), "An error occurred",  wx.OK | wx.ICON_ERROR)
                msg.ShowModal()
                msg.Destroy()

            progress_dlg.Update(maximum)
            self.GetStatusBar().SetStatusText("Ready")

        dlg.Destroy()
        self.RefreshAll()

    def OnRun(self, event):
        data = self._dag_tree.GetPyData(self._dag_tree.GetRootItem())
        if not data:
            return
        root_ctx, root_node = data

        progress_dlg = None
        start_date = None
        end_date = None

        # callback called by 'run'
        def callback(date, ctx):    
            while True:
                # evaluate all the direct dependencies
                for node, ctx in root_node.get_dependencies(root_ctx):
                    ctx[node]
                    
                date = root_ctx.get_date()
                days_left = (date - start_date).days
                keep_going, skip = progress_dlg.Update(days_left)
                if not keep_going:
                    raise StopIteration
                yield

        dlg = RunToDialog(self,
                            "Run to...",
                            start_date=self._last_selected_start_date,
                            end_date=self._last_selected_end_date)

        dlg.CenterOnScreen()
        if wx.ID_OK == dlg.ShowModal():
            # create a date range inclusive of the end date
            start_date, end_date = dlg.GetDates()
            date_range = pd.bdate_range(start_date, end_date)

            self._last_selected_start_date = start_date
            self._last_selected_end_date = end_date

            # create a progress dialog
            maximum = max(1, (end_date - start_date).days)
            progress_dlg = wx.ProgressDialog("Please wait...",
                                             "Calculating nodes",
                                             maximum=maximum,
                                             style=wx.PD_CAN_ABORT
                                                    | wx.PD_ELAPSED_TIME
                                                    | wx.PD_REMAINING_TIME
                                                    | wx.PD_AUTO_HIDE)

            self.GetStatusBar().SetStatusText("Calculating...")

            # and run through that date range using the builder
            try:
                with self.UpdatingGuard(self):
                    run(date_range, [callback], ctx=root_ctx)

            except StopIteration:
                pass
            except Exception, e:
                traceback.print_tb(sys.exc_traceback)
                msg = wx.MessageDialog(self, str(e), "An error occurred",  wx.OK | wx.ICON_ERROR)
                msg.ShowModal()
                msg.Destroy()

            progress_dlg.Update(maximum)
            self.GetStatusBar().SetStatusText("Ready")

        dlg.Destroy()
        self.RefreshAll()

    def OnNext(self, event):
        data = self._dag_tree.GetPyData(self._dag_tree.GetRootItem())
        if not data:
            return
        ctx, node = data

        date = ctx.get_date()
        date += timedelta(days=1)
        if date.weekday() > 4:
            date += timedelta(days=7-date.weekday())

        self.GetStatusBar().SetStatusText("Setting date...")
        try:
            ctx.set_date(date)
            self.GetStatusBar().SetStatusText("Ready")
        except Exception:
            # any errors he should be re-raised by calculate
            pass

        self.Calculate()

    def OnRecalculate(self, event):
        self.Calculate()

    def OnTreeItemMenu(self, event):
        tree = event.GetEventObject()
        items = tree.GetSelections()
        if not items:
            items = [event.GetItem()]

        nodes_and_ctxs = []
        for item in items:
            data = tree.GetPyData(item)
            if not data:
                return
            ctx, node = data
            if (node, ctx) not in nodes_and_ctxs:
                nodes_and_ctxs.append((node, ctx))
        nodes, ctxs = zip(*nodes_and_ctxs)

        def _show_dag(event):
            self.ShowDag(nodes, ctxs[0])

        def _plot(event):
            self.Plot(nodes, ctxs)

        def _export_to_excel(event):
            self.ExportToExcel(nodes, ctxs)

        def _add_to_root(event):
            for node, ctx in nodes_and_ctxs:
                self.AddNodeToRoot(node, ctx)

        menu = wx.Menu()
        show_dag = menu.Append(wx.NewId(), "Show DAG")

        is_first_level = tree.GetItemParent(item) == tree.GetRootItem()
        if not is_first_level:
            add_to_root = menu.Append(wx.NewId(), "Add to root")
            menu.Bind(wx.EVT_MENU, _add_to_root, id=add_to_root.Id)

        export = menu.Append(wx.NewId(), "Export to Excel")
        plot = menu.Append(wx.NewId(), "Plot")
        menu.Bind(wx.EVT_MENU, _show_dag, id=show_dag.Id)
        menu.Bind(wx.EVT_MENU, _export_to_excel, id=export.Id)
        menu.Bind(wx.EVT_MENU, _plot, id=plot.Id)

        rect = tree.GetBoundingRect(item)
        position = event.GetPoint().x, rect.GetBottom()
        self.PopupMenu(menu, position)

    def OnTreeItemExpanding(self, event):
        if self.__collapsing:
            event.Veto()
            return

        tree = event.GetEventObject()
        item = event.GetItem()

        data = tree.GetPyData(item)
        if not data:
            return
        ctx, node = data

        dependencies = node.get_dependencies(ctx)
        dependencies = _apply_filters(dependencies, self._ctx_filter, self._cat_filter)
        dependencies = sorted(dependencies, key=lambda (node, ctx): (node.name, ctx.get_id()))
        for dep_node, dep_ctx in dependencies:
            image = self.NORMAL_FILE
            if dep_node.get_dependencies(dep_ctx):
                image = self.FOLDER
            last = tree.AppendItem(item, dep_node.short_name, image)
            tree.SetPyData(last, (dep_ctx, dep_node))
            self.RefreshItem(tree, last)

    def OnTreeItemCollapsing(self, event):
        if self.__collapsing:
            event.Veto()
            return
 
        self.__collapsing = True
        tree = event.GetEventObject()
        item = event.GetItem()
        tree.CollapseAndReset(item)
        tree.SetItemHasChildren(item, True)
        self.__collapsing = False

    def OnTreeSelectionChanged(self, event):
        if self.__collapsing:
            event.Veto()
            return
        
        tree = event.GetEventObject()
        item = event.GetItem()

        # reset any current search item
        if item != self._last_search_item:
            self._last_search_item = None

        data = tree.GetItemData(item)
        if not data or not data.GetData():
            return

        ctx, node = data.GetData()
        self.SetNode(node, ctx)

    def OnSearch(self, event):
        # get the starting point to search from
        tree = self._dag_tree
        starting_item = tree.GetSelection()
        if not starting_item.IsOk():
            starting_item = tree.GetRootItem()

        data = tree.GetPyData(starting_item)
        if data is None:
            return

        def search(tree_node, search_value):
            """does a depth first search for the search string and returns the first result"""
            tree_nodes = deque([tree_node])
            seen = set()
            while tree_nodes:
                current = tree_nodes.popleft()

                # check for cycles
                if (current.node, current.ctx) in seen:
                    continue
                seen.add((current.node, current.ctx))

                if search_value.lower() in current.node.short_name.lower() \
                and current.node is not self._root_node:
                    return current

                # add children to be processed depth first
                for child in reversed(list(current.children)):
                    tree_nodes.appendleft(child)

        def expand_node(tree_node):
            """expands tree to show the tree_node and returns the corresponding item"""
            expand_path = deque(tree_node.id)
            item = tree.GetRootItem()
            while expand_path:
                sibling_id = expand_path.popleft()
                for x in xrange(sibling_id):
                    item = tree.GetNextSibling(item)

                # if this is the last item select it and return
                if not expand_path:
                    return item

                # otherwise expand it and continue down
                tree.Expand(item)
                item, cookie = tree.GetFirstChild(item)

        def get_next_node(tree_node):
            """returns the next node to search from once tree_node has been searched"""
            # walk back up the tree and continue searching down the next sibling
            next = None
            current = tree_node
            while not next and current:
                next = current.next_sibling
                current = current.parent

            # if there are no more siblings to search start again at the root
            if next is None:
                next = tree_node
                while next.parent is not None:
                    next = next.parent

            return next

        starting_node = TreeNode.create_from_item(tree,
                                                  starting_item,
                                                  self._ctx_filter,
                                                  self._cat_filter)
        search_value = event.GetEventObject().GetValue()

        # if we're in the middle of searching start from the next child node
        current_node = starting_node
        if self._last_search_item == starting_item \
        and search_value.lower() in starting_node.node.short_name.lower():
            try:
                current_node = starting_node.children.next()
            except StopIteration:
                current_node = get_next_node(starting_node)

        found = None
        while not found and current_node:
            found = search(current_node, search_value)
            if not found:
                next_node = get_next_node(current_node)
                if next_node == current_node:
                    break
                current_node = next_node

        if not found:
            wx.MessageBox("No nodes matching '%s' found" % search_value, style=wx.ICON_INFORMATION)
            return

        found_item = expand_node(found)
        self._last_search_item = found_item
        tree.SelectItem(found_item)

    def OnSetFilters(self, event):
        # get the root context (used for the possible filtering options)
        data = self._dag_tree.GetPyData(self._dag_tree.GetRootItem())
        if not data:
            wx.MessageBox("No data loaded")
            return
        root_ctx, root_node = data

        toolbar = event.EventObject
        tool_id = event.GetId()
        toolbar.ToggleTool(tool_id, not toolbar.GetToolToggled(tool_id))

        # show the filters dialog
        dlg = FiltersDialog(self, root_ctx, self._ctx_filter, self._cat_filter)
        dlg.CenterOnScreen()
        if wx.ID_OK == dlg.ShowModal():
            # update the active filter
            self._ctx_filter = None
            if dlg.IsCtxFiltered():
                self._ctx_filter = dlg.GetCtxFilter()

            self._cat_filter = None
            if dlg.IsCategoryFiltered():
                self._cat_filter = dlg.GetCategoryFilter()

            toolbar.ToggleTool(tool_id, dlg.IsCtxFiltered() or dlg.IsCategoryFiltered())

            self._dag_tree.Collapse(self._dag_tree.GetRootItem())
            self._dag_tree.Expand(self._dag_tree.GetRootItem())

        toolbar.Refresh()
        self.RefreshAll()

    def PopulateTreeCtrl(self, ctx, tree=None, root_label="root", nodes=None):
        """populates a tree control with all the nodes with values in a context"""
        tree = tree or self._dag_tree

        # clear any previous data
        prev_data = tree.GetPyData(tree.GetRootItem())
        if prev_data:
            prev_ctx, prev_node = prev_data
            prev_node.clear(prev_ctx)

        root_ctx = ctx
        date = root_ctx.get_date()
        self._date_picker.SetValue(to_wxdate(date))
        self._last_selected_start_date = date

        # only enable the refresh button if date is a weekday
        self._toolbar.EnableTool(self.refreshId, date.weekday() <= 4)
        self._toolbar.Refresh()

        if nodes is None:
            head_nodes = {}
            for node, ctx in _get_head_nodes(ctx):
                head_nodes.setdefault(ctx, []).append(node)
        else:
            head_nodes = {ctx : list(nodes)}

        # clear the tree and add the new items
        tree.DeleteAllItems()

        root = tree.AddRoot(root_label, 0)
        for i in range(len(self._columns[:1])):
            tree.SetItemText(root, "", i+1)

        # make the root context dependent on the head nodes
        self._root_node.clear(root_ctx)
        for ctx, nodes in head_nodes.iteritems():
            for node in nodes:
                self._root_node.add_dependency(root_ctx, node, ctx)

        # evaluate the root node to mark it as not dirty
        root_ctx[self._root_node]

        tree.SetItemHasChildren(root, len(head_nodes) > 0)

        # and set the data for the root item
        tree.SetPyData(root, (root_ctx, self._root_node))

        # expand to show first level children
        tree.Expand(root)

    def AddNodeToRoot(self, node, ctx=None):
        """
        Add an mdf node to the root node of the tree.
        If ctx is None the current root context is used.
        """
        assert isinstance(node, MDFNode), \
            "node must be an mdf node"

        tree = self._dag_tree
        root_item = tree.GetRootItem()

        # create a new context if there isn't one already
        data = tree.GetPyData(root_item)
        if data is None:
            root_ctx = ctx or MDFContext()
            self.SetContext(root_ctx, nodes=[])
            data = self.tree.GetPyData(root_item)
        root_ctx, root_node = data

        if ctx is None:
            ctx = root_ctx

        assert ctx.is_shift_of(root_ctx), \
            "ctx must either be the root context or a shift of the root context"

        # if the node has already been added don't add it again
        if (node, ctx) in root_node.get_dependencies(root_ctx):
            return

        root_node.add_dependency(root_ctx, node, ctx)
        tree.SetItemHasChildren(root_item, True)

        if tree.IsExpanded(root_item):
            image = self.NORMAL_FILE
            if node.get_dependencies(root_ctx):
                image = self.FOLDER
            last = tree.AppendItem(root_item, node.short_name, image)
            self._dag_tree.SetPyData(last, (ctx, node))
            self.RefreshItem(self._dag_tree, last)
        else:
            tree.Expand(root_item)

    def OnOpen(self, event):
        dlg = wx.FileDialog(self, message="Choose a pickled context file",
                            defaultDir=os.getcwd(), 
                            defaultFile="",
                            wildcard=
                                ("DAG files (*.dag; *.zip; *.bz2; *.gz)|*.dag;*.zip;*.bz2;*.gz|"
                                 "All files (*.*)|*.*"),
                            style=wx.OPEN | wx.CHANGE_DIR)

        try:
            if dlg.ShowModal() == wx.ID_OK:
                # load the context
                path = dlg.GetPaths()[0]
                self.Open(path)
        finally:
            dlg.Destroy()

    def Save(self, filename):
        data = self._dag_tree.GetPyData(self._dag_tree.GetRootItem())
        if not data:
            return

        # remember the dependencies of the root node
        root_ctx, root_node = data
        dependencies = root_node.get_dependencies(root_ctx)

        # clear the root node so it doesn't get pickled
        root_node.clear(root_ctx)
        
        self.GetStatusBar().SetStatusText("Saving to %s..." % filename)
        try:
            # save the context
            root_ctx.save(filename)
            self._filename = filename
        finally:
            # restore the dependencies
            for node, ctx in dependencies:
                root_node.add_dependency(root_ctx, node, ctx)

            # re-set the node value after clearing it
            root_ctx[root_node] = None

            self.GetStatusBar().SetStatusText("Ready")

    def OnSave(self, event):
        if self._filename is None:
            return self.OnSaveAs(event)

        data = self._dag_tree.GetPyData(self._dag_tree.GetRootItem())
        if not data:
            msg = wx.MessageDialog(self, "Nothing to save", "Nothing to save",  wx.OK | wx.ICON_INFORMATION)
            msg.ShowModal()
            msg.Destroy()
            return

        self.Save(self._filename)

    def OnSaveAs(self, event):
        data = self._dag_tree.GetPyData(self._dag_tree.GetRootItem())
        if not data:
            msg = wx.MessageDialog(self, "Nothing to save", "Nothing to save",  wx.OK | wx.ICON_INFORMATION)
            msg.ShowModal()
            msg.Destroy()
            return

        dlg = wx.FileDialog(self, message="Save As...",
                            defaultDir=os.getcwd(), 
                            defaultFile="",
                            wildcard=
                                ("DAG files (*.dag; *.zip; *.bz2; *.gz)|*.dag;*.zip;*.bz2;*.gz|"
                                 "All files (*.*)|*.*"),
                            style=wx.SAVE | wx.CHANGE_DIR)
        try:
            if dlg.ShowModal() == wx.ID_OK:
                self.Save(dlg.GetPaths()[0])
        finally:
            dlg.Destroy()

    def OnExit(self, event):
        self.Close(True)

    #
    # API Methods
    #
    def Open(self, filename):
        """loads a pickled context file"""
        self._filename = None
        try:
            # load the context
            self.GetStatusBar().SetStatusText("Loading %s..." % filename)
            ctx = MDFContext.load(filename)
    
            # rebuild the tree with the nodes in the context
            self.PopulateTreeCtrl(ctx, self._dag_tree, os.path.basename(filename))
            self.GetStatusBar().SetStatusText("Ready")
            self._filename = filename
        except Exception, e:
            traceback.print_tb(sys.exc_traceback)
            msg = wx.MessageDialog(self, str(e), "An error occurred",  wx.OK | wx.ICON_ERROR)
            msg.ShowModal()
            msg.Destroy()
            raise

    def SetContext(self, ctx, nodes=None):
        """
        resets the viewer for a specific context and 
        optionally a set of nodes.
        """
        self.PopulateTreeCtrl(ctx, nodes=nodes)

def _get_head_nodes(root_ctx):
    """
    returns a list of (context, node) tuples from which all other
    nodes can be reached.
    """
    class Vertex:
        """vertex in the digraph of (node, ctx) -> (node, ctx)"""
        def __init__(self, node, ctx):
            """don't call directly, use get"""
            self.node = node
            self.ctx = ctx
            self.__edges_in = set()
            self.__edges_out = set()

        @classmethod
        def add_edge(self, lhs, rhs):
            """add directed edge lhs -> rhs"""
            lhs.__edges_out.add((rhs.node, rhs.ctx))
            rhs.__edges_in.add((lhs.node, lhs.ctx))

        @property
        def edges_out(self):
            return [self.get(n, c) for n, c in self.__edges_out]

        @property
        def edges_in(self):
            return [self.get(n, c) for n, c in self.__edges_in]

        @property
        def in_degree(self):
            return len(self.__edges_in)

        @property
        def out_degree(self):
            return len(self.__edges_out)

        _node_ctx_to_verts = {} 
        @classmethod
        def get(cls, node, ctx):
            v = cls._node_ctx_to_verts.get((node, ctx), None)
            if v is None:
                v = cls(node, ctx)
                cls._node_ctx_to_verts[(node, ctx)] = v
            return v  

        @classmethod
        def all_vertices(cls):
            return cls._node_ctx_to_verts.values()

    #
    # build the set of vertices
    #
    root_ctx = root_ctx.get_parent() or root_ctx
    contexts = [root_ctx] + root_ctx.get_shifted_contexts()
    for ctx in contexts:
        nodes = ctx.all_nodes()
        for node in nodes:
            left = Vertex.get(node, ctx)
            for dep_node, dep_ctx in node.get_dependencies(ctx):
                right = Vertex.get(dep_node, dep_ctx)
                Vertex.add_edge(left, right)

    # sort by in_degree so we can find the vertices with the minimum
    vertices = Vertex.all_vertices()
    vertices.sort(key=lambda x: x.in_degree)

    head_vertices = set()
    while vertices:
        # select the vertex with the minimum in degree
        v = vertices.pop(0)
        head_vertices.add(v)

        # remove its dependencies from the set left to 
        # be considered and the set of previously found
        # best nodes
        edges_out = list(v.edges_out)
        seen = set([v])
        while edges_out:
            e = edges_out.pop()
            if e in seen:
                continue

            edges_out.extend(e.edges_out)
            seen.add(e)

            for s in (vertices, head_vertices):
                try:
                    s.remove(e)
                except (KeyError, ValueError):
                    pass

    return [(x.node, x.ctx) for x in head_vertices]
