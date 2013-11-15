"""
Panel for showing mdf DAG
"""
import wx
import wx.stc
import wx.grid
import wx.lib.scrolledpanel
import wx.lib.agw.customtreectrl
from datetime import datetime, date
from collections import deque
import numpy as np
from ..mixins import GridCopyMixin
from ...nodes import MDFNode

_default_float_format = "%.9f"

try:
    import pandas as pa
    _pandas_available = True
except ImportError:
    _pandas_available = False

def _get_panel_cls(value):
    """return the class best suited for displaying 'value'"""
    if _pandas_available:
        if isinstance(value, (pa.DataFrame, pa.Series)):
            return DataFramePanel
    if isinstance(value, dict):
        return DictPanel
    elif isinstance(value, (list, tuple, deque)):
        return ListPanel
    elif isinstance(value, np.ndarray):
        return NpArrayPanel
    return _get_text_value_panel(value)

def _get_type_name(value):
    """return a string describing the type of value"""
    try:
        return value.__class__.__name__
    except AttributeError:
        pass
    return repr(type(value))

def _seq_to_dict(seq):
    """returns a dict from a sequence"""
    # namedtuple
    if isinstance(seq, tuple) and hasattr(seq, "_asdict"):
        return seq._asdict()
    # everything else
    return dict(zip(range(len(seq)), seq))

class NodeValueViewer(wx.Panel):
    """
    Panel that displays the current value of a node.
    """
    def __init__(self, parent, id=wx.ID_ANY):
        wx.Panel.__init__(self, parent, id)
        self.current_panel_cls = None
        self.value_panel = None
        self.curr_type = ""
        self.sizer = wx.BoxSizer(wx.VERTICAL)
        self.SetSizer(self.sizer)

    def GetCurrentValueType(self):
        """returns a string representing the type of the current value"""
        return self.curr_type

    def SetNode(self, node, ctx):
        # get the node value
        value = "<stale value>"
        if node.has_value(ctx) and not node.is_dirty(ctx):
            value = ctx[node]

        # get the type of the current value
        self.curr_type = _get_type_name(value)

        self.Freeze()
        try:
            # create/re-use the panel depending on the value's type
            cls = _get_panel_cls(value)
            if cls is not self.current_panel_cls or self.value_panel is None:
                self.sizer.DeleteWindows()
                self.current_panel_cls = cls
                self.value_panel = cls(self)
                self.sizer.Add(self.value_panel, 1, wx.EXPAND)

            self.value_panel.SetValue(value)
            self.sizer.Layout()
        finally:
            self.Thaw()

class StyledTextValue(wx.stc.StyledTextCtrl):
    """for text and for all types not handled by a more specialised panel"""

    def __init__(self, parent, id=wx.ID_ANY):
        wx.stc.StyledTextCtrl.__init__(self, parent, id, wx.DefaultPosition, wx.DefaultSize, 0)

        self.StyleSetSpec(wx.stc.STC_STYLE_DEFAULT, "face:Courier New,size:10")
        self.StyleClearAll()
        self.SetWrapMode(wx.stc.STC_WRAP_WORD)

    def SetValue(self, value):
        self.SetReadOnly(False)
        self.SetText(str(value))
        self.SetReadOnly(True)

class SimpleTextValue(wx.StaticText):
    """for short strings that don't need wrapping etc"""
    
    def __init__(self, parent, id=wx.ID_ANY):
        wx.StaticText.__init__(self, parent, id, "")
        self.SetFont(wx.Font(10, wx.SWISS, wx.NORMAL, wx.NORMAL))

    def SetValue(self, value):
        if isinstance(value, float):
            value = _default_float_format % value
        self.SetLabel(str(value))

def _get_text_value_panel(value):
    value = str(value)
    if "\n" in value or len(value) > 100:
        return StyledTextValue
    return SimpleTextValue

class DataFramePanel(wx.grid.Grid, GridCopyMixin):
    """for pandas series and dataframes"""
    
    class DataFrameTable(wx.grid.PyGridTableBase):
        
        _dtypes = {
            int         : wx.grid.GRID_VALUE_NUMBER,
            float       : wx.grid.GRID_VALUE_FLOAT,
            basestring  : wx.grid.GRID_VALUE_STRING,
            bool        : wx.grid.GRID_VALUE_BOOL,
            date        : wx.grid.GRID_VALUE_DATETIME,
            datetime    : wx.grid.GRID_VALUE_DATETIME,
        }

        def __init__(self, df):
            wx.grid.PyGridTableBase.__init__(self)
            self.df = df.convert_objects()

            self.datatypes = []
            for dtype in df.dtypes:
                self.datatypes.append(self._dtypes.get(dtype, wx.grid.GRID_VALUE_STRING))

        def GetNumberRows(self):
            return self.df.index.size

        def GetNumberCols(self):
            return self.df.columns.size
            
        def IsEmptyCell(self, row, col):
            return False

        def GetValue(self, row, col):
            idx = self.df.index[row]
            col = self.df.columns[col]
            return self.df.xs(idx)[col]

        def GetColLabelValue(self, col):
            return self.df.columns[col]
    
        def GetRowLabelValue(self, row):
            label = self.df.index[row]
            if isinstance(label, datetime):
                return label.strftime("%Y-%m-%d %H:%M:%S")
            elif isinstance(label, date):
                return label.strftime("%Y-%m-%d")
            return str(label)
    
        def GetTypeName(self, row, col):
            return self.datatypes[col]

    def __init__(self, parent, id=wx.ID_ANY):
        wx.grid.Grid.__init__(self, parent, id)
        GridCopyMixin.__init__(self)
        self.EnableEditing(False)
        self.SetTable(self.DataFrameTable(pa.DataFrame()), True)

        self.__show_zeros_id = wx.NewId()
        self.__hide_zeros_id = wx.NewId()
        self.__show_nans_id = wx.NewId()
        self.__hide_nans_id = wx.NewId()
        
        self.__hide_zeros = False
        self.__hide_nans = False
        self.__df = self.GetTable().df

        self.Bind(wx.EVT_MENU, self.ShowZeros, id=self.__show_zeros_id)
        self.Bind(wx.EVT_MENU, self.HideZeros, id=self.__hide_zeros_id)
        self.Bind(wx.EVT_MENU, self.ShowNaNs, id=self.__show_nans_id)
        self.Bind(wx.EVT_MENU, self.HideNaNs, id=self.__hide_nans_id)

    def __FilterDataFrame(self):
        df = self.__df

        mask = pa.DataFrame({}, columns=df.columns, index=df.index, dtype=bool)
        mask.values.fill(False)

        if self.__hide_zeros:
            mask |= (df == 0.0)
        if self.__hide_nans:
            mask |= pa.isnull(df)

        mask = mask.apply(lambda x: x.all(), axis=1)
        self.SetTable(self.DataFrameTable(df[~mask]), True)
        self.Refresh()

    def HideZeros(self, event):
        self.__hide_zeros = True
        self.__FilterDataFrame()

    def ShowZeros(self, event):
        self.__hide_zeros = False
        self.__FilterDataFrame()

    def HideNaNs(self, event):
        self.__hide_nans = True
        self.__FilterDataFrame()

    def ShowNaNs(self, event):
        self.__hide_nans = False
        self.__FilterDataFrame()

    def CreateContextMenu(self, menu):
        if self.__hide_zeros:
            menu.Append(self.__show_zeros_id, "Show zeros")
        else:
            menu.Append(self.__hide_zeros_id, "Hide zeros")

        if self.__hide_nans:
            menu.Append(self.__show_nans_id, "Show NaNs")
        else:
            menu.Append(self.__hide_nans_id, "Hide NaNs")

        super(DataFramePanel, self).CreateContextMenu(menu)

    def SetValue(self, value):
        # convert series to a dataframe with a single column
        if isinstance(value, pa.Series):
            value = pa.DataFrame({value.name or "": value})

        self.__hide_zeros = False
        self.__hide_nans = False

        # set the table to the first 10 rows and calculate the row and col sizes
        self.SetTable(self.DataFrameTable(value.head(10)), True)
        self.SetRowLabelSize(wx.grid.GRID_AUTOSIZE)
        self.AutoSizeColumns(False)
        self.AutoSizeRows(False)

        # set the table to the full table now the resizing has been done
        if value.index.size > 10:
            self.SetTable(self.DataFrameTable(value), True)

        self.__df = self.GetTable().df

class DictPanel(wx.lib.scrolledpanel.ScrolledPanel):
    """panel for displaying dictionaries of objects"""

    class DictTable(wx.grid.PyGridTableBase):

        _dtypes = {
            int         : wx.grid.GRID_VALUE_NUMBER,
            float       : wx.grid.GRID_VALUE_FLOAT,
            str         : wx.grid.GRID_VALUE_STRING,
            unicode     : wx.grid.GRID_VALUE_STRING,
            bool        : wx.grid.GRID_VALUE_BOOL,
            date        : wx.grid.GRID_VALUE_DATETIME,
            datetime    : wx.grid.GRID_VALUE_DATETIME,
        }

        def __init__(self, d):
            wx.grid.PyGridTableBase.__init__(self)
            self.keys = sorted(d.keys())
            self.values = [d[k] for k in self.keys]

        def GetNumberRows(self):
            return len(self.values)

        def GetNumberCols(self):
            return 1

        def IsEmptyCell(self, row, col):
            return False

        def GetValue(self, row, col):
            return self.values[row]
    
        def GetRowLabelValue(self, row):
            return str(self.keys[row])

        def GetColLabelValue(self, col):
            return ""

        def GetTypeName(self, row, col):
            value = self.values[row]
            return self._dtypes.get(type(value), wx.grid.GRID_VALUE_STRING)

    class Grid(wx.grid.Grid, GridCopyMixin):
        def __init__(self, parent):
            wx.grid.Grid.__init__(self, parent)
            GridCopyMixin.__init__(self)

    def __init__(self, parent, id=wx.ID_ANY):
        wx.lib.scrolledpanel.ScrolledPanel.__init__(self, parent, id)
        self.simple_grid = None
        self.tree_ctrl = None
        self.sizer = wx.BoxSizer(wx.VERTICAL)
        self.SetSizer(self.sizer)
        self.SetupScrolling()

    def SetValue(self, value):
        has_complex_type = False
        for v in value.itervalues():
            if not isinstance(v, (basestring, int, float, bool, datetime, date)):
                has_complex_type = True

        if not has_complex_type:
            # put the values in a simple grid
            if self.simple_grid is None:
                # clear the sizer and add a new grid to it
                self.sizer.Clear(True)
                self.tree_ctrl = None
                self.simple_grid = self.Grid(self)
                self.simple_grid.EnableEditing(False)
                self.sizer.Add(self.simple_grid, 1, flag=wx.EXPAND)

            self.simple_grid.SetTable(self.DictTable(value), True)
            self.simple_grid.SetRowLabelSize(wx.grid.GRID_AUTOSIZE)
            self.simple_grid.SetColLabelSize(1)
            self.simple_grid.AutoSizeColumns(False)
            self.simple_grid.AutoSizeRows(False)
        else:
            # otherwise use the grid sizer to display all the values
            # using custom panels
            if self.tree_ctrl is None:
                self.sizer.Clear(True)
                self.simple_grid = None
                self.tree_ctrl = DictTreeCtrl(self)
                self.sizer.Add(self.tree_ctrl, 1, flag=wx.EXPAND)

            self.tree_ctrl.SetValue(value)

        self.sizer.Layout()
        self.AdjustScrollbars()

class DictTreeCtrl(wx.lib.agw.customtreectrl.CustomTreeCtrl):
    """shows a dict of complex objects as a tree"""
    
    def __init__(self, parent, id=wx.ID_ANY):
        wx.lib.agw.customtreectrl.CustomTreeCtrl.__init__(self,
                    parent, id,
                    agwStyle=(wx.lib.agw.customtreectrl.TR_HAS_VARIABLE_ROW_HEIGHT | 
                              wx.lib.agw.customtreectrl.TR_HIDE_ROOT))

        image_list = wx.ImageList(16, 16)
        for id in (wx.ART_FOLDER, wx.ART_FOLDER_OPEN, wx.ART_NORMAL_FILE, wx.ART_EXECUTABLE_FILE):
            bmp = wx.ArtProvider_GetBitmap(id, wx.ART_TOOLBAR, (16, 16))
            image_list.Add(bmp)
        self.AssignImageList(image_list)

    def SetValue(self, value):
        # convert to a dictionary if necessary
        if not isinstance(value, dict):
            value = dict(zip(range(len(value)), value))

        # clear the tree and build it again
        self.DeleteAllItems()
        root = self.AddRoot("")
        self._add_dict(root, value)
        self.ExpandAllChildren(root)

    def _add_dict(self, root, d):
        keys = sorted(d.iterkeys())
        values = (d[k] for k in keys)

        self.SetItemHasChildren(root, True)
        self.SetItemImage(root, 0, wx.lib.agw.customtreectrl.TreeItemIcon_Normal)
        self.SetItemImage(root, 1, wx.lib.agw.customtreectrl.TreeItemIcon_Expanded)

        for k, v in zip(keys, values):
            if isinstance(k, MDFNode):
                node_label = "%s (node):" % k.short_name 
            else:
                node_label = "%s (%s):" % (k, _get_type_name(v))

            if isinstance(v, MDFNode):
                self.AppendItem(root, "%s %s (node)" % (node_label, v.short_name))
                continue

            if isinstance(v, dict):
                item = self.AppendItem(root, node_label)
                self._add_dict(item, v)
                continue

            if isinstance(v, (tuple, list, deque)):
                item = self.AppendItem(root, node_label)
                d = _seq_to_dict(v)
                self._add_dict(item, d)
                continue

            if isinstance(v, (int, basestring)):
                self.AppendItem(root, "%s %s" % (node_label, v))
                continue

            if isinstance(v, (int, float)):
                self.AppendItem(root, "%s %s" % (node_label, _default_float_format % v))
                continue

            cls = _get_panel_cls(v)
            if issubclass(cls, wx.StaticText):
                self.AppendItem(root, "%s %s" % (node_label, v))
                continue

            # create a control to display this value and add it to the tree
            ctrl = cls(self)
            ctrl.SetValue(v)
            
            if isinstance(ctrl, wx.grid.Grid):
                # expand the grid to its full size
                ctrl.ForceRefresh()
                ctrl.SetSize(ctrl.GetEffectiveMinSize())

            item = self.AppendItem(root, node_label)
            self.SetItemHasChildren(root, True)
            self.SetItemImage(item, 0, wx.lib.agw.customtreectrl.TreeItemIcon_Normal)
            self.SetItemImage(item, 1, wx.lib.agw.customtreectrl.TreeItemIcon_Expanded)
            self.AppendItem(item, "",  wnd=ctrl)

class ListPanel(DictPanel):
    """panel for displaying lists and tuples of objects"""
    def SetValue(self, value):
        DictPanel.SetValue(self, _seq_to_dict(value))

class NpArrayPanel(wx.grid.Grid, GridCopyMixin):
    """panel for displaying numpy arrays"""

    class NpArrayTable(wx.grid.PyGridTableBase):
        
        _dtypes = {
            int         : wx.grid.GRID_VALUE_NUMBER,
            float       : wx.grid.GRID_VALUE_FLOAT,
            basestring  : wx.grid.GRID_VALUE_STRING,
            bool        : wx.grid.GRID_VALUE_BOOL,
            date        : wx.grid.GRID_VALUE_DATETIME,
            datetime    : wx.grid.GRID_VALUE_DATETIME,
        }

        def __init__(self, array):
            wx.grid.PyGridTableBase.__init__(self)
            self.array = array
            self.dtype = self._dtypes.get(array.dtype, wx.grid.GRID_VALUE_STRING)

        def GetNumberRows(self):
            return self.array.shape[0]

        def GetNumberCols(self):
            if len(self.array.shape) > 1:
                return self.array.shape[1]
            return 1

        def IsEmptyCell(self, row, col):
            return False

        def GetValue(self, row, col):
            if len(self.array.shape) > 1:
                return self.array[row, col]
            return self.array[row]

        def GetColLabelValue(self, col):
            return str(col)
    
        def GetRowLabelValue(self, row):
            return str(row)
    
        def GetTypeName(self, row, col):
            return self.dtype

    def __init__(self, parent, id=wx.ID_ANY):
        wx.grid.Grid.__init__(self, parent, id)
        GridCopyMixin.__init__(self)
        self.EnableEditing(False)
        self.SetTable(self.NpArrayTable(np.array([])), True)

    def SetValue(self, value):
        self.SetTable(self.NpArrayTable(value), True)
        self.SetRowLabelSize(wx.grid.GRID_AUTOSIZE)
        self.AutoSizeColumns(False)
        self.AutoSizeRows(False)
