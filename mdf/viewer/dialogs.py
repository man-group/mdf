"""
Dialogs used by the mdf viewer
"""
import wx
import wx.lib.rcsizer as rcs
import wx.lib.agw.customtreectrl as CT
from common import to_wxdate, to_datetime
import string
from ..nodes import MDFNode
import mixins

class RunToDialog(wx.Dialog):
    """
    Dialog for selecting dates to iterate through
    """
    
    def __init__(self, parent, title, start_date=None, end_date=None, id=wx.ID_ANY):
        pre = wx.PreDialog()
        pre.SetExtraStyle(wx.DIALOG_EX_CONTEXTHELP)
        pre.Create(parent, id, title, wx.DefaultPosition, wx.DefaultSize, wx.DEFAULT_DIALOG_STYLE)
        self.PostCreate(pre)
        
        sizer = wx.BoxSizer(wx.VERTICAL)
        label = wx.StaticText(self, -1, "Please select date range")
        sizer.Add(label, 0, wx.ALIGN_CENTRE|wx.ALL, 5)
        
        box = wx.BoxSizer(wx.HORIZONTAL)
        self.start = wx.DatePickerCtrl(self, style=wx.DP_DROPDOWN|wx.DP_SHOWCENTURY|wx.DP_ALLOWNONE)
        self.end = wx.DatePickerCtrl(self, style=wx.DP_DROPDOWN|wx.DP_SHOWCENTURY|wx.DP_ALLOWNONE)
        box.Add(self.start, 0, wx.ALIGN_CENTRE|wx.ALL, 5)
        box.Add(wx.StaticText(self, -1, "to"), 0, wx.ALIGN_CENTRE|wx.ALL, 5) 
        box.Add(self.end, 0, wx.ALIGN_CENTRE|wx.ALL, 5)
        sizer.Add(box, 0, wx.ALIGN_CENTRE|wx.EXPAND|wx.ALL, 5)

        self.start.SetValue(to_wxdate(start_date))
        self.end.SetValue(to_wxdate(end_date))

        btnsizer = wx.StdDialogButtonSizer()
        
        btn = wx.Button(self, wx.ID_OK, "Run")
        btn.SetDefault()
        btnsizer.AddButton(btn)

        btn = wx.Button(self, wx.ID_CANCEL)
        btnsizer.AddButton(btn)
        btnsizer.Realize()

        sizer.Add(btnsizer, 0, wx.ALIGN_CENTRE|wx.ALIGN_CENTER_VERTICAL|wx.ALL, 5)
        
        self.SetSizer(sizer)
        sizer.Fit(self)

    def GetDates(self):
        """returns tuple (start, end) for the selected dates"""
        start = self.start.GetValue()
        start = to_datetime(start)
        end = self.end.GetValue()
        end = to_datetime(end)
        return start, end

class StringValidator(wx.PyValidator):
    """Validator for use with the text box to only alloy certain characters"""
    def __init__(self, allowed_chars):
        wx.PyValidator.__init__(self)
        self.allowed_chars = allowed_chars
        self.Bind(wx.EVT_CHAR, self.OnChar)

    def Clone(self):
        return StringValidator(self.allowed_chars)

    def Validate(self, win):
        tc = self.GetWindow()
        val = tc.GetValue()
        for x in val:
            if x not in self.allowed_chars:
                return False
        return True

    def OnChar(self, event):
        key = event.GetKeyCode()

        if key < wx.WXK_SPACE or key == wx.WXK_DELETE or key > 255:
            event.Skip()
            return

        if chr(key) in self.allowed_chars:
            event.Skip()
            return

        if not wx.Validator_IsSilent():
            wx.Bell()

        # Returning without calling even.Skip eats the event before it
        # gets to the text control
        return

    def TransferToWindow(self):
        return True # Prevent wxDialog from complaining.

    def TransferFromWindow(self):
        return True # Prevent wxDialog from complaining.

class DAGViewerOptions(wx.Dialog):
    """
    Modal dialog for the user to choose some settings before
    rendering the DAG.
    """
    _layout_choices = [
        ("Left to right", "LR"),
        ("Top to bottom", "TB"),
        ("Right to left", "RL"),
        ("Bottom to top", "BT")
    ]

    _last_selected_max_depth = 5
    _last_selected_layout = "Left to right"

    def __init__(self, parent, id=wx.ID_ANY,
                    style=wx.DEFAULT_DIALOG_STYLE,
                    size=wx.DefaultSize, pos=wx.DefaultPosition):
        pre = wx.PreDialog()
        pre.SetExtraStyle(wx.DIALOG_EX_CONTEXTHELP)
        pre.Create(parent, id, "DAG Options", pos, size, style)
        self.PostCreate(pre)

        sizer = wx.BoxSizer(wx.VERTICAL)
        inputs_sizer = rcs.RowColSizer()
        inputs_sizer.AddGrowableCol(2)

        # max depth input
        label = wx.StaticText(self, -1, "Max depth")
        label.SetHelpText("Maximum depth of nodes to show in the graph")
        self._max_depth = wx.TextCtrl(self, -1,
                                        str(self._last_selected_max_depth),
                                        validator=StringValidator(string.digits))
        self._max_depth.SetHelpText("Maximum depth of nodes to show in the graph")
        inputs_sizer.Add(label, flag=wx.EXPAND|wx.ALIGN_CENTER_VERTICAL|wx.ALL, row=0, col=1, border=5)
        inputs_sizer.Add(self._max_depth, flag=wx.EXPAND|wx.ALIGN_CENTER_VERTICAL|wx.ALL, row=0, col=2, border=5)

        # layout choice
        label = wx.StaticText(self, -1, "Layout")
        label.SetHelpText("How to layout the nodes when drawing")
        self._layout_choice = wx.Choice(self, -1, choices=[x[0] for x in self._layout_choices])
        self._layout_choice.SetStringSelection(self._last_selected_layout)
        inputs_sizer.Add(label, flag=wx.EXPAND|wx.ALIGN_CENTER_VERTICAL|wx.ALL, row=1, col=1, border=5)
        inputs_sizer.Add(self._layout_choice, flag=wx.EXPAND|wx.ALIGN_CENTER_VERTICAL|wx.ALL, row=1, col=2, border=5)

        sizer.Add(inputs_sizer, 0, wx.EXPAND|wx.ALIGN_CENTER_VERTICAL|wx.ALL, 5)

        # ok/cancel buttons
        btnsizer = wx.StdDialogButtonSizer()
        okbtn = wx.Button(self, wx.ID_OK)
        okbtn.SetDefault()
        btnsizer.AddButton(okbtn)
        cancelbtn = wx.Button(self, wx.ID_CANCEL)
        btnsizer.AddButton(cancelbtn)
        btnsizer.Realize()
        sizer.Add(btnsizer, 0, wx.EXPAND|wx.ALIGN_CENTER_VERTICAL|wx.ALL, 5)

        self.Bind(wx.EVT_BUTTON, self.OnOK, okbtn)

        self.SetSizer(sizer)
        sizer.Fit(self)
        self.CenterOnParent()

    def OnOK(self, event):
        self.__class__._last_selected_max_depth = self._max_depth.GetValue()
        self.__class__._last_selected_layout = self._layout_choice.GetStringSelection()
        event.Skip()

    @property
    def max_depth(self):
        return int(self._max_depth.GetValue())

    @property
    def layout_style(self):
        styles = dict(self._layout_choices)
        return styles[self._layout_choice.GetStringSelection()]

class FiltersDialog(wx.Dialog):
    """
    Dialog for selecting how to filter the DAG view.
    """

    def __init__(self, parent, root_ctx, shift_filter, category_filter,
                    id=wx.ID_ANY,
                    style=wx.DEFAULT_DIALOG_STYLE|wx.RESIZE_BORDER,
                    size=wx.DefaultSize, pos=wx.DefaultPosition):
        pre = wx.PreDialog()
        pre.SetExtraStyle(wx.DIALOG_EX_CONTEXTHELP)
        pre.Create(parent, id, "Filters", pos, size, style)
        self.PostCreate(pre)

        sizer = wx.BoxSizer(wx.VERTICAL)

        shift_filter_active = True
        if shift_filter is None:
            shift_filter = {}
            shift_filter_active = False

        category_filter_active = True
        if category_filter is None:
            category_filter = set()
            category_filter_active = False

        # get all categories
        categories = set()
        def collect_categories(node, context):
            for category in node.categories:
                if category is not None:
                    categories.add(category)
            return True
        root_ctx.visit_nodes(collect_categories)

        # list control for category filters
        choices = ["<none>"] + sorted(categories)
        self.categories_listbox = wx.CheckListBox(self, style=wx.NO_BORDER, choices=choices)
        box = wx.StaticBox(self, -1, "Categories")
        boxsizer = wx.StaticBoxSizer(box, wx.VERTICAL)
        boxsizer.Add(self.categories_listbox, 1, wx.ALL, 5)
        sizer.Add(boxsizer, 0, wx.EXPAND|wx.ALL, 5)

        for i in xrange(self.categories_listbox.GetCount()):
            category = self.categories_listbox.GetString(i)
            if category == "<none>":
                category = None
            if not category_filter_active or category in category_filter:
                self.categories_listbox.Check(i)        

        # tree control of all possible shifted contexts
        all_shifts = {}
        for shifted_ctx in root_ctx.iter_shifted_contexts():
            for key, value in shifted_ctx.get_shift_set().iteritems():
                all_shifts.setdefault(key, set()).add(value)

        tree_ctrl = CT.CustomTreeCtrl(self)
        box = wx.StaticBox(self, -1, "Contexts")
        boxsizer = wx.StaticBoxSizer(box, wx.VERTICAL)
        boxsizer.Add(tree_ctrl, 1, wx.EXPAND|wx.ALL, 5)
        sizer.Add(boxsizer, 1, wx.EXPAND|wx.ALL, 5)

        root = tree_ctrl.AddRoot("Root context", ct_type=1)
        root.Check(not shift_filter_active or None in shift_filter)
        root.SetData((None, None, False))

        for key in sorted(all_shifts.iterkeys()):
            node_item = tree_ctrl.AppendItem(root, key.short_name, ct_type=1)
            node_item.SetData((key, None, True))
            node_item.Set3State(True)

            # add the individual shift items
            num_checked = 0
            for shift in sorted(all_shifts[key]):
                if isinstance(shift, MDFNode):
                    shift_str = shift.short_name
                else:
                    shift_str = str(shift)[:50]
                shift_item = tree_ctrl.AppendItem(node_item, shift_str, ct_type=1)
                shift_item.SetData((key, shift, False))

                if ((not shift_filter_active) or 
                (key in shift_filter and shift in shift_filter[key])):
                    shift_item.Check(True)
                    num_checked += 1

            # set the state of the group item
            if num_checked == len(all_shifts[key]):
                node_item.Set3StateValue(wx.CHK_CHECKED)
            elif num_checked > 0:
                node_item.Set3StateValue(wx.CHK_UNDETERMINED)
            else:
                node_item.Set3StateValue(wx.CHK_UNCHECKED)

        tree_ctrl.Expand(root)

        self.ctx_tree = tree_ctrl
        tree_ctrl.Bind(CT.EVT_TREE_ITEM_CHECKED, self.OnCtxTreeItemChecked)

        # ok/cancel buttons
        btnsizer = wx.StdDialogButtonSizer()
        okbtn = wx.Button(self, wx.ID_OK)
        okbtn.SetDefault()
        btnsizer.AddButton(okbtn)
        clearbtn = wx.Button(self, label="Clear All")
        btnsizer.SetNegativeButton(clearbtn)
        cancelbtn = wx.Button(self, wx.ID_CANCEL)
        btnsizer.AddButton(cancelbtn)
        btnsizer.Realize()
        sizer.Add(btnsizer, 0, wx.EXPAND|wx.ALIGN_CENTER_VERTICAL|wx.ALL, 5)

        self.Bind(wx.EVT_BUTTON, self.OnClearAll, clearbtn)

        self.SetSizer(sizer)
        sizer.Fit(self)
        self.CenterOnParent()

    def IsCtxFiltered(self):
        """return True if any context filter should be applied"""
        items = [self.ctx_tree.GetRootItem()]
        seen = set()
        while items:
            item = items.pop()
            if item in seen:
                continue
            seen.add(item)
            
            # if any item is unchecked some filtering has to be applied
            if not item.IsChecked():
                return True
            
            sibling = self.ctx_tree.GetNextSibling(item)
            while sibling is not None and sibling.IsOk():
                items.append(sibling)
                sibling = self.ctx_tree.GetNextSibling(sibling)

            child, cookie = self.ctx_tree.GetFirstChild(item)
            while child is not None and child.IsOk():
                items.append(child)
                child, cookie = self.ctx_tree.GetNextChild(item, cookie)

        # none of the items are unchecked so no filtering is needed
        return False

    def GetCtxFilter(self):
        """
        return a dictionary of node -> set([shift values...])
        None -> None indicates the root context should be included.
        """
        filter = {}
        items = [self.ctx_tree.GetRootItem()]
        seen = set()
        while items:
            item = items.pop()
            if item in seen:
                continue
            seen.add(item)
            
            # add the item to the filter dictionary if checked
            if item.IsChecked():
                node, value, cascade = item.GetData()
                if not cascade:
                    filter.setdefault(node, set()).add(value)

            child, cookie = self.ctx_tree.GetFirstChild(item)
            while child is not None and child.IsOk():
                items.append(child)
                child, cookie = self.ctx_tree.GetNextChild(item, cookie)

        return filter

    def IsCategoryFiltered(self):
        """return true if category filters should be applied"""
        for i in xrange(self.categories_listbox.GetCount()):
            if not self.categories_listbox.IsChecked(i):
                return True
        return False
        
    def GetCategoryFilter(self):
        """
        returns a set of category names that are to be included.
        """
        included = set()
        for i in xrange(self.categories_listbox.GetCount()):
            if self.categories_listbox.IsChecked(i):
                category = self.categories_listbox.GetString(i)
                if category == "<none>":
                    category = None
                included.add(category)
        return included

    def OnCtxTreeItemChecked(self, event):
        item = event.GetItem()
        
        # if the user has set the check box to undetermined set it to unchecked
        if item.Is3State():
            if item.Get3StateValue() == wx.CHK_UNDETERMINED:
                item.Set3StateValue(wx.CHK_UNCHECKED)
                item.Check(False)

        checked = item.IsChecked()
        node, value, cascade = item.GetData()

        # un/check the child checkboxes
        if cascade:
            child, cookie = self.ctx_tree.GetFirstChild(item)
            while child is not None and child.IsOk():
                child.Check(checked)
                child, cookie = self.ctx_tree.GetNextChild(item, cookie)

        # update the state of the parent checkbox
        parent = self.ctx_tree.GetItemParent(item)
        if parent is not None and parent.Is3State():
            num_children = 0
            num_checked = 0
            child, cookie = self.ctx_tree.GetFirstChild(parent)
            while child is not None and child.IsOk():
                num_children += 1
                num_checked += 1 if child.IsChecked() else 0
                child, cookie = self.ctx_tree.GetNextChild(parent, cookie) 

            if num_checked == num_children:
                parent.Set3StateValue(wx.CHK_CHECKED)
            elif num_checked > 0:
                parent.Set3StateValue(wx.CHK_UNDETERMINED)
            else:
                parent.Set3StateValue(wx.CHK_UNCHECKED)

        self.ctx_tree.Refresh()
        event.Skip()

    def OnClearAll(self, event):        
        # mark all categories as being included (checked)
        for i in xrange(self.categories_listbox.GetCount()):
            self.categories_listbox.Check(i)

        # mark all contexts as being included (checked)
        def check_all(item):
            if item.Is3State():
                item.Set3StateValue(wx.CHK_CHECKED)
            else:
                item.Check(True)

            child, cookie = self.ctx_tree.GetFirstChild(item)
            while child is not None and child.IsOk():
                check_all(child)
                child, cookie = self.ctx_tree.GetNextChild(item, cookie)

        check_all(self.ctx_tree.GetRootItem())
        self.ctx_tree.Refresh()
