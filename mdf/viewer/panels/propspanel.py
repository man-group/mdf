"""
Panel for showing node properties
"""
import wx
import inspect
from ...nodes import MDFNode

class NodeProperties(wx.Panel):
    """
    Panel that shows the node name and a few properties including:
        - node type
        - module
        - filename
        - line number
    """
    
    def __init__(self, parent, id=wx.ID_ANY):
        wx.Panel.__init__(self, parent, id)

        # static text boxes for the node properties
        self.title = wx.StaticText(self, label=" " * 50)
        self.title.SetFont(wx.Font(12, wx.SWISS, wx.NORMAL, wx.BOLD))

        self.nodetype = wx.StaticText(self, label="?")
        self.valuetype = wx.StaticText(self, label="?")
        self.categories = wx.StaticText(self, label="?")
        self.modulename = wx.StaticText(self, label="?")
        self.path = wx.StaticText(self, label="?")
        self.line = wx.StaticText(self, label="?")

        self.sizer = sizer = wx.BoxSizer(wx.VERTICAL)
        sizer.Add(self.title, 0, wx.ALL | wx.EXPAND, border=10)

        grid_sizer = wx.GridBagSizer(vgap=3, hgap=10)

        grid_sizer.Add(wx.StaticText(self, label="Node Type:"), (0, 0))
        grid_sizer.Add(self.nodetype, (0, 1))

        grid_sizer.Add(wx.StaticText(self, label="Value Type:"), (1, 0))
        grid_sizer.Add(self.valuetype, (1, 1))

        grid_sizer.Add(wx.StaticText(self, label="Categories:"), (2, 0))
        grid_sizer.Add(self.categories, (2, 1))

        grid_sizer.Add(wx.StaticText(self, label="Module:"), (3, 0))
        grid_sizer.Add(self.modulename, (3, 1))

        grid_sizer.Add(wx.StaticText(self, label="Filename:"), (4, 0))
        grid_sizer.Add(self.path, (4, 1))

        grid_sizer.Add(wx.StaticText(self, label="Line:"), (5, 0))
        grid_sizer.Add(self.line, (5, 1))        

        sizer.Add(grid_sizer, 0, wx.LEFT | wx.RIGHT | wx.EXPAND, border=25)

        self.ctx_title = wx.StaticText(self, label="")
        self.ctx_title.SetFont(wx.Font(10, wx.SWISS, wx.NORMAL, wx.BOLD))
        sizer.Add(self.ctx_title, 0, wx.ALL | wx.EXPAND, border=10)

        self.ctx_grid_sizer = wx.GridBagSizer(vgap=3, hgap=10)
        sizer.Add(self.ctx_grid_sizer, 0, wx.LEFT | wx.RIGHT | wx.EXPAND, border=25)

        self.SetSizer(wx.BoxSizer(wx.VERTICAL))
        self.GetSizer().Add(sizer, 0, wx.EXPAND)
        self.Fit()

        # hide everything initially
        self.GetSizer().Hide(sizer, recursive=True)

    def SetNode(self, node, ctx):
        """update the panel with values from a node"""
        self.Freeze()
        try:
            self.title.SetLabel(node.short_name)
            self.nodetype.SetLabel(node.node_type or "?")
            self.modulename.SetLabel(node.modulename or "?")
            self.path.SetLabel("?")
            self.line.SetLabel("?")

            categories = ", ".join([str(x) for x in node.categories if x])
            self.categories.SetLabel(categories)

            # get the type of the current value
            valuetype = None
            if node.has_value(ctx) and not node.is_dirty(ctx):
                value = ctx[node]
                valuetype = repr(type(value))
                if hasattr(value, "__class__"):
                    valuetype = getattr(value.__class__, "__name__", repr(value.__class__))
            self.valuetype.SetLabel(valuetype or "?")
    
            try:
                # get the module and line number using inspect
                path = inspect.getsourcefile(node.func)
                self.path.SetLabel(path)
                source, line = inspect.findsource(node.func)
                self.line.SetLabel(str(line) if line > 0 else "?")
            except (IOError, TypeError, AttributeError):
                # if that fails try getting the module name from the node
                try:
                    if node.modulename:
                        module = __import__(node.modulename)
                        self.path.SetLabel(module.__file__)
                except (ImportError, AttributeError):
                    pass
    
            # show the shift set if there is one
            shift_set = ctx.get_shift_set()
            self.ctx_grid_sizer.Clear(True)
            if shift_set:
                self.ctx_title.SetLabel("ctx shift set:")
                for i, n in enumerate(sorted(shift_set.iterkeys(), key=lambda x: x.short_name)):
                    # convert the shift value to some sensible looking string
                    shift_value = shift_set[n]
                    if isinstance(shift_value, basestring):
                        shift_value = repr(shift_value)

                    if isinstance(shift_value, MDFNode):
                        shift_value = shift_value.short_name

                    if isinstance(shift_value, float):
                        shift_value = "%.9f" % shift_value

                    shift_value = str(shift_value)
                    if "\n" in shift_value:
                        shift_value = shift_value.split("\n", 1)[0] + "..."

                    if len(shift_value) > 100:
                        shift_value = shift_value[:100] + "..."

                    # add a row to the grid
                    self.ctx_grid_sizer.Add(wx.StaticText(self, label=n.short_name), (i, 0))
                    self.ctx_grid_sizer.Add(wx.StaticText(self, label="="), (i, 1))
                    self.ctx_grid_sizer.Add(wx.StaticText(self, label=str(shift_value)), (i, 2))
            else:
                self.ctx_title.SetLabel("")

            self.GetSizer().Show(self.sizer, recursive=True)
            self.GetSizer().Layout()
        finally:
            self.Thaw()
