"""
Common mixin classes intended to be shared by controls in the mdf viewer.
"""
import wx
import wx.grid
import operator

class GridCopyMixin:
    """mixin for use with a wx.Grid to provide Ctrl+C copy binding"""
    
    def __init__(self, enable_context_menu=True):
        self.Bind(wx.EVT_KEY_DOWN, self.__OnKey)
        
        if enable_context_menu:
            self.__all_to_cb_id = wx.NewId()
            self.Bind(wx.EVT_MENU, self.GridToClipboard, id=self.__all_to_cb_id)

            self.__sel_to_cb_id = wx.NewId()
            self.Bind(wx.EVT_MENU, self.SelectionToClipboard, id=self.__sel_to_cb_id)
            
            self.Bind(wx.grid.EVT_GRID_CELL_RIGHT_CLICK, self.OnContextMenu)
            self.Bind(wx.grid.EVT_GRID_LABEL_RIGHT_CLICK, self.OnContextMenu)

    def __OnKey(self, event):
        # Ctrl+C
        if event.ControlDown() and event.GetKeyCode() == 67:
            self.SelectionToClipboard()
            return

        # Skip other Key events
        event.Skip()

    def CreateContextMenu(self, menu):
        """
        Called when the context menu is popped up.
        Add menu items by overriding this method.
        """
        menu.Append(self.__all_to_cb_id, "Copy all to clipboard")
        menu.Append(self.__sel_to_cb_id, "Copy selection to clipboard")

    def OnContextMenu(self, event):
        menu = wx.Menu()
        self.CreateContextMenu(menu)
        self.PopupMenu(menu)
        menu.Destroy()

    def GridToClipboard(self, event=None):
        """copy whole grid including column and row labels to the clipboard"""
        lines = []
        
        col_labels = [self.GetColLabelValue(c) for c in xrange(self.GetNumberCols())]
        if reduce(operator.or_, map(bool, col_labels), False):
            lines.append("\t".join([""] + col_labels))

        for r in range(self.GetNumberRows()):
            row = [str(self.GetRowLabelValue(r))]
            for c in range(self.GetNumberCols()):
                value = self.GetCellValue(r, c)
                row.append(str(value).replace("\t", "     "))
            lines.append("\t".join(row))
        data = "\n".join(lines)

        # create text data object
        clipboard = wx.TextDataObject()
        clipboard.SetText(data)

        # put the data in the clipboard
        if wx.TheClipboard.Open():
            wx.TheClipboard.SetData(clipboard)
            wx.TheClipboard.Close()
        else:
            wx.MessageBox("Can't open the clipboard", "Error")

    def SelectionToClipboard(self, event=None):
        """copies the current selection to the clipboard"""
        # get the number of rows and cols to copy
        br = self.GetSelectionBlockBottomRight()[0]
        tl = self.GetSelectionBlockTopLeft()[0]
        rows = br[0] - tl[0] + 1
        cols = br[1] - tl[1] + 1

        # build a tab separated list of lines to copy to the clipboard
        lines = []
        for r in range(rows):
            row = []
            for c in range(cols):
                value = self.GetCellValue(tl[0] + r, tl[1] + c)
                row.append(str(value).replace("\t", "     "))
            lines.append("\t".join(row))
        data = "\n".join(lines)

        # create text data object
        clipboard = wx.TextDataObject()
        clipboard.SetText(data)

        # put the data in the clipboard
        if wx.TheClipboard.Open():
            wx.TheClipboard.SetData(clipboard)
            wx.TheClipboard.Close()
        else:
            wx.MessageBox("Can't open the clipboard", "Error")
