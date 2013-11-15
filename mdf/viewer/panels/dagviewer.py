"""
Panel for showing mdf DAG
"""
import wx
import tempfile
import shutil
import os

try:
    from wx.lib.pdfwin import PDFWindow
    _pdf_available = True
except ImportError:
    _pdf_available = False

class DAGViewer(wx.Panel):
    """
    Panel that calls ctx.to_dot for a particular node to
    render it to a .pdf and displays that using an embedded
    pdf ActiveX control.
    """

    def __init__(self, parent, id=wx.ID_ANY):
        wx.Panel.__init__(self, parent, id)
        sizer = wx.BoxSizer(wx.HORIZONTAL)
        if _pdf_available:
            # create the pdf viewer
            self.pdf = PDFWindow(self, style=wx.SUNKEN_BORDER)
            sizer.Add(self.pdf, 1, flag=wx.EXPAND)

            # create the tmp dir and bind to the close event
            self.tmpdir = tempfile.mkdtemp()
            self.Bind(wx.EVT_CLOSE, self.OnClose)
        else:
            sizer.Add(wx.StaticText(self, -1, "PDF viewer not available"))

        self.SetSizer(sizer)
        self.Fit()

    def OnClose(self, event):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def SetNodes(self, nodes, ctx, max_depth, layout_style):
        if not _pdf_available:
            return

        filename = os.path.join(self.tmpdir, "dot.pdf")
        ctx.to_dot(filename,
                    nodes=nodes,
                    all_contexts=False,
                    max_depth=max_depth,
                    rankdir=layout_style)

        self.pdf.LoadFile(filename)
        self.GetSizer().Layout()
