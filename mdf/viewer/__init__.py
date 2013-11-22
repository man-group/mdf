"""
MDF Viewer wx frame.

Example usage:

app = wx.App(redirect=False)
frame = MDFViewerFrame(None, -1, "MDF Viewer", size=(800, 600))
frame.CenterOnScreen()
frame.Show()
"""
import wx
from frame import MDFViewerFrame
from ..context import _get_current_context
from ..nodes import MDFNode

__all__ = [
    "MDFViewerFrame",
    "show",
    "open",
    "reset_viewer",
    "get_selected",
    "get_dataframes",
]


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

try:
    import IPython
    from IPython.lib import guisupport, inputhook

    if IPython.__version__ >= "0.12":
        from IPython.kernel.zmq import kernelapp, eventloops
        
    _in_ipython = _ipython_active()
except ImportError:
    _in_ipython = False

_wx_app = None


def get_wx_app(redirect=False):
    global _wx_app
    if _wx_app is None:
        if _in_ipython:
            _wx_app = guisupport.get_app_wx()
            if IPython.__version__ >= "0.12":
                if kernelapp.IPKernelApp.initialized():
                    eventloops.enable_gui("wx")
            inputhook.enable_wx(_wx_app)
            guisupport.start_event_loop_wx(_wx_app)
        else:
            _wx_app = wx.App(redirect=redirect)
    return _wx_app

_frame = None


def show(nodes=[], ctx=None, contexts=[], redirect=False, style=wx.DEFAULT_FRAME_STYLE):
    """
    Opens a new mdf viewer for a context and nodes, or adds
    the nodes to the currently opened viewer if there is one.

    If nodes is None all the head nodes with values
    will be shown.
    """
    global _frame

    if isinstance(nodes, MDFNode):
        nodes = [nodes]

    if ctx is None:
        ctx = _get_current_context()

    if not contexts:
        contexts = [ctx] * len(nodes)

    root_ctx = ctx.get_parent() or ctx

    # get/create a wx.App
    app = get_wx_app(redirect)

    def _on_frame_close(event):
        global _frame
        if event.GetEventObject() is _frame:
            _frame = None
        event.Skip()

    if _frame is None:
        _frame = MDFViewerFrame(None, -1, "MDF Viewer", size=(800, 600), style=style)
        _frame.Bind(wx.EVT_CLOSE, _on_frame_close)
        _frame.CenterOnScreen()
        _frame.SetContext(root_ctx, nodes=[])
        _frame.Show()

    for node, ctx in zip(nodes, contexts):
        _frame.AddNodeToRoot(node, ctx)

    if not _in_ipython:
        app.MainLoop()


def updating_guard():
    """
    return an object that will stop the current frame from updating
    using with semantics
    """
    if _frame is None:
        return None
    return _frame.UpdatingGuard(_frame)


def open(filename, redirect=False):
    """
    Opens a new mdf viewer with a picked context file.
    """
    # get/create a wx.App
    if _in_ipython:
        app = guisupport.get_app_wx()
    else:
        app = wx.App(redirect=redirect)

    frame = MDFViewerFrame(None, -1, "MDF Viewer", size=(800, 600))
    frame.CenterOnScreen()
    frame.Open(filename)
    frame.Show()

    if _in_ipython:
        inputhook.enable_wx(app)
    else:
        app.MainLoop()


def reset_viewer():
    """set the viewer to the current context - used by pylab"""
    if _frame:
        _frame.SetContext(_get_current_context())


def get_selected():
    """
    Return the currently selected nodes and their contexts from the
    open mdf viewer.

    returns a list of (ctx, node)
    """
    assert _frame is not None, "MDF viewer not open"
    return _frame.GetSelectedContextsAndNodes()


def get_dataframes(ctxs_and_nodes, start=None, end=None):
    """
    compute dataframes for each of the (ctx, node) pairs between
    start and end.
    If start or end are None the currently selected dates will be used.
    for use from PyCrust as 'get_dataframes()'
    """
    assert _frame is not None, "MDF viewer not open"
    return _frame.GetDataFrames(ctxs_and_nodes, start, end)

# aliases for use in ipython
mdf_show = show
mdf_open = open
