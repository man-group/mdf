"""
Panel for showing graphs
"""
import wx
import numpy as np

# force matplotlib to use whatever wx is installed
import sys
sys.frozen = True

from matplotlib.backends.backend_wxagg import FigureCanvasWxAgg
from matplotlib.figure import Figure

class PlotPanel(wx.Panel):
    """
    The PlotPanel has a Figure and a Canvas. OnSize events simply set a 
    flag, and the actual resizing of the figure is triggered by an Idle event.
    See:
        http://www.scipy.org/Matplotlib_figure_in_a_wx_panel
    """

    def __init__(self, parent, dataframes, color=None, dpi=None, **kwargs):
        # initialize Panel
        if 'id' not in kwargs.keys():
            kwargs['id'] = wx.ID_ANY
        if 'style' not in kwargs.keys():
            kwargs['style'] = wx.NO_FULL_REPAINT_ON_RESIZE
        wx.Panel.__init__(self, parent, **kwargs)
        self.parent = parent
        self.dataframes = dataframes

        # initialize matplotlib stuff
        self.figure = Figure(None, dpi)
        self.figure.autofmt_xdate()
        self.canvas = FigureCanvasWxAgg(self, -1, self.figure)
        self.SetColor(color)
        #self._SetSize((800, 600))

        self.draw()
        self._resizeflag = False
        self.Bind(wx.EVT_IDLE, self._onIdle)
        self.Bind(wx.EVT_SIZE, self._onSize)

    def SetColor(self, rgbtuple=None):
        """Set figure and canvas colours to be the same."""
        if rgbtuple is None:
            rgbtuple = wx.SystemSettings.GetColour(wx.SYS_COLOUR_BTNFACE).Get()
        clr = [c/255. for c in rgbtuple]
        self.figure.set_facecolor( clr )
        self.figure.set_edgecolor( clr )
        self.canvas.SetBackgroundColour(wx.Colour(*rgbtuple))

    def _onSize(self, event):
        self._resizeflag = True

    def _onIdle(self, evt):
        if self._resizeflag:
            self._resizeflag = False
            self._SetSize()

    def _SetSize(self, size=None):
        if size is None:
            size = tuple(self.GetClientSize())
        self.SetSize(size)
        self.canvas.SetSize(size)
        self.figure.set_size_inches(float(size[0])/self.figure.get_dpi(),
                                    float(size[1])/self.figure.get_dpi())

    def draw(self):
        ax = self.figure.add_subplot(111)
        for dataframe in self.dataframes:
            x = dataframe.index
            for col in dataframe.columns:
                empty = dataframe[col].count() == 0
                y = dataframe[col].values if not empty else np.zeros(x.shape)
                ax.plot(x, y, label=col)

        try:
            self.figure.autofmt_xdate()
        except:
            pass

        ax.legend(loc="best")
        ax.grid()
