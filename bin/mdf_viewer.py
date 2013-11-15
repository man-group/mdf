"""
Graphical viewer for MDF valuation graphs.
"""
import argparse
import sys
import wx
from mdf.viewer import MDFViewerFrame
from datetime import datetime

if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    parsed_args = arg_parser.parse_args(sys.argv)
    
    filename = arg_parser.filename

    start_date = arg_parser.start_date
    
    if start_date:
        start_date = datetime.strptime(start_date, "%Y-%m-%d")

    end_date = arg_parser.end_date
    if end_date:
        end_date = datetime.strptime(end_date, "%Y-%m-%d")

    app = wx.App(redirect=False)
    frame = MDFViewerFrame(None, -1, "MDF Viewer", size=(800, 600))
    frame.CenterOnScreen()
    frame.Show()

    if filename:
        frame.Open(filename)

    frame.SetDefaultDateRange(start_date, end_date)
    app.MainLoop()
