"""
Util funtions used by the viewer classes
"""
import wx
from datetime import datetime
import os

def to_datetime(wx_date):
    return datetime(wx_date.Year, wx_date.Month+1, wx_date.Day)

def to_wxdate(date):
    return wx.DateTimeFromDMY(date.day, date.month-1, date.year)

def load_icon(filename, width=-1, height=-1):
    """return a wx.Icon from the icons folder"""
    icon_path = os.path.join(os.path.dirname(__file__), "icons", filename)
    return wx.Icon(icon_path, wx.BITMAP_TYPE_ICO, width, height)

def load_bitmap(filename, width=-1, height=-1):
    """load icon as a wx.Bitmap"""
    i = load_icon(filename, width, height)
    b = wx.EmptyBitmap(i.GetWidth(), i.GetHeight())
    b.CopyFromIcon(i)
    return b