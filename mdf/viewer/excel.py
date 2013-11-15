"""
Util functions for exporting data to Excel
"""
from decorator import decorator
import numpy as np
import pywintypes
import re
 
try:
    import win32com.client
    _com_available = True
except ImportError:
    _com_available = False

@decorator
def needs_com(func, *args, **kwargs):
    if not _com_available:
        raise Exception("win32com not installed")
    return func(*args, **kwargs)

def _to_range(col, row):
    """returns an Excel range string, e.g. 0, 0 => A1"""
    cell = ""
    while col >= 26:
        cell = "%s%s" % (chr(ord("A") + (col % 26)), cell)
        col = (col // 26) - 1
    cell = "%s%s" % (chr(ord("A") + (col % 26)), cell) 
    cell += "%d" % (row + 1)
    return cell

def _to_numeric_range(cell):
    """
    Translate an Excel cell (eg 'A1') into a (col, row) tuple indexed from zero.
    e.g. 'A1' returns (0, 0)
    """
    match = re.match("^\$?([A-Z]+)\$?(\d+)$", cell.upper())
    if not match:
        raise RuntimeError("'%s' is not a valid excel cell address" % cell)
    col, row = match.groups()

    # A = 1
    col_digits = map(lambda c: ord(c) - ord("A") + 1, col)
    col = 0
    for digit in col_digits:
        col = (col * 26) + digit

    row = int(row) - 1
    col = col - 1

    return col, row

def _get_book_and_sheet(app, sheet_name):
    """
    return a workbook and sheet object from a named sheet,
    e.g. "[Book1]Sheet1"

    If no book is specified in the name the active book is used.
    """    
    # split the sheet name into the workbook name and sheet name
    # e.g. "[Book1]Sheet1"
    match = re.match("^(?:\[(.*)\])?(.*)$", sheet_name)            
    if not match:
        raise AssertionError("Error parsing sheet name '%s'" % sheet_name)

    workbook_name, sheet_name = match.groups()
    if workbook_name is None:
        wb = app.ActiveWorkbook
    else:
        try:
            wb = app.Workbooks(workbook_name)
        except pywintypes.com_error:
            raise RuntimeError("Workbook '%s' not found" % workbook_name)

    try:
        sheet = wb.Sheets(sheet_name)
    except pywintypes.com_error:
        raise RuntimeError("Worksheet '%s' not found" % sheet_name)

    return wb, sheet

@needs_com
def export_dataframe(dataframe,
                        sheet_name=None,
                        target_cell="A1",
                        use_active_workbook=False,
                        use_active_sheet=False):
    """
    Copies the provided data into an Excel workbook.
    
    :param dataframe: DataFrame or list of DataFrames
        Each DataFrame is exported to a separate worksheet.

    :param sheet_name: name of sheet to export to. May only be set
        when exporting a single dataframe.

    :param target_cell: top left corner of range to export data to.
        Also accepts 'active' to use the currently selected cell.

    :param use_active_workbook: If True use the currently active workbook.
        Only relevant if sheet_name is None.

    :param use_active_sheet: bool, default=False
        If True, uses the active sheet within an existing workbook as a starting point.
        New sheets are created if multiple dataframes are supplied.
        Only relevant if sheet_name is None.
    """
    if isinstance(dataframe, list):
        dataframes = dataframe
    else:
        dataframes = [dataframe]
    
    # get the excel application object and create a new workbook
    app = win32com.client.GetActiveObject("Excel.Application")
    
    # make sure the type wrapper is built so we have access to any constants
    app = win32com.client.gencache.EnsureDispatch(app)

    orig_calc_mode = app.Calculation
    orig_updating = app.ScreenUpdating
    try:
        # stop Excel from updating while we export the data
        app.ScreenUpdating = False
        app.Calculation = win32com.client.constants.xlCalculationManual

        if target_cell == "active":
            assert sheet_name is None, \
                "Can't specify the sheet name and to use the active cell at the same time"
            use_active_sheet = True
            target_cell = app.ActiveCell.GetAddress()

        # get the workbook and sheet from the various parameters
        if sheet_name is not None:
            wb, sheet = _get_book_and_sheet(app, sheet_name)
            next_sheet_index = wb.Sheets.Count + 1
        elif use_active_sheet:
            # use the currently active sheet
            wb = app.ActiveWorkbook
            sheet = app.ActiveSheet
            next_sheet_index = wb.Sheets.Count + 1
        elif use_active_workbook:
            # use the currently active book and create a new sheet
            wb = app.ActiveWorkbook
            sheet = wb.Sheets.Add()
            next_sheet_index = wb.Sheets.Count + 1
        else:
            # create a new workbook and use the first sheet
            wb = app.Workbooks.Add()
            sheet = wb.Sheets(1)
            next_sheet_index = 2

        # get the coordinates to export to
        top_left_col, top_left_row = _to_numeric_range(target_cell)

        def _format(x):
            # use #NUM! instead of NaN as NaNs show up as 65535 in Excel
            if isinstance(x, float):
                if np.isnan(x) or np.isinf(x):
                    return "#NUM!"
            # use a string representation for tuples and lists
            if isinstance(x, (tuple, list)):
                return repr(x)
            return x
    
        while len(dataframes) > 0:
            dataframe = dataframes.pop(0)
            
            # set the index in the first column
            sheet.Range(_to_range(top_left_col, top_left_row)).Value = "Index"
            range = sheet.Range(_to_range(top_left_col, top_left_row + 1),
                                _to_range(top_left_col, top_left_row + len(dataframe.index)))
            range.Value = [[_format(x)] for x in dataframe.index.tolist()]
    
            # copy the columns
            for i, col in enumerate(dataframe.columns):
                col_num = top_left_col + i + 1
                sheet.Range(_to_range(col_num, top_left_row)).Value = col
                range = sheet.Range(_to_range(col_num, top_left_row + 1),
                                    _to_range(col_num, top_left_row + len(dataframe.index)))
                range.Value = [[_format(x)] for x in dataframe[col].tolist()]

            if len(dataframes) > 0:
                if next_sheet_index > wb.Sheets.Count:
                    sheet = wb.Sheets.Add(After=sheet)
                    next_sheet_index = wb.Sheets.Count + 1
                else:
                    sheet = wb.Sheets.Item(next_sheet_index)
                    next_sheet_index += 1

    finally:
        # restore the previous application state
        app.ScreenUpdating = orig_updating
        app.Calculation = orig_calc_mode
        del app

