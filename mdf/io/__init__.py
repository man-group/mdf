"""
Functions for saving and loading MDF objects.
"""
import os
import sys
import logging
import shutil
from .simplezipfile import SimpleZipFile
from bz2 import BZ2File
from gzip import GzipFile

import pickle

_log = logging.getLogger(__name__)

if sys.version_info[0] > 2:
    basestring = str

# access via the MDFContext.save method
def save_context(ctx, filename, start_date=None, end_date=None):
    """
    Write the context and its state, including all shifted contexts and node
    states, to a binary file.

    The resulting file can be re-loaded using :py:func:`MDFContext.load`.

    If filename endswith .zip or .bz2 or .gz the data will be compressed.
    The :py:func:`MDFContext.load` method is able to load these compressed
    files.

    :param filename: filename of the output file, or an open file handle.

    :param start_date: datetime used as an optional argument to start the mdf
                       viewer in the .bat file.

    :param end_date: datetime used as an optional argument to start the mdf
                     viewer in the .bat file.
    """
    close_fh = True

    # determine what compression to use, if any
    if not isinstance(filename, basestring):
        fh = filename
        close_fh = False
    else:
        # use w+b to workaround problem writing large chunks of data in a single call to fwrite
        # http://support.microsoft.com/default.aspx?scid=kb;en-us;899149

        base, ext = os.path.splitext(filename)
        if ext == ".zip":
            inner_filename = os.path.basename(base) + ".dag"
            fh = SimpleZipFile(filename, inner_filename=inner_filename, mode="w+b")
        elif ext == ".bz2":
            fh = BZ2File(filename, "wb") # w+b isn't a valid mode for BZ2File
        elif ext == ".gz":
            fh = GzipFile(filename, "w+b")
        else:
            fh = open(filename, "w+b")

    try:
        pickle.dump(ctx, fh, pickle.HIGHEST_PROTOCOL)
    finally:
        if close_fh:
            fh.close()


# access via the MDFContext.load static method
def load_context(filename):
    """
    Load a context from a file and return a new MDFContext with the same
    state as the context that was saved (i.e. all the same shifted contexts
    and node values).
    
    :param filename: filename of the file to load or an open file handle.
    """
    close_fh = True

    # determine what compression to use, if any
    if not isinstance(filename, basestring):
        fh = filename
        close_fh = False
    else:
        _, ext = os.path.splitext(filename)
        if ext == ".zip":
            fh = SimpleZipFile(filename, mode="rb")
        elif ext == ".bz2":
            fh = BZ2File(filename, "rb")
        elif ext == ".gz":
            fh = GzipFile(filename, "rb")
        else:
            fh = open(filename, "rb")

    try:
        return pickle.load(fh)
    finally:
        if close_fh:
            fh.close()
