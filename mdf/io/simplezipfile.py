"""
classes used for reading and writing compressed mdf objects
"""
from zipfile import ZipFile, ZIP_DEFLATED
from tempfile import NamedTemporaryFile
import logging
import os

_log = logging.getLogger(__name__)

class SimpleZipFile(object):
    """
    File-like object for reading from and writing to simple zipfiles
    with a single inner file
    """

    def __init__(self, filename, inner_filename=None, mode="w",
                    compression=ZIP_DEFLATED, allowZip64=False):
        mode = mode.rstrip("+b")
        assert mode in ("r", "w"), "unsupported mode '%s'" % mode
        self.__mode = mode
        self.__filename = filename
        self.__inner_filename = inner_filename
        self.__compression = compression
        self.__allow_zip_64 = allowZip64
        
        # keep a reference to unlink as during shutdown the os module may be
        # discarded before this object is
        self.__unlink = os.unlink
        self.__closed = False

        # Open a named temporary file for reading and writing.
        #
        # Don't delete on close because the file needs to be closed before it
        # can be written to the zipfile as there's no way to incremenatally write
        # bytes to a zipfile, and loading the whole file into memory could be
        # problematic.
        #
        self._fh = NamedTemporaryFile("w+b", delete=False)

        if mode == "r":
            # unzip the whole file into the temporary file
            zip_fh = ZipFile(filename, "r")
            try:
                if inner_filename:
                    self._fh.write(zip_fh.read(inner_filename))
                else:
                    infos = zip_fh.infolist()
                    assert len(infos) == 1, "Multiple entries found (%s)" % infos
                    self._fh.write(zip_fh.read(infos[0]))

                # seek self back to the start of the file for reading
                self._fh.seek(0, os.SEEK_SET)
            finally:
                zip_fh.close()

    def __getattr__(self, name):
        if name in self.__dict__:
            return self.__dict__[name]
        if name in self.__class__.__dict__:
            return self.__dict__.__class__[name]
        return getattr(self.__dict__["_fh"], name)

    def __del__(self):
        self.close()

    def close(self):
        if self.__closed:
            return

        tmp_filename = self._fh.name
        self._fh.close()
        self.__closed = True
        try:
            # write everything to the zipfile
            if "w" in self.__mode:
                inner_filename = self.__inner_filename
                if not inner_filename:
                    base, ext = os.path.splitext(self.__filename)
                    inner_filename = os.path.basename(base) + ".txt"

                _log.debug("Compressing %s" % (self.__filename))
                zip_fh = ZipFile(self.__filename, "w", self.__compression)
                try:
                    zip_fh.write(tmp_filename, inner_filename)
                finally:
                    zip_fh.close()
        finally:
            # delete the tempfile
            self.__unlink(tmp_filename)
            self._fh = None
