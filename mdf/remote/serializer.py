"""
Subclass of the standard Pyro Serializer to support larger objects
by compressing them on the fly rather than pickle them to a string
and then compress that string.

bz2 is used as it was found to be faster than zlib for this purpose.

Disable by setting the environment variable 'MDF_PYRO_NO_BZ2=1'
"""
import Pyro4.core
import Pyro4.util
import logging
import bz2
import os
import sys

if sys.version_info[0] > 2:
    from io import BytesIO
else:
    from StringIO import StringIO as BytesIO

try:
    _Serializer = Pyro4.util.PickleSerializer
except AttributeError:
    _Serializer = Pyro4.util.Serializer

_log = logging.getLogger(__name__)

class MaxSizeStringIO(BytesIO):
    """raises a MemoryError when trying to write more than max size"""

    def __init__(self, max_size=None, buffer=None):
        """
        max_size is the max size of the buffer in bytes
        """
        args = []
        if buffer is not None:
            args.append(buffer)
        BytesIO.__init__(self, *args)
        self.__max_size = max_size

    def write(self, data):
        if self.tell() + len(data) > self.__max_size:
            raise MemoryError
        BytesIO.write(self, data)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

class BZ2Writer(BytesIO):
    """Compresses using bz2 on the fly"""

    def __init__(self, compresslevel=9):
        BytesIO.__init__(self)
        self.__uncompressed_size = 0
        self.__compressor = bz2.BZ2Compressor(compresslevel)

    def write(self, data):
        self.__uncompressed_size += len(data)
        BytesIO.write(self, self.__compressor.compress(data))

    def getvalue(self):
        BytesIO.write(self, self.__compressor.flush())
        return BytesIO.getvalue(self)

    @property
    def uncompressed_size(self):
        return self.__uncompressed_size

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

class BZ2Reader(object):
    """Decompress using bz2 on the fly"""
    _block_size = 64 << 10 # decompress in 64kb chunks
    _buffer_size = 16 << 20 # ~16Mb buffer for decompressed data (this can grow)

    def __init__(self, data):
        self.__decompressor = bz2.BZ2Decompressor()
        self.__decompress = self.__decompressor.decompress
        self.__data = data
        self.__buffer_size = self._buffer_size

        # positions in the uncompressed data
        self.__cpos = 0
        self.__cend = len(data)

        # position in the decompressed data
        self.__dbuf = BytesIO()
        self.__dpos = 0

    def read(self, size=-1):
        if size < 0:
            self.__dbuf.write(self.__decompress(self.__data[self.__cpos:]))
            self.__cpos = self.__cend
            start = self.__dpos
            end = self.__dbuf.tell()
            self.__dpos = end
            return self.__dbuf.getvalue()[start:end]

        # if the buffer's grown to the max buffer size then push the old data off
        if self.__dbuf.tell() > self.__buffer_size:
            # expand the buffer size if we've overrun when decompressing, or if
            # a previous size was more than the buffer (pickle doesn't deal with
            # read returning less than was asked for)
            self.__buffer_size = max(self.__buffer_size, self.__dbuf.tell())

            # move the unread data to the start of the buffer
            data = self.__dbuf.getvalue()[self.__dpos:self.__dbuf.tell()]
            self.__dbuf.seek(0, os.SEEK_SET)
            self.__dpos = 0
            self.__dbuf.write(data)

        while (self.__dbuf.tell() - self.__dpos) < size and self.__cpos < self.__cend:
            end = self.__cpos + self._block_size
            self.__dbuf.write(self.__decompress(self.__data[self.__cpos:end]))
            self.__cpos = end

        start = self.__dpos
        end = min(start + size, self.__dbuf.tell())
        self.__dpos = end
        return self.__dbuf.getvalue()[start:end]

    def readline(self, size=-1):
        c = line = self.read(size=1)
        while c and c != '\n' \
        and (len(line) < size if size > 0 else True):
            c = self.read(size=1)
            line += c
        return line

    def readlines(self, sizehint=-1):
        size = 0
        line = self.readline()
        lines = [line]
        while True:
            line = self.readline()
            if not line:
                break

            lines.append(line)
            size += len(line)

            if sizehint >= 0 and size > sizehint:
                break

        return lines

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.__dbuf.close()

class Serializer(_Serializer):
    """
    Subclass of the normal serializer that will switch between compressed
    and uncompressed data depending on the size of the object being pickled.
    """
    _max_uncompressed_size = 64 << 10 # compress anything more than 64Kb
    _warn_size = 16 << 20 # warn about anything more than 16Mb
    _compression_level = 1
    _is_enabled = True

    def serialize(self, data, compress=True):
        """
        Serialize the given data object, try to compress if told so.
        Returns a tuple of the serialized data and a bool indicating if it is compressed or not.

        :param compress: this is disregarded, the data is always compressed.
        """
        if not self._is_enabled:
            return _Serializer.serialize(self, data, compress)
            
        # try and write the decompressed pickled data first
        try:
            with MaxSizeStringIO(self._max_uncompressed_size) as buf:
                self.pickle.dump(data, buf, self.pickle.HIGHEST_PROTOCOL)
                return buf.getvalue(), False
        except MemoryError:
            pass

        # otherwise compress the data
        with BZ2Writer(self._compression_level) as bz_file:
            self.pickle.dump(data, bz_file, self.pickle.HIGHEST_PROTOCOL)
            if bz_file.uncompressed_size > self._warn_size:
                size_mb = bz_file.uncompressed_size >> 20
                _log.warn(("Transferring a very large object via Pyro (%d Mb)" % size_mb)
                          + ", performance may be affected")
            return bz_file.getvalue(), True

    def deserialize(self, data, compressed=False):
        """Deserializes the given data. Set compressed to True to decompress the data first."""
        if not self._is_enabled:
            return _Serializer.deserialize(self, data, compressed)

        if compressed:
            with BZ2Reader(data) as bz_file:
                return self.pickle.load(bz_file)

        return self.pickle.loads(data)

def disable_custom_pyro_serialization(enable=False):
    """
    Disables the custom pyro serialization.
    Call with enable=True to re-enable.
    
    Return True if previously enabled, False otherwise
    """
    prev_state = Serializer._is_enabled  
    Serializer._is_enabled = enable
    return prev_state

#
# patch Pyro4.util and Pyro.core.Proxy
#
if not int(os.environ.get("MDF_PYRO_NO_BZ2", 0)):
    Pyro4.util.Serializer = Serializer
    Pyro4.core.Proxy._pyroSerializer = Serializer()
