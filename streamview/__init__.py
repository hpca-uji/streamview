"""Stream structure"""
from __future__ import annotations

import io
from collections import abc, deque


__all__ = (
    "byteview",
    "Stream"
)


# bytearray fast-path
try:
    from sabctools import bytearray_malloc as bytearray
except Exception:
    pass


def byteview(b: abc.Buffer) -> memoryview:
    """Return a byte view of a buffer"""
    with memoryview(b) as view:
        return view.cast("B")


def view_index(b: abc.Buffer, sub: bytes) -> int:
    """Find lowest index where substring is found"""
    if len(sub) != 1:
        raise TypeError("Only single byte substring are supported")
    for i, byte in enumerate(b):  # type: ignore
        if byte == sub:
            return i
    else:
        raise ValueError("Substring not found")


# memoryview fast-path
try:
    view_index = memoryview.index  # type: ignore # noqa: F811
except AttributeError:
    pass


class Stream(io.BufferedIOBase):
    """
    Zero-copy non-blocking pipe-like

    Interface mimics a non-blocking BufferedIOBase,
    but operations return memoryviews instead of bytes.

    Operations consume the stream.
    Operations are not thread-safe.
    Reader is responsible of releasing views.
    Writer hands off responsibility over views.

    Stream has `with`, `bytes`, `bool`, `copy` and `deepcopy` support.
    """

    __slots__ = ("_nbytes", "_views")

    def __init__(self):
        """Initialize stream"""
        self._views = deque[memoryview]()
        self._nbytes = 0

    # stream properties
    def __repr__(self) -> str:
        """Stream representation"""
        return f"<{self.__class__.__name__} views={len(self)} nbytes={self.nbytes} at 0x{id(self):x}>"

    def __bool__(self) -> bool:
        """Stream has data (would read not block)"""
        return bool(self._views)

    def __len__(self) -> int:
        """Number of views held in stream"""
        return len(self._views)

    @property
    def nbytes(self) -> int:
        """Number of bytes held in stream"""
        return self._nbytes

    @property
    def views(self) -> abc.Iterable[memoryview]:
        """Peeking iterator over views"""
        return iter(self._views)

    # stream methods
    def __getitem__(self, index):
        """Peek a view from the stream"""
        return self._views[index]

    def unreadview(self, v: memoryview, /) -> int:
        """Unread a view into the stream"""
        size = len(v)

        if size > 0:
            self._views.appendleft(v)
        else:
            v.release()

        self._nbytes += size
        return size

    def readview(self) -> memoryview:
        """Read a view from stream"""
        if not self:
            raise BlockingIOError()

        v = self._views.popleft()
        self._nbytes -= len(v)
        return v

    def readviews(self) -> abc.Iterable[memoryview]:
        """Read all views from stream"""
        while self:
            yield self.readview()

    def unwriteview(self) -> memoryview:
        """Unwrite a view from the stream"""
        if not self:
            raise BlockingIOError()

        v = self._views.pop()
        self._nbytes -= len(v)
        return v

    def writeview(self, v: memoryview, /) -> int:
        """Write a view into the stream"""
        size = len(v)

        if size > 0:
            self._views.append(v)
        else:
            v.release()

        self._nbytes += size
        return size

    def writeviews(self, vs: abc.Iterable[memoryview], /) -> int:
        """Write many views into the stream"""
        size = 0

        for v in vs:
            size += self.writeview(v)

        return size

    # helper methods
    @classmethod
    def frombuffer(cls, b: abc.Buffer, /) -> Stream:
        """Construct a stream from a buffer"""
        stream = cls()
        stream.write(b)
        return stream

    def toview(self) -> memoryview:
        """Transform stream to contiguous view (may copy)"""
        return self.read() if self else byteview(b"")

    def __bytes__(self) -> bytes:
        """Transform stream to bytes (will copy)"""
        with self.toview() as view:
            return bytes(view)

    def copy(self) -> Stream:
        """Shallow copy of stream"""
        other = self.__class__()
        other.writelines(self._views)
        return other

    def __copy__(self) -> Stream:
        """Shallow copy of stream"""
        return self.copy()

    def __deepcopy__(self, memo) -> Stream:
        """Deep copy of stream"""
        stream = self.copy()
        memo[id(self)] = stream
        stream.writeview(stream.read())
        return stream

    # io methods
    def write(self, b: abc.Buffer, /) -> int:
        """Inserts buffer into stream"""
        v = byteview(b)
        size = self.writeview(v)
        return size

    def writelines(self, bs: abc.Iterable[abc.Buffer], /) -> int:
        """Write many buffers into the stream"""
        size = 0

        for b in bs:
            size += self.write(b)

        return size

    def read1(self, size: int = -1, /) -> memoryview:
        """Reads, with at most one operation, and returns a memoryview"""
        v = self.readview()

        if size < 0 or size >= len(v):
            return v

        with v:
            read, keep = v[:size], v[size:]
        self.unreadview(keep)

        return read

    def readinto1(self, b: abc.Buffer, /) -> int:
        """Reads, with at most one operation, into a buffer"""
        with byteview(b) as dst:
            with self.read1(len(dst)) as src:
                size = len(src)
                dst[:size] = src

        return size

    def readinto(self, b: abc.Buffer, /) -> int:
        """Reads, until drained, into a buffer"""
        if not self:
            raise BlockingIOError()

        read = 0
        with byteview(b) as dst:
            while self and read < len(dst):
                with dst[read:] as free:
                    read += self.readinto1(free)

        return read

    def read(self, size: int = -1, /) -> memoryview:
        """Reads, until drained, and returns a memoryview (may copy)"""
        if size < 0:
            size = self.nbytes
        else:
            size = min(size, self.nbytes)

        # View path
        v = self.read1(size)
        if len(v) == size:
            return v

        # Copy path
        self.unreadview(v)
        v = bytearray(size)
        self.readinto(v)
        return byteview(v)

    def readline(self) -> memoryview:
        """Read a line and return a memoryview (may copy)"""
        if not self:
            raise BlockingIOError()

        with Stream() as stream:
            for v in self.readviews():
                try:
                    i = view_index(v, b"\n")
                except ValueError:
                    stream.writeview(v)
                    continue
                else:
                    self.unreadview(v)
                    v = self.read1(i)
                    stream.writeview(v)
                    break
            return stream.read()

    def readlines(self) -> abc.Iterable[memoryview]:
        """Read many lines from the stream"""
        while self:
            yield self.readline()

    def __iter__(self) -> abc.Iterable[memoryview]:
        """Read many lines from the stream"""
        return self.readlines()

    def seek(self, offset, whence=io.SEEK_SET, /) -> int:
        """
        Change stream position to the given byte offset (only SEEK_SET is supported)

        *Note*: consumes data as read, but guaranteeing no copies
        """
        if whence is not io.SEEK_SET:
            raise OSError()

        read = 0
        while self and read < offset:
            with self.read1(offset - read) as v:
                read += len(v)

        return read

    def truncate(self, size=None, /) -> int:
        """Resize the stream to the given size in bytes"""
        if size is None or size < 0:
            size = 0

        with Stream() as stream:
            # Swap
            self._views, stream._views = stream._views, self._views
            self._nbytes, stream._nbytes = stream._nbytes, self._nbytes

            # Slice
            read = 0
            while stream and read < size:
                v = stream.read1(size - read)
                read += self.writeview(v)

        return read

    def close(self) -> None:
        """Release all views"""
        for v in self.readviews():
            v.release()

    def __del__(self) -> None:
        """Best effort finalizer"""
        try:
            self.close()
        except:  # noqa: E722
            pass

    # io properties
    def tell(self) -> int:
        """Return the current stream position"""
        return 0

    def isatty(self) -> bool:
        """Return True if the stream is interactive"""
        return False

    def readable(self) -> bool:
        """Return True if the stream can be read from"""
        return True

    def seekable(self) -> bool:
        """Return True if the stream supports random access"""
        return True

    def writable(self) -> bool:
        """Return True if the stream supports writing"""
        return True

    def flush(self) -> None:
        """Flush the buffers of the stream (not applicable)"""
        pass

    # io stubs
    def fileno(self) -> int:
        """Return the underlying file descriptor (not applicable)"""
        raise OSError()

    def detach(self) -> abc.Buffer:
        """Separate the underlying raw stream and return it (not applicable)"""
        raise io.UnsupportedOperation()
