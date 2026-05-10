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


def buffer_index(b: abc.Buffer, sub: bytes) -> int:
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
    buffer_index = memoryview.index  # type: ignore
except AttributeError:
    pass


class Stream(io.BufferedIOBase):
    """
    Zero-copy non-blocking pipe-like

    Interface mimics a non-blocking BufferedIOBase,
    but operations return memoryviews instead of bytes.

    Operations consume the stream.
    Operations are not thread-safe.
    Reader is responsible of releasing buffers.
    Writer hands off responsibility over buffers.

    Stream has `with`, `bytes`, `bool`, `copy` and `deepcopy` support.
    """

    __slots__ = ("_nbytes", "_buffers")

    def __init__(self):
        """Initialize stream"""
        self._buffers = deque[memoryview]()
        self._nbytes = 0

    # stream properties
    def __repr__(self) -> str:
        """Stream representation"""
        return f"<{self.__class__.__name__} buffers={len(self)} nbytes={self.nbytes} at 0x{id(self):x}>"

    def __bool__(self) -> bool:
        """Stream has data (would read not block)"""
        return bool(self._buffers)

    def __len__(self) -> int:
        """Number of buffers held in stream"""
        return len(self._buffers)

    @property
    def nbytes(self) -> int:
        """Number of bytes held in stream"""
        return self._nbytes

    @property
    def buffers(self) -> abc.Iterable[memoryview]:
        """Peeking iterator over buffers"""
        return iter(self._buffers)

    # stream methods
    def __getitem__(self, index):
        """Peek a buffer from the stream"""
        return self._buffers[index]

    def unreadbuffer(self, b: memoryview, /) -> int:
        """Unread a buffer into the stream"""
        size = len(b)

        if size > 0:
            self._buffers.appendleft(b)
        else:
            b.release()

        self._nbytes += size
        return size

    def readbuffer(self) -> memoryview:
        """Read a buffer from stream"""
        if not self:
            raise BlockingIOError()

        b = self._buffers.popleft()
        self._nbytes -= len(b)
        return b

    def readbuffers(self) -> abc.Iterable[memoryview]:
        """Read all buffers from stream"""
        while self:
            yield self.readbuffer()

    def unwritebuffer(self) -> memoryview:
        """Unwrite a buffer from the stream"""
        if not self:
            raise BlockingIOError()

        b = self._buffers.pop()
        self._nbytes -= len(b)
        return b

    def writebuffer(self, b: memoryview, /) -> int:
        """Write a buffer into the stream"""
        size = len(b)

        if size > 0:
            self._buffers.append(b)
        else:
            b.release()

        self._nbytes += size
        return size

    def writebuffers(self, bs: abc.Iterable[memoryview], /) -> int:
        """Write many buffers into the stream"""
        size = 0

        for b in bs:
            size += self.writebuffer(b)

        return size

    # helper methods
    @classmethod
    def frombuffer(cls, b: abc.Buffer, /) -> Stream:
        """Construct a stream from a buffer"""
        stream = cls()
        stream.write(b)
        return stream

    def tobuffer(self) -> memoryview:
        """Transform stream to contiguous buffer (may copy)"""
        return self.read() if self else byteview(b"")

    def __bytes__(self) -> bytes:
        """Transform stream to bytes (will copy)"""
        with self.tobuffer() as view:
            return bytes(view)

    def copy(self) -> Stream:
        """Shallow copy of stream"""
        other = self.__class__()
        other.writelines(self._buffers)
        return other

    def __copy__(self) -> Stream:
        """Shallow copy of stream"""
        return self.copy()

    def __deepcopy__(self, memo) -> Stream:
        """Deep copy of stream"""
        stream = self.copy()
        memo[id(self)] = stream
        stream.writebuffer(stream.read())
        return stream

    # io methods
    def write(self, b: abc.Buffer, /) -> int:
        """Inserts buffer into stream"""
        buffer = byteview(b)
        size = self.writebuffer(buffer)
        return size

    def writelines(self, bs: abc.Iterable[abc.Buffer], /) -> int:
        """Write many buffers into the stream"""
        size = 0

        for b in bs:
            size += self.write(b)

        return size

    def read1(self, size: int = -1, /) -> memoryview:
        """Reads, with at most one operation, and returns a memoryview"""
        b = self.readbuffer()

        if size < 0 or size >= len(b):
            return b

        with b:
            read, keep = b[:size], b[size:]
        self.unreadbuffer(keep)

        return read

    def readinto1(self, b: abc.Buffer, /) -> int:
        """Reads, with at most one operation, into a buffer"""
        with byteview(b) as view:
            with self.read1(len(view)) as buffer:
                size = len(buffer)
                view[:size] = buffer

        return size

    def readinto(self, b: abc.Buffer, /) -> int:
        """Reads, until drained, into a buffer"""
        if not self:
            raise BlockingIOError()

        read = 0
        with byteview(b) as view:
            while self and read < len(view):
                with view[read:] as subview:
                    read += self.readinto1(subview)

        return read

    def read(self, size: int = -1, /) -> memoryview:
        """Reads, until drained, and returns a memoryview (may copy)"""
        if size < 0:
            size = self.nbytes
        else:
            size = min(size, self.nbytes)

        # View path
        b = self.read1(size)
        if len(b) == size:
            return b

        # Copy path
        self.unreadbuffer(b)
        b = bytearray(size)
        self.readinto(b)
        return byteview(b)

    def readline(self) -> memoryview:
        """Read a line and return a memoryview (may copy)"""
        if not self:
            raise BlockingIOError()

        with Stream() as stream:
            for b in self.readbuffers():
                try:
                    i = buffer_index(b, b"\n")
                except ValueError:
                    stream.writebuffer(b)
                    continue
                else:
                    self.unreadbuffer(b)
                    b = self.read1(i)
                    stream.writebuffer(b)
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
            with self.read1(offset - read) as view:
                read += len(view)

        return read

    def truncate(self, size=None, /) -> int:
        """Resize the stream to the given size in bytes"""
        if size is None or size < 0:
            size = 0

        with Stream() as stream:
            # Swap
            self._buffers, stream._buffers = stream._buffers, self._buffers
            self._nbytes, stream._nbytes = stream._nbytes, self._nbytes

            # Slice
            read = 0
            while stream and read < size:
                view = stream.read1(size - read)
                read += self.writebuffer(view)

        return read

    def close(self) -> None:
        """Release all buffers"""
        for b in self.readbuffers():
            b.release()

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
        """Flush the write buffers of the stream (not applicable)"""
        pass

    # io stubs
    def fileno(self) -> int:
        """Return the underlying file descriptor (not applicable)"""
        raise OSError()

    def detach(self) -> abc.Buffer:
        """Separate the underlying raw stream from the buffer and return it (not applicable)"""
        raise io.UnsupportedOperation()
