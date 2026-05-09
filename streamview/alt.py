"""Alternative stream structure"""

from collections import abc

from streamview import Stream, byteview


__all__ = (
    "PipeStream",
    "BytesStream"
)


class PipeStream(Stream):
    """Stream that emulates a pipe behavior"""

    # stream extended methods
    def copy(self) -> Stream:
        """Shallow copy of stream"""
        other = self.__class__()
        other.writelines(map(bytes, self._buffers))
        return other

    # io methods
    def write(self, b: abc.Buffer) -> int:
        """Inserts buffer into stream"""
        return super().write(bytes(b))

    def read1(self, size: int = -1, /) -> memoryview:
        """Reads, with at most one operation, and returns a memoryview"""
        return byteview(bytes(super().read1(size)))


class BytesStream(Stream):
    """Stream that emulates a bytes behavior"""

    def _merge_buffers(self) -> None:
        """Merge buffers into contiguous memory"""
        b = byteview(b"".join(self._buffers))
        self._buffers.clear()
        self._buffers.append(b)

    # stream base methods
    def unreadbuffer(self, buffer: memoryview) -> int:
        """Unread a buffer into the stream"""
        size = super().unreadbuffer(buffer)
        self._merge_buffers()
        return size

    def writebuffer(self, buffer: memoryview) -> int:
        """Write a buffer into the stream"""
        size = super().writebuffer(buffer)
        self._merge_buffers()
        return size
