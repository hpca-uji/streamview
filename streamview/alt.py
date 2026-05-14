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
        other.writelines(map(bytes, self._views))
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

    def _merge_views(self) -> None:
        """Merge views into contiguous view"""
        b = byteview(b"".join(self._views))
        self._views.clear()
        self._views.append(b)

    # stream base methods
    def unreadview(self, v: memoryview) -> int:
        """Unread a view into the stream"""
        size = super().unreadview(v)
        self._merge_views()
        return size

    def writeview(self, v: memoryview) -> int:
        """Write a view into the stream"""
        size = super().writeview(v)
        self._merge_views()
        return size
