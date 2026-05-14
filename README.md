# streamview
Zero-copy non-blocking pipe-like structure

Stream primitive extending Python's standard I/O model with zero-copy and scatter/gather semantics.

- Zero-copy operations using `memoryview`
- `BufferedIOBase` compatible API
- Non-blocking I/O semantics
- Compatible with existing Python I/O tooling and libraries
- Efficient handling of fragmented buffers without unnecessary copies
- Explicit ownership transfer of buffers
- Lightweight stream copies with shared underlying buffers
- Supports both high-level I/O usage and low-level buffer access
- Lightweight and dependency-free
- Designed for high-throughput data pipelines

## Example
```python
from streamview import Stream

stream = Stream()

stream.write(b"hello")
stream.write(b"world")

print(stream)
# <Stream views=2 nbytes=10>

print(bytes(stream.read()))
# b"helloworld"
```

## Install
### Production
```bash
pip install streamview
```

### Development
```bash
git clone https://github.com/hpca-uji/streamview.git
cd streamview
pip install -e .
```

## Documentation
### Classes
- `Stream()`

  Zero-copy non-blocking pipe-like

  Interface mimics a non-blocking BufferedIOBase,
  but operations return memoryviews instead of bytes.

  Operations consume the stream.
  Operations are not thread-safe.
  Reader is responsible of releasing views.
  Writer hands off responsibility over views.

  Stream has `with`, `bytes`, `bool`, `copy` and `deepcopy` support.

  Extends: `BufferedIOBase`

  - `__len__() -> int`

    Number of views held in stream

  - `nbytes -> int`

    Number of bytes held in stream

  - `views -> Iterable[memoryview]`

    Peeking iterator over views

  - `__getitem__(index) -> memoryview`

    Peek a view from the stream

  - `unreadview(v: memoryview) -> int`

    Unread a view into the stream

  - `readview() -> memoryview`

    Read a view from stream

  - `readviews() -> Iterable[memoryview]`

    Read all views from stream

  - `unwriteview() -> memoryview`

    Unwrite a view from the stream

  - `writeview(v: memoryview) -> int`

    Write a view into the stream

  - `writeviews(vs: Iterable[memoryview]) -> int`

    Write many views into the stream

  - `frombuffer(b: Buffer) -> Stream`

      Construct a stream from a buffer

  - `toview() -> memoryview`

      Transform stream to contiguous view (may copy)

  - `copy() -> Stream`

    Shallow copy of stream

### Functions
- `byteview(b: Buffer) -> memoryview`

  Return a byte view of a buffer

## Acknowledgments
The library has been partially supported by:
- Project PID2023-146569NB-C22 "Inteligencia sostenible en el Borde-UJI" funded by the Spanish Ministry of Science, Innovation and Universities.
- Project C121/23 Convenio "CIBERseguridad post-Cuántica para el Aprendizaje FEderado en procesadores de bajo consumo y aceleradores (CIBER-CAFE)" funded by the Spanish National Cybersecurity Institute (INCIBE).

![](footer.jpg)