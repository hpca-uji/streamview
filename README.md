# streamview
Zero-copy non-blocking pipe-like structure

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
  Reader is responsible of releasing chunks.
  Writer hands off responsibility over chunks.

  Stream has `with`, `bytes`, `bool`, `copy` and `deepcopy` support.

  Extends: `BufferedIOBase`

  - `__len__() -> int`

    Number of chunks held in stream

  - `nbytes -> int`

    Number of bytes held in stream

  - `chunks -> Iterable[memoryview]`

    Peeking iterator over chunks

  - `__getitem__(index) -> memoryview`

    Peek a chunk from the stream

  - `unreadchunk(chunk: memoryview) -> int`

    Unread a chunk into the stream

  - `readchunk() -> memoryview`

    Read a chunk from stream

  - `readchunks() -> Iterable[memoryview]`

    Read all chunks from stream

  - `unwritechunk() -> memoryview`

    Unwrite a chunk from the stream

  - `writechunk(chunk: memoryview) -> int`

    Write a chunk into the stream

  - `writechunks(chunks: Iterable[memoryview]) -> int`

    Write many chunks into the stream

  - `frombuffer(b: Buffer) -> Stream`

      Construct a stream from a buffer

  - `tobuffer() -> memoryview`

      Transform stream to contiguous buffer (may copy)

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