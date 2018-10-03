# splitfile.py

Python file-like object that facilitates reading and writing of binary data split across multiple volumes (files) of a specified size. Includes support for compression and encryption.  Sequential read/write methods only.

### Example
```
import pySplitfile

with pySplitfile.open('data.bin', volume_size=1000000, compression=True, lzma_preset=6, aes_key='encryption-key') as f:
    f.write(data)
   
```
### Dependencies

PyCryptodome for AES
