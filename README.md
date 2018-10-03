# splitfile.py

Python file-like object that facilitates reading and writing of binary data split across multiple volumes (files) of a specified size. Includes support for compression and encryption.  Sequential read/write methods only.

Can be used with modules such as tarfile, enabling read/write of compressed, encrypted, and split tar archives (use 'w|' and 'r|' tarfile stream modes).

### Example
```
import splitfile

with splitfile.open('data.bin', 'wb', volume_size=1000000, compression=True, 
                   lzma_preset=9, aes_key=b'encryption-key') as f:
    f.write(data)
   
Result:
data.bin
data.bin.2
data.bin.3
...
```
### Dependencies

PyCryptodome for AES
