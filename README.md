# splitfile

Python file-like object that facilitates reading and writing of binary data split across multiple volumes (files) of a specified size.  Supports random access IO.

Can be used with modules such as tarfile, zipfile, lzma, etc. enabling read/write of split archives.

### Examples
```
import splitfile

with splitfile.open('data.bin', 'wb') as f:
    f.write(b'Hello, World!')
```
```
import splitfile
import tarfile

with splitfile.open('data.bin', 'wb', volume_size=1000000) as f:
    with tarfile.open(mode='w', fileobj=f) as t:
        for file in files:
            t.add(file)
     
Result:
data.bin
data.bin.2
data.bin.3
...
```
### Dependencies

Standard library only

### Installation

pip install splitfile
