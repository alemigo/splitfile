# splitfile

Python file-like object that facilitates reading and writing of binary data split across multiple volumes (files) of a specified size.  Supports random access IO.

Can be used with modules such as tarfile, zipfile, lzma, etc. enabling read/write of split archives.

Note: Version 2.0 removes previously integrated encryption and compression options.  The
breadth of choice across both suggests they are better employed as a separate layer,
leaving splitfile to simply manage the final IO across volumes.

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
