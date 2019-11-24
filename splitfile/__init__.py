#!/usr/bin/env python3
# -----------------------------
# splitfile
# -----------------------------
#
"""Python file-like object that facilitates reading and writing of
   binary data split across multiple volumes (files) of a specified
   size.  Supports random access IO.
"""

__version__ = "2.0.1"
__author__ = "github.com/alemigo"

# Imports
from builtins import open as python_open
import os
import io
import math
import shutil

# from splitfile import *
__all__ = ["SplitFile", "open"]


class SplitFile(object):
    """Interface to read and write a stream across multiple volumes"""

    file = None  # Python file object
    file_name = None  # Filename of first volume
    file_index = 0  # Volume index
    file_pos = 0  # Track file position in current volume
    total_pos = 0  # Track file position in total file (all volumes)
    volumes = []  # List of max volume sizes and cumulative total sizes by index
    mode = None  # IO mode: rb, wb, rb+, wb+
    volume_size = 0  # Maximum size of new volumes
    append_to_partial = True  # Append new data to partially used volumes
    buffer = None  # Buffer for uncompressed, unencrypted data
    last_nl = 0  # Position in buffer up to which no \n was found
    first_io = True  # First read or write of stream (first volume)
    last_io = None  # Type of last IO performed (read vs write)
    EOF = False  # End of all volumes reached for read operation
    file_closed = None  # Boolean is file has been closed
    BLOCKSIZE = io.DEFAULT_BUFFER_SIZE  # Size of read blocks

    def __init__(self, filename, mode, volume_size=0, append_to_partial=True):
        """Initialize splitfile object"""

        # validate parameters
        if mode not in ["rb", "wb", "rb+", "wb+"]:
            raise ValueError("Supported modes: rb, wb, rb+ and wb+")

        if volume_size < 0:
            raise ValueError("Volume size must be positive or zero")

        self.file_name = filename
        self.mode = mode
        self.file_index = 0
        self.file_pos = 0
        self.total_pos = 0
        self.first_io = True
        self.last_io = None
        self.last_nl = 0
        self.buffer = b""
        self.EOF = False
        self.file_closed = False
        self.append_to_partial = append_to_partial
        self.volume_size = volume_size

        # if reading, create list of volumes and volume sizes
        # if writing, delete existing volumes
        suff = ""
        i = 1
        total_size = 0
        self.volumes = [{"volume_size": 0, "total_size": 0}]
        while os.path.exists(self.file_name + suff):
            if "rb" in mode:
                file_size = os.stat(self.file_name + suff).st_size
                total_size += file_size
                self.volumes.append(
                    {"volume_size": file_size, "total_size": total_size}
                )
            elif "wb" in mode:
                try:
                    os.remove(self.file_name + suff)
                except IOError:
                    raise IOError(
                        "Unable to overwrite existing volume " + self.file_name + suff
                    )
            i += 1
            suff = "." + str(i)

        # if appending to partially filled volumes, expand volume size of last
        # volume if unused space exists
        if self.append_to_partial and len(self.volumes) > 1:
            self.volumes[-1]["volume_size"] = max(
                self.volumes[-1]["volume_size"], self.volume_size
            )

        # open first file
        if not self._next_file():
            if "rb" in mode:  # rb or rb+
                raise FileNotFoundError("File not found")
            else:  # wb or wb+
                raise OSError("Invalid filename")

    def write(self, data):
        """write method implementation"""
        if self.mode != "wb" and "+" not in self.mode:
            raise io.UnsupportedOperation("Cannot write in read mode")

        if self.closed:
            raise ValueError("I/O operation on closed file.")

        self.last_io = "w"

        if self.first_io:  # first write to first volume
            self.first_io = False

        return self._write(data)  # write to file

    def writelines(self, lines):
        """writelines implementation"""
        for line in lines:
            self.write(line)

    def read(self, size=-1, *, line=False):
        """read method implementation"""
        if self.mode != "rb" and "+" not in self.mode:
            raise io.UnsupportedOperation("Cannot read in write mode")

        if self.closed:
            raise ValueError("I/O operation on closed file.")

        if self.last_io == "w":  # reset read buffer after write operation
            self.buffer = b""
            self.last_nl = 0

        if self.first_io:  # first read from first volume
            self.first_io = False
            self.buffer = b""
            self.last_nl = 0

        if self.EOF:
            return b""
        self.last_io = "r"

        # first check if existing buffer is sufficient
        output = self._read_buffer(size, line)
        if output:
            return self._return_read(output)

        # add data to buffer
        while True:
            data = self._read_file(size)

            if data != b"":
                self.buffer += data

                # try to read from buffer
                output = self._read_buffer(size, line)
                if output:
                    return self._return_read(output)

            else:  # no more to read, flush buffer
                output = self.buffer
                self.buffer = b""
                self.last_nl = 0
                self.EOF = True
                return self._return_read(output)

    def readline(self, size=-1):
        """readline implementation"""
        return self.read(size, line=True)

    def readlines(self, sizehint=0):
        """readlines implementation"""
        output = []
        while True:
            rr = self.readline()
            if rr == b"":
                break
            output.append(rr)

        return output

    def tell(self):
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        return self.total_pos

    def seek(self, offset, whence=0):
        if self.closed:
            raise ValueError("I/O operation on closed file.")

        if whence not in [0, 1, 2]:
            raise ValueError("Invalid whence value")

        if whence == 1:
            offset += self.tell()
        elif whence == 2:
            offset += self.volumes[-1]["total_size"]

        if offset < 0:
            raise ValueError("Negative seek position")
        if offset > self.volumes[-1]["total_size"]:
            self.EOF = True
            self.total_pos = offset
            return offset

        # guess correct volume by proportion of offset to total size
        if self.volumes[-1]["total_size"] == 0:
            i = 1
        else:
            i = min(
                max(
                    math.ceil(
                        offset / self.volumes[-1]["total_size"] * len(self.volumes)
                    ),
                    1,
                ),
                len(self.volumes) - 1,
            )

        while True:
            if offset > self.volumes[i]["total_size"]:
                i += 1
            elif offset < self.volumes[i - 1]["total_size"]:
                i -= 1
            else:
                break

        self._open_volume(i)

        self.file_pos = offset - self.volumes[i - 1]["total_size"]
        self.total_pos = offset

        self.file.seek(self.file_pos)
        self.buffer = b""
        self.last_nl = 0
        self.EOF = True if offset == self.volumes[-1]["total_size"] else False

        return offset

    def truncate(self, size=-1):
        # delete unneeded volumes, truncate last needed volume to size
        if "wb" not in self.mode and "+" not in self.mode:
            raise io.UnsupportedOperation("Cannot truncate in read mode")

        if self.closed:
            raise ValueError("I/O operation on closed file.")

        if size == -1:
            size = self.total_pos

        start_pos = self.tell()
        if size > self.volumes[-1]["total_size"]:  # extend file
            self.seek(0, 2)
            add_size = size - self.volumes[-1]["total_size"]
            num_blocks = int(add_size / self.BLOCKSIZE)
            if num_blocks > 0:
                write_data = b"0" * self.BLOCKSIZE
            for b in range(num_blocks):
                self.write(write_data)

            self.write(b"0" * (add_size - (num_blocks * self.BLOCKSIZE)))
            self.seek(start_pos)

        else:  # reduce size of file
            if self.total_pos != size:
                self.seek(size)
            self.file.truncate(self.file_pos)
            self.volumes[self.file_index]["total_size"] = size

            for v in range(len(self.volumes) - 1, self.file_index, -1):
                suff = "." + str(v) if v > 1 else ""
                if os.path.exists(self.file_name + suff):
                    try:
                        os.remove(self.file_name + suff)
                    except IOError:
                        raise IOError(
                            "Unable to remove volume "
                            + self.file_name
                            + suff
                            + " as part of truncate operation"
                        )
                del self.volumes[v]

            self.seek(start_pos)

    def close(self):
        """close file implementation"""
        if self.file_closed:
            return

        if self.file:
            self.file.close()
            file_name = self._get_file_name()
            if os.path.exists(file_name) and os.stat(file_name).st_size == 0:
                try:
                    os.remove(file_name)
                except IOError:
                    pass

        self.file_closed = True

    def readable(self):
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        if "rb" in self.mode or "+" in self.mode:
            return True
        else:
            return False

    def writable(self):
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        if "wb" in self.mode or "+" in self.mode:
            return True
        else:
            return False

    def seekable(self):
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        if "+" in self.mode:
            return True
        else:
            return False

    def flush(self):
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        self.file.flush()

    @property
    def closed(self):
        return self.file_closed

    @property
    def size(self):
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        return self.volumes[-1]["total_size"]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __iter__(self):
        return self

    def __next__(self):
        r = self.readline()
        if r == b"":
            raise StopIteration
        else:
            return r

    def _next_file(self):
        """Open next volume in sequence"""
        return self._open_volume(self.file_index + 1)

    def _get_file_name(self, index=-1):
        """Return volume filename for a given index"""
        if index == -1:
            index = self.file_index
        file_name = self.file_name
        if index > 1:
            file_name += "." + str(index)
        return file_name

    def _open_volume(self, new_file_index):
        """Close prior volume if open, and open new volume (if possible
           in read mode)
        """
        if self.file_index == new_file_index:
            return True

        new_file_name = self._get_file_name(new_file_index)
        path_exists = os.path.exists(new_file_name)

        if (
            self.last_io == "w"
            or ("wb" in self.mode and self.last_io != "r")
            or path_exists
        ):
            if self.mode == "rb+" and self.last_io == "w" and not path_exists:
                mode = "wb+"  # new volume file
            elif "wb" in self.mode and path_exists:
                mode = "rb+"  # open existing volume file
            else:
                mode = self.mode

            # remove unused volume file
            if not self.first_io and self.file:
                self.file.close()
                old_file_name = self._get_file_name()
                if (
                    os.path.exists(old_file_name)
                    and os.stat(old_file_name).st_size == 0
                ):
                    try:
                        os.remove(old_file_name)
                    except IOError:
                        pass

            if self.last_io == "w":
                self.file_pos = 0
                self.total_pos = self.volumes[new_file_index - 1]["total_size"]

            # for new files, populate volume sizes data
            if not path_exists and self.volume_size > 0:
                vs = {
                    "volume_size": self.volume_size,
                    "total_size": self.volumes[new_file_index - 1]["total_size"],
                }
                self.volumes.insert(new_file_index, vs)

            self.file = python_open(new_file_name, mode)
            self.file_index = new_file_index
            return True
        else:
            return False

    def _write(self, data):
        """write bytes data to file, advance to next volume if needed"""
        if self.volume_size == 0:  # for volume_size =0, do not split to volumes
            self.file_pos += len(data)
            self.total_pos += len(data)
            return self.file.write(data)

        # is position is beyond current size of the file, truncate to extend
        if self.total_pos > self.volumes[-1]["total_size"]:
            self.truncate()

        bytes_written = 0
        while len(data) > 0:
            write_size = 0
            while write_size == 0:
                write_size = min(
                    len(data),
                    self.volumes[self.file_index]["volume_size"] - self.file_pos,
                )
                if write_size > 0:
                    break
                else:
                    self._next_file()

            output = data[:write_size]
            self.file_pos += len(output)
            self.total_pos += len(output)
            bytes_written += self.file.write(output)
            self.volumes[self.file_index]["total_size"] = max(
                self.total_pos, self.volumes[self.file_index]["total_size"]
            )
            data = data[len(output) :]

        return bytes_written

    def _return_read(self, output):
        """advance total and volume specific read position based on read"""
        self.total_pos += len(output)
        i = self.file_index

        while True:
            if self.total_pos > self.volumes[i]["total_size"]:
                i += 1
            elif self.total_pos < self.volumes[i - 1]["total_size"]:
                i -= 1
            else:
                break

        self.file_pos = self.total_pos - self.volumes[i - 1]["total_size"]
        return output

    def _read_file(self, size=-1):
        """read1 block from file, advance to next volume if needed"""
        if size == -1:
            size = self.BLOCKSIZE
        while True:
            data = self.file.read1(size)
            if not data:
                if self._next_file():
                    continue
                else:
                    return b""
            else:
                return data

    def _read_buffer(self, size, line):
        """add new data to buffer, and read if enough data is available"""
        rp = -1
        if line:
            rp = self.buffer.find(b"\n", self.last_nl)
            if rp == -1:
                self.last_nl = len(self.buffer)
            else:
                rp += 1
                self.last_nl = 0

            if size != -1 and len(self.buffer) >= size and size < rp:
                rp = size
        else:
            if size != -1 and len(self.buffer) >= size:
                rp = size

        if rp == -1:
            return None
        else:  # read from beginning of buffer to position RP
            output = self.buffer[:rp]
            self.buffer = self.buffer[rp:]
            return output


def open(filename, mode, volume_size=0, append_to_partial=True):
    """alternative constructor"""
    return SplitFile(filename, mode, volume_size, append_to_partial)
