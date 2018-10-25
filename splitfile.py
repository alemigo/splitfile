#!/usr/bin/env python3
# -----------------------------
# splitfile.py
# -----------------------------
#
"""Python file-like object that facilitates reading and writing of
   binary data split across multiple volumes (files) of a specified
   size. Includes support for compression and encryption.
"""

version = '1.1.0'
__author__ = 'github.com/alemigo'

# Imports
from builtins import open as python_open
import os
import io
import lzma
from Crypto.Cipher import AES

# from splitfile import *
__all__ = ['SplitFile', 'open']


class SplitFile(object):
    """Interface to read and write a stream across multiple volumes"""
    file = None  # Python file object
    file_name = None  # Filename of first volume, or iterable of filenames
    file_index = 0  # Volume index
    file_size = 0  # Track size of current volume
    mode = None  # IO mode: rb, wb
    volume_size = 0  # Maximum size of each volume
    compression = False  # Use LZMA compression: True or False
    lzma_preset = None  # LZMA preset
    lzma_obj = None  # LZMA Compressor/Decompressor object
    aes_key = None  # Encryption key for AES, or None for unencrypted
    buffer = None  # Buffer for uncompressed, unencrypted data
    last_nl = 0  # Position in buffer up to which no \n was found
    cipher = None  # Cipher object used for AES encryption
    first_io = True  # First read or write of stream (first volume)
    EOF = False  # End of all volumes reached for read operation
    BLOCKSIZE = 20 * 512  # Size of read blocks

    def __init__(self, filename, mode, volume_size=0, compression=False,
                 lzma_preset=6, aes_key=None):
        """Initialize splitfile object"""
        self.file_name = filename
        self.mode = mode
        self.file_index = 0
        self.first_io = True
        self.aes_key = aes_key

        if volume_size < 0: raise ValueError(
            'Volume size must be positive or zero')
        self.volume_size = volume_size

        if mode not in ['rb', 'wb']:
            raise ValueError('Supported modes: rb and wb')

        self.compression = compression
        if compression:
            if mode == 'wb':
                self.lzma_obj = lzma.LZMACompressor(preset=lzma_preset)
            else:
                self.lzma_obj = lzma.LZMADecompressor()

        # open first file
        if not self._next_file():
            if mode == 'rb':
                raise FileNotFoundError('File not found')
            else: #wb
                raise OSError('Invalid filename')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __iter__(self):
        return self

    def __next__(self):
        r = self.readline()
        if r == b'':
            raise StopIteration
        else:
            return r

    def _next_file(self):
        """Close prior volume if open, and open next volume (if possible
           in read mode)
        """
        if not self.first_io and self.file:
            self.file.close()
            self.file_size = 0

        self.file_index += 1

        # Get file name for next file:
        # self.file_name may be a string or an iterable
        if isinstance(self.file_name, str):
            if self.file_index == 1:
                next_file_name = self.file_name
            else:
                next_file_name = self.file_name + '.' + str(self.file_index)
        else:
            next_file_name = next(self.file_name)

        # Open the file or return false if in read mode and next file
        # doesn't exist
        if self.mode == 'wb' or os.path.exists(next_file_name):
            self.file = python_open(next_file_name, self.mode)
            return True
        else:
            return False

    def write(self, data):
        """write method implementation"""
        if self.mode != 'wb': raise io.UnsupportedOperation(
            'Cannot write in read mode')

        if self.first_io:  # first write to first volume
            self.first_io = False
            if self.aes_key:  # write encryption header if needed
                self.cipher = AES.new(self.aes_key, AES.MODE_CTR)
                self.file.write(self.cipher.nonce)

        # compress and encrypt if needed
        if self.compression: data = self.lzma_obj.compress(data)
        if self.aes_key: data = self.cipher.encrypt(data)
        self._write(data)  # write to file

    def writelines(self, lines):
        """writelines implementation"""
        for line in lines:
            self.write(line)

    def _write(self, data):
        """write bytes data to file, advance to next volume if needed"""
        if self.volume_size > 0:
            bytes_written = 0
            while len(data) > 0:
                write_size = 0
                while write_size == 0:
                    write_size = min(len(data),
                                     self.volume_size - self.file_size)
                    if write_size > 0:
                        break
                    else:
                        self._next_file()

                output = data[:write_size]
                self.file_size += len(output)
                bytes_written += self.file.write(output)
                data = data[len(output):]

            return bytes_written
        else:  # for volume_size of 0, do not split to volumes
            return self.file.write(data)

    def read(self, size=-1, *, line=False):
        """read method implementation"""
        if self.mode != 'rb':
            raise io.UnsupportedOperation('Cannot read in write mode')

        if self.first_io:  # first read from first volume
            self.first_io = False
            if self.aes_key:  # read encryption header
                nonce = self.file.read(8)
                self.cipher = AES.new(self.aes_key, AES.MODE_CTR, nonce=nonce)

            self.buffer = b''
            self.last_nl = 0

        # EOF check
        if self.EOF: return b''

        # first check if existing buffer is sufficient
        output = self._read_buffer(size, line)
        if output: return output

        while True:
            data = self._read_file()
            if data != b'':
                if self.aes_key: data = self.cipher.decrypt(data)

                if self.compression:
                    decomp = self.lzma_obj.decompress(data)
                    if decomp == b'':
                        continue
                    else:
                        data = decomp

                self.buffer += data

                # try to read from buffer
                output = self._read_buffer(size, line)
                if output: return output
            else:  # no more to read, flush buffer
                output = self.buffer
                self.buffer = b''
                self.last_nl = 0
                self.EOF = True
                return output

    def _read_file(self):
        """read1 block from file, advance to next volume if needed"""
        while True:
            data = self.file.read1(self.BLOCKSIZE)
            if not data:
                if self._next_file():
                    continue
                else:
                    return b''
            else:
                return data

    def _read_buffer(self, size, line):
        """add new data to buffer, and read if enough data is available"""
        rp = -1
        if line:
            rp = self.buffer.find(b'\n', self.last_nl)
            if rp == -1:
                self.last_nl = len(self.buffer)
            else:
                rp += 1
                self.last_nl = 0

            if size != -1 and len(self.buffer) >= size and size < rp: rp = size
        else:
            if size != -1 and len(self.buffer) >= size: rp = size

        if rp == -1:
            return None
        else:  # read from beginning of buffer to position RP
            output = self.buffer[:rp]
            self.buffer = self.buffer[rp:]
            return output

    def readline(self, size=-1):
        """readline implementation"""
        return self.read(size, line=True)

    def readlines(self, sizehint=0):
        """readlines implementation"""
        output = []
        while True:
            rr = self.readline()
            if rr == b'': break
            output.append(rr)

        return output

    def close(self):
        """close file implementation"""
        if self.mode == 'wb' and self.compression and self.file:
            # flush compression
            data = self.lzma_obj.flush()
            if self.aes_key: data = self.cipher.encrypt(data)
            self._write(data)  # write to file

        if self.file: self.file.close()
        self.buffer = b''

    @property
    def closed(self):
        return self.file.closed

    def readable(self):
        return self.file.readable()

    def writable(self):
        return self.file.writable()

    def seekable(self):
        return False

    def flush(self):
        self.file.flush()


def open(filename, mode, volume_size=0, compression=False, lzma_preset=6,
         aes_key=None):
    """alternative constructor"""
    return SplitFile(filename, mode, volume_size, compression, lzma_preset,
                     aes_key)
