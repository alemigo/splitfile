import os
import re
import codecs
import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()


def read(*parts):
    here = os.path.abspath(os.path.dirname(__file__))
    return codecs.open(os.path.join(here, *parts), 'r').read()


def find_version(*file_paths):
    version_file = read(*file_paths)
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)

    raise RuntimeError("Unable to find version string.")


setuptools.setup(
    name="splitfile",
    version=find_version("splitfile","__init__.py"),
    author="github.com/alemigo",
    description="File like object that splits data across volumes",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/alemigo/splitfile",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: OS Independent",
    ],
)