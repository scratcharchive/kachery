# kachery

Kachery is a Python package, command-line tool and an optional NodeJS server
that lets you store files and directories in local or remote content-addressable
storage databases. Those files may then be represented by hash URLs. Here are
some examples

```
sha1://25d74fb08ea3e238255ba76efe1cf21c9e8c7db1/some_file.npy
md5://b8355743c06a9944fbd5d1790a894483/another_file.csv
```

You can also represent snapshots of entire directories... for example:

```
sha1dir://615aa23efde8898aa89002613e20ad59dcde42f9.hybrid_janelia/drift_siprobe/rec_16c_1200s_11
```

Those strings are now universal pointers to the underlying file content, which
can be retrieved as part of Python scripts and other applications. In this way
we separate the representation of the files from their underlying locations.
This provides many advantages as will be discussed (somewhere).

## Installation

```
pip install --upgrade kachery
```

Or a development installation (after cloning this repo and stepping into the directory):

```
pip install -e .
```

Separate instructions will be provided in the future for optionally hosting a remote kachery database server (it requires NodeJS 12 and yarn).

## Command line

Basic usage examples for files:

```
** Store a file in the local database **
> kachery-store /path/to/file.dat
sha1://ad7fb868e59c495f355d83f61da1c32cc21571cf/file.dat

** Load it later on **
> kachery-load sha1://ad7fb868e59c495f355d83f61da1c32cc21571cf/file.dat --dest file.dat
file.dat

** Or get info about the file **
> kachery-info sha1://ad7fb868e59c495f355d83f61da1c32cc21571cf/file.dat 
{
    "path": "/tmp/sha1-cache/a/d7/ad7fb868e59c495f355d83f61da1c32cc21571cf",
    "size": 70600000,
    "sha1": "ad7fb868e59c495f355d83f61da1c32cc21571cf"
}
```

Basic usage examples for directories:

```
** Store a directory **
> kachery-store /path/to/directory/ds001
sha1dir://4d329a296cfe0b3142d57226ff881b6572c3ed20.ds001

** List the contents at a later time **
> kachery-ls sha1dir://4d329a296cfe0b3142d57226ff881b6572c3ed20.ds001
subdir1/
subdir2/
file1.txt
file2.csv

** List subdirectories **
> kachery-ls sha1dir://4d329a296cfe0b3142d57226ff881b6572c3ed20.ds001/subdir1
other_file1.txt
other_file2.txt

** Show the contents of a file **
> kachery-cat sha1dir://4d329a296cfe0b3142d57226ff881b6572c3ed20.ds001/subdir1/other_file1.txt
Content of other_file1.txt
```

To share files, simply upload them to a remote kachery server using the --upload or --upload-only flag as in the following example:

```
** Store a directory in a remote database **
> kachery-store --upload /path/to/directory/ds001
sha1dir://4d329a296cfe0b3142d57226ff881b6572c3ed20.ds001

** Now somebody else on a different computer can retrieve it **
> kachery-ls sha1dir://4d329a296cfe0b3142d57226ff881b6572c3ed20.ds001
subdir1/
subdir2/
file1.txt
file2.csv
```

