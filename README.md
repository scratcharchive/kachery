# kachery

Kachery is a Python package, command-line tool and an optional content-addressable storage server
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
we separate the representation of files from their underlying locations.
This provides many advantages as will be seen.

## Installation

```
pip install --upgrade kachery
```

Or a development installation (after cloning this repo and stepping into the directory):

```
pip install -e .
```

See the documentation below for hosting your own kachery server.

## Command line

Basic usage examples for files:

```bash
#### Store a file in the local database ####
> kachery-store /path/to/file.dat
sha1://ad7fb868e59c495f355d83f61da1c32cc21571cf/file.dat

#### Load it later on ####
> kachery-load sha1://ad7fb868e59c495f355d83f61da1c32cc21571cf/file.dat --dest file.dat
file.dat

#### Or get info about the file ####
> kachery-info sha1://ad7fb868e59c495f355d83f61da1c32cc21571cf/file.dat 
{
    "path": "/tmp/sha1-cache/a/d7/ad7fb868e59c495f355d83f61da1c32cc21571cf",
    "size": 70600000,
    "sha1": "ad7fb868e59c495f355d83f61da1c32cc21571cf"
}

#### You can also pipe the file content ####
> kachery-cat sha1://ad7fb868e59c495f355d83f61da1c32cc21571cf/file.dat > file.dat

#### Or a portion (byte range) thereof ####
> kachery-cat sha1://ad7fb868e59c495f355d83f61da1c32cc21571cf/file.dat --start 0 --end 180000 > file_portion.dat
```

Basic usage examples for directories:

```bash
#### Store a directory ####
> kachery-store /path/to/directory/ds001
sha1dir://4d329a296cfe0b3142d57226ff881b6572c3ed20.ds001

#### List the contents at a later time ####
> kachery-ls sha1dir://4d329a296cfe0b3142d57226ff881b6572c3ed20.ds001
subdir1/
subdir2/
file1.txt
file2.csv

#### List subdirectories ####
> kachery-ls sha1dir://4d329a296cfe0b3142d57226ff881b6572c3ed20.ds001/subdir1
other_file1.txt
other_file2.txt

#### Show the contents of a file ####
> kachery-cat sha1dir://4d329a296cfe0b3142d57226ff881b6572c3ed20.ds001/subdir1/other_file1.txt
Content of other_file1.txt
```

To share files, simply upload them to a remote kachery server using the --upload or --upload-only flag as in the following example:

```bash
#### Store a directory in a remote database ####
# See section below on setting environment variables
> kachery-store --upload /path/to/directory/ds001
sha1dir://4d329a296cfe0b3142d57226ff881b6572c3ed20.ds001

#### Now somebody else on a different computer can retrieve it ####
# See section below on setting environment variables
> kachery-ls --download sha1dir://4d329a296cfe0b3142d57226ff881b6572c3ed20.ds001
subdir1/
subdir2/
file1.txt
file2.csv
```

More detailed documentation on these commands by using the --help flag:

```bash
kachery-store --help
kachery-load --help
kachery-ls --help
kachery-cat --help
kachery-info --help
```

## Python

The above operations are also accessible from Python scripts along with some crucial helper functions.

```python
import kachery as ka
import numpy as np

#### To store text, a dict, a numpy array, a file, or a directory ####

p_text = ka.store_text('some text')
p_obj = ka.store_object(dict(a=['some', 'object', 'that', 'can', 'be', 'json-serialized']))
p_npy = ka.store_npy(np.ones(12, 12))
p_file = ka.store_file('/path/to/file.dat')
p_dir =ka.store_dir('/path/to/some/directory')

# For example p_text will be something like:
# 'sha1://efceddd6e5aa418f965d29e50cf294c08f6a91ec/file.txt'

#### Then later we can retrieve that data using the load functions ####

txt = ka.load_text(p_text)
obj = ka.load_object(p_obj)
array = ka.load_npy(p_npy)
local_path = ka.load_file(p_file)
bytes0 = ka.load_bytes(p_file, start=0, end=180000)
dir_content = ka.read_dir(p_dir)
txt2 = ka.load_text(p_dir + '/some_file_in_the_dir.txt')
```

We can configure kachery to upload and download to and from a remote kachery database using `set_config` as follows:

```python
import kachery as ka
import numpy as np

#### Configure the kachery client to upload/download from a remote kachery database ####

# See section below on setting environment variables
ka.set_config(
    download=True,
    upload=True
)

#### The following will now upload to the remote kachery database ####
p_text = ka.store_text('some text')

#### And later someone on a different computer can retrieve that data ####

txt = ka.load_text(p_text)
```

## Environment variables

Set the following environment variables to determine which kachery server to connect to when using the upload / download capabilities above.

```
KACHERY_URL='http://ip-address-or-host:port'
KACHERY_CHANNEL='name-of-channel'
KACHERY_PASSWORD='password-for-channel'
```

## Hosting a kachery server

To host a kachery server you will need to create a directory
with a kachery.json configuration file inside. For an example
configuration file, see [server/example_kachery.json](server/example_kachery.json). It is possible to configure multiple password-protected channels in order to balance the usage limits for different subsets of users. For example, you may want some subset of users to have download (but not upload) access.

You can either use docker or NodeJS 12.x.x to run the server.
The easiest is to use docker.

For docker instructions, see [server/docker_instructions.txt](server/docker_instructions.txt).

## License

Apache 2.0 - see the LICENSE file

Please acknowledge the authors if you fork this repository or make a derivative
work. I'd prefer if you could collaborate and contribute your improvements back to
this repo.

## Authors

Jeremy Magland, Center for Computational Mathematics, Flatiron Institute

## Help wanted

Seeking co-developers and testers.