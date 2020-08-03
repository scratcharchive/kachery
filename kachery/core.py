import os
import numpy as np
from typing import Union, Tuple, Optional, List, Dict
import simplejson
import json
import hashlib
import tempfile
import time
import urllib.request as request
import shutil
import io
import sys
import math
from copy import deepcopy
from .filelock import FileLock
from .localhashcache import LocalHashCache
from ._temporarydirectory import TemporaryDirectory
from ._update_config_repos import _update_config_repos

_global_config=dict(
    to=dict(
        url=None,
        channel=None,
        password=None
    ),
    fr=dict(
        url=None,
        channel=None,
        password=None
    ),
    from_remote_only=False,
    to_remote_only=False,
    algorithm='sha1',
    verbose=False,
    use_hard_links=False
)

_global_data: dict=dict(
    server_configs=None
)

_hash_caches = dict(
    sha1=LocalHashCache(algorithm='sha1'),
    md5=LocalHashCache(algorithm='md5')
)

class config:
    def __init__(self, *,
        to: Union[dict, str, None]=None,
        fr: Union[dict, str, None]=None,
        from_remote_only: Union[bool, None]=None,
        to_remote_only: Union[bool, None]=None,
        verbose: Union[bool, None]=None,
        algorithm: Union[str, None]=None,
        use_hard_links: Union[bool, None]=None
    ):
        self._config = dict(
            to=to, fr=fr,
            from_remote_only=from_remote_only,
            to_remote_only=to_remote_only,
            verbose=verbose,
            algorithm=algorithm,
            use_hard_links=use_hard_links
        )
        self._old_config = None
    def __enter__(self):
        self._old_config = deepcopy(get_config())
        set_config(**self._config)
    def __exit__(self, exc_type, exc_val, exc_tb):
        set_config(**self._old_config)

def set_config(*,
        to: Union[dict, str, None]=None,
        fr: Union[dict, str, None]=None,
        from_remote_only: Union[bool, None]=None,
        to_remote_only: Union[bool, None]=None,
        verbose: Union[bool, None]=None,
        algorithm: Union[str, None]=None,
        use_hard_links: Union[bool, None]=None
) -> None:
    if to is not None:
        if isinstance(to, str):
            set_config(to=_get_server_config_for_name(to, write=True))
        else:
            _global_config['to'] = deepcopy(to)
    if fr is not None:
        if isinstance(fr, str):
            set_config(fr=_get_server_config_for_name(fr, write=False))
        else:
            _global_config['fr'] = deepcopy(fr)
    if from_remote_only is not None:
        _global_config['from_remote_only'] = from_remote_only
    if to_remote_only is not None:
        _global_config['to_remote_only'] = to_remote_only
    if verbose is not None:
        _global_config['verbose'] = verbose
    if algorithm is not None:
        _global_config['algorithm'] = algorithm
    if use_hard_links is not None:
        _global_config['use_hard_links'] = use_hard_links

def _get_server_config_for_name(name: str, *, write: bool) -> dict:
    if '~' in name:
        a = name.split('~')
        assert len(a) == 2, f'Invalid name: {name}'
        server_name = a[0]
        channel_name = a[1]
    else:
        server_name = name
        channel_name = None
    server_configs = _load_server_configs()
    try:
        server_config = [c for c in server_configs if c['name'] == server_name][0]
    except:
        raise Exception(f'Unable to find server: {server_name}')
    if channel_name is None:
        if not write:
            channel_name = server_config['default_read_channel']
        else:
            channel_name = server_config['default_write_channel']
    try:
        channel_config = [c for c in server_config['channels'] if c['name'] == channel_name][0]
    except:
        raise Exception(f'Unable to find channel {channel_name} in server {server_name}')
    password = channel_config.get('password', None)
    if password is None:
        server_passwords_fname = _config_dir() + '/server_passwords.json'
        try:
            with open(server_passwords_fname, 'r') as f:
                server_passwords = json.load(f)
        except:
            server_passwords = []
        for r in server_passwords:
            if r['server_name'] == server_name and r['channel_name'] == channel_name:
                password = r['password']
    if password is None:
        raise Exception(f'No password found for server.channel: {server_name}.{channel_name}')
    return dict(
        url=server_config['url'],
        channel=channel_name,
        password=password
    )

def get_config() -> dict:
    return _load_config()

def _config_dir():
    homedir = os.path.expanduser("~")
    if not os.path.exists(homedir):
        # this is not the best way to handle it
        # but i'm trying to solve a tricky case where
        # the resolved home directory does not exist inside
        # a container :(
        if 'KACHERY_STORAGE_DIR' in os.environ:
            homedir = os.environ['KACHERY_STORAGE_DIR']
        else:
            # last resort
            homedir = tempfile.gettempdir()
    ret = os.path.join(homedir, '.kachery')
    if not os.path.exists(ret):
        os.mkdir(ret)
    return ret

def _load_server_configs():
    if _global_data['server_configs'] is not None:
        return _global_data['server_configs']
    else:
        config_repos_path = os.path.join(_config_dir(), 'config_repos')
        x = _update_config_repos(config_repos_path)
        server_configs = x['servers']
        _global_data['server_configs'] = server_configs
    return _global_data['server_configs']

def _load_config(**kwargs) -> dict:
    if 'config' in kwargs:
        ret = kwargs['config']
    else:
        ret = deepcopy(_global_config)
    for key, val in kwargs.items():
        if key in ['to', 'fr'] and (val == None or val == ''):
            ret[key] = dict(url=None, channel=None, password=None)
        elif key == 'to':
            if isinstance(val, str):
                ret[key] = _get_server_config_for_name(val, write=True)
            else:
                ret[key] = val
        elif key == 'fr':
            if isinstance(val, str):
                ret[key] = _get_server_config_for_name(val, write=False)
            else:
                ret[key] = val
        else:
            ret[key] = val
    return ret

def _is_hash_url(path):
    algs = ['sha1', 'md5']
    for alg in algs:
        if path.startswith(alg + '://') or path.startswith(alg + 'dir://'):
            return True
    return False

def load_file(path: str, dest: str=None, **kwargs)-> Union[str, None]:
    config = _load_config(**kwargs)
    fr = config['fr']
    if _is_hash_url(path):
        if not config['from_remote_only']:
            ret, _, _ = _find_file_locally(path, config=config)
            if ret:
                if dest:
                    shutil.copyfile(ret, dest)
                    return dest
                return ret
        if fr['url'] is not None:
            url0, algorithm, hash0, size0 = _check_remote_file(path, config=config)
            if size0 == 0:
                # This is an empty file, we handle it differently because the server has trouble
                fname_empty = load_file(str(store_text('', algorithm=str(algorithm), to='')), fr='')
                if fname_empty is None:
                    raise Exception('Unexpected fname_empty is None')
                if dest:
                    shutil.copyfile(str(fname_empty), dest)
                    return dest
                return fname_empty

            if url0:
                assert algorithm is not None
                assert hash0 is not None
                assert size0 is not None
                return _hash_caches[algorithm].downloadFile(url=url0, hash=hash0, size=size0, target_path=dest)
            else:
                return None
        return None
    else:
        if os.path.isfile(path):
            if dest:
                shutil.copyfile(path, dest)
                return dest
            return path
        else:
            return None
    
def load_text(path: str, **kwargs) -> Union[str, None]:
    path2 = load_file(path, **kwargs)
    if not path2:
        return None
    with open(path2, 'r') as f:
        return f.read()

def load_object(path: str, **kwargs) -> Union[dict, None]:
    path2 = load_file(path, **kwargs)
    if not path2:
        return None
    with open(path2, 'r') as f:
        return simplejson.load(f)

def load_npy(path: str, **kwargs) -> Union[np.ndarray, None]:
    path2 = load_file(path, **kwargs)
    if not path2:
        return None
    return np.load(path2)

def load_bytes(path: str, start: Union[int, None]=None, end: Union[int, None]=None, write_to_stdout=False, **kwargs) -> Union[bytes, None]:
    config = _load_config(**kwargs)
    fr = config['fr']
    if start is None and end is None:
        local_fname = load_file(path=path, config=config)
        if local_fname:
            return _load_bytes_from_local_file(local_fname, config=config, write_to_stdout=write_to_stdout)
    if _is_hash_url(path):
        if not config['from_remote_only']:
            local_fname, _, _ = _find_file_locally(path, config=config)
            if local_fname:
                return _load_bytes_from_local_file(local_fname, start=start, end=end, write_to_stdout=write_to_stdout, config=config)
        if fr['url'] is not None:
            if start is None and end is None:
                path_local = load_file(path, config=config)
                if path_local is None:
                    return None
                return load_bytes(path_local, start=start, end=end, write_to_stdout=write_to_stdout, config=config)
            if start is None:
                start = 0
            if end is None:
                raise Exception('This case not handled.')
            with open_file(path, config=config) as f:
                f.seek(start)
                return f.read(end - start)
            # url0, algorithm0, hash0, size0 = _check_remote_file(path, config=config)
            # if size0 == 0:
            #     # This is an empty file, we handle it differently because the server has trouble
            #     return bytes([])
            # if url0:
            #     assert algorithm0 is not None
            #     assert hash0 is not None
            #     assert size0 is not None
            #     code: Dict[str, int]=dict(
            #         start=start,
            #         end=end
            #     )
            #     code[algorithm0] = hash0
            #     code_hash = _sha1_of_object(code)
            #     hc = _hash_caches[algorithm0]
            #     path0 = hc.find_file_by_code(code=code_hash)
            #     if path0:
            #         return load_bytes(path0, start=None, end=None, write_to_stdout=write_to_stdout, **kwargs)
            #     bytes0 = _load_bytes_from_remote_file(url=url0, size=size0, start=start, end=end, config=config, write_to_stdout=write_to_stdout)
            #     if bytes0 is not None:
            #         hc.store_file_by_code(code=code_hash, data=bytes0)
            #     return bytes0
            # else:
            #     return None
        return None
    else:
        if os.path.isfile(path):
            return _load_bytes_from_local_file(path, start=start, end=end, config=config, write_to_stdout=write_to_stdout)
        else:
            return None

def _load_remote_file_block(path: str, *, url: str, size: int, config: dict, start: int, end: int):
    if _is_hash_url(path):
        hash0, algorithm0 = _determine_file_hash_from_url(url=path, config=config)
    else:
        hash0 = _compute_local_file_hash(path, config=config, algorithm='sha1')
        algorithm0 = 'sha1'
    if hash0 is None:
        raise Exception('Unable to compute hash of file: {}'.format(path))
    assert algorithm0 is not None
    code: Dict[str, Union[str, int]]=dict(
        start=start,
        end=end
    )
    code[algorithm0] = hash0
    code_hash = _sha1_of_object(code)
    hc = _hash_caches[algorithm0]
    path0 = hc.find_file_by_code(code=code_hash)
    if path0:
        return path0
    bytes0 = _load_bytes_from_remote_file(url=url, config=config, size=size, start=start, end=end)
    if not bytes0:
        return None
    return hc.store_file_by_code(code=code_hash, data=bytes0)
    


def open_file(path: str, block_size=10 * 1024 * 1024, **kwargs):
    config = _load_config(**kwargs)
    verbose = config['verbose']
    info = get_file_info(path, **kwargs)
    if info is None:
        raise Exception('Unable to find file: {}'.format(path))
    if 'path' in info:
        if verbose:
            print('opening from path', info['path'])
        return open(info['path'], 'rb')
    elif 'url' in info:
        if verbose:
            print('opening from url', info['url'])
        return _RemoteFile(path, url=info['url'], size=info['size'], block_size=block_size, config=config)
    else:
        raise Exception('Unexpected info')

def load_dir(path: str, dest: str, **kwargs)-> None:
    print('Loading directory {} -> {}'.format(path, dest))
    if os.path.exists(dest):
        raise Exception('Destination directory already exists: {}'.format(dest))
    os.mkdir(dest)
    config = _load_config(**kwargs)
    dd = read_dir(path=path, recursive=False, config=config)
    for filename in dd['files']:
        load_file(path + '/' + filename, dest=os.path.join(dest, filename), config=config)
    for dirname in dd['dirs']:
        load_dir(path + '/' + dirname, dest=os.path.join(dest, dirname), config=config)

class _RemoteFile:
    def __init__(self, path: str, *, url: str, size: int, block_size: int, config: dict):
        self._path = path
        self._url = url
        self._size = size
        self._block_size = block_size
        self._config = config
        self._current_pos = 0
        self._block_paths: Dict[str] = dict()
        self._current_block_num = None
        self._current_block_file = None
    def __enter__(self):
        return self
    def __exit__(self, type, value: object, traceback) -> None:
        if self._current_block_file is not None:
            self._current_block_file.close()
    def seek(self, offset):
        self._current_pos = offset
    def read(self, size):
        p1 = self._current_pos
        p2 = self._current_pos + size
        return self._read(p1, p2)
    def _read(self, p1, p2):
        b_start = math.floor(p1 / self._block_size)
        b_end = math.floor((p2 - 1) / self._block_size)
        if b_start == b_end:
            self._load_block_file(b_start)
            f = self._current_block_file
            f.seek(p1 - b_start * self._block_size)
            return f.read(p2 - p1)
        else:
            buffers = []
            buffers.append(self._read(p1, (b_start + 1) * self._block_size ))
            for bb in range(b_start + 1, b_end):
                buffers.append(self._read(bb * self._block_size, (bb + 1) * self._block_size))
            buffers.append(self._read(b_end * self._block_size, p2))
            return b''.join(buffers)
    def _get_block_path(self, block_num) -> Union[str, None]:
        if block_num == 0 and self._block_size >= self._size:
            # in this case we are loading the entire file
            # we need to put load_from='remote' in case the config has remote_only (a bit tricky)
            return load_file(path=self._path, config=self._config, from_remote_only=False)
        if block_num not in self._block_paths:
            self._block_paths[block_num] = _load_remote_file_block(path=self._path, url=self._url, size=self._size, config=self._config, start=block_num * self._block_size, end=min((block_num + 1) * self._block_size, self._size))
        return self._block_paths[block_num]
    def _load_block_file(self, block_num):
        if self._current_block_num == block_num:
            return
        block_path = self._get_block_path(block_num)
        assert block_path is not None
        if self._current_block_file is not None:
            self._current_block_file.close()
        self._current_block_file = open(block_path, 'rb')
        self._current_block_num = block_num

def get_object_hash(obj: dict):
    return _sha1_of_object(obj)

def get_file_hash(path: str):
    info = get_file_info(path, algorithm='sha1')
    return info['sha1']

def get_file_info(path: str, **kwargs) -> Union[dict, None]:
    config = _load_config(**kwargs)
    fr = config['fr']
    if _is_hash_url(path):
        if not config['from_remote_only']:
            fname, hash1, algorithm1 = _find_file_locally(path, config=config)
            if fname:
                assert hash1 is not None
                assert algorithm1 is not None
                ret = dict(
                    path=fname,
                    size=os.path.getsize(fname)
                )
                ret[algorithm1] = hash1
                return ret
        if fr['url'] is not None:
            url0, algorithm, hash0, size0 = _check_remote_file(path, config=config)
            if size0 == 0:
                # This is an empty file, we handle it differently because the server has trouble
                return dict(
                    url='empty-file',
                    size=0
                )
            if url0:
                assert algorithm is not None
                assert hash0 is not None
                assert size0 is not None
                ret = dict(
                    url=url0,
                    size=size0
                )
                ret[algorithm] = hash0
                return ret
            else:
                return None
        return None
    else:
        if os.path.isfile(path):
            ret: Dict[str, Union[str, int, None]] = dict(
                path=path,
                size=os.path.getsize(path)
            )
            ret[config['algorithm']] = _compute_local_file_hash(path, algorithm=config['algorithm'], config=config)
            return ret
        else:
            return None

def store_file(path: str, basename: Union[str, None]=None, git_annex_mode: bool=False, _no_manifest: bool=False, **kwargs) -> Union[str, None]:
    if basename is None:
        basename = os.path.basename(path)
    if _is_hash_url(path):
        path2 = load_file(path, **kwargs)
        if path2 is None:
            raise Exception('Unable to load file (in store_file): {}'.format(path))
        path = str(path2)
    config = _load_config(**kwargs)
    to = config['to']
    algorithm = config['algorithm']
    if algorithm == 'sha1' and (not _no_manifest) and (os.path.getsize(path) > 4000000):
        hash0, manifest0 = _compute_local_file_sha1_and_manifest(path)
    else:
        hash0 = _compute_local_file_hash(path, algorithm=algorithm, config=config)
        manifest0 = None
        
    if not hash0:
        raise Exception('Unable to compute {} hash of file: {}'.format(algorithm, path))
    if not config['to_remote_only']:
        _store_local_file_in_cache(path, algorithm=algorithm, hash=hash0, config=config)
    if (to['url'] is not None) and (not git_annex_mode):
        _upload_local_file(path, algorithm=algorithm, hash=hash0, config=config)
    if manifest0 is None:
        return '{}://{}/{}'.format(algorithm, hash0, basename)
    else:
        manifest_uri = store_object(manifest0, _no_manifest=True)
        assert manifest_uri is not None
        manifest_sha1 = get_file_hash(manifest_uri)
        return '{}://{}/{}?manifest={}'.format(algorithm, hash0, basename, manifest_sha1)
    
def _compute_manifest_of_buf(data):
    algorithm = 'sha1'
    manifest = {
        'size': 0,
        'sha1': '',
        'chunks': []
    }
    size0 = len(data)
    chunk_size = 10000000
    while True:
        num_chunks = math.ceil(size0 / chunk_size)
        if num_chunks > 100:
            chunk_size = chunk_size * 2
        elif num_chunks < 10:
            chunk_size = math.ceil(chunk_size / 2)
        else:
            break
    hashsum = getattr(hashlib, algorithm)()
    pos = 0
    while pos < size0:
        
        this_chunk_size = min(chunk_size, size0 - pos)

        this_chunk_hashsum = getattr(hashlib, algorithm)()
        buf = data[pos:pos + this_chunk_size]
        this_chunk_hashsum.update(buf)
        
        hashsum.update(buf)
        
        chunk = {
            'start': pos,
            'end': pos + this_chunk_size,
            'sha1': this_chunk_hashsum.hexdigest()
        }
        if (pos == 0) and (this_chunk_size > 10000000):
            chunk['manifest'] = _compute_manifest_of_buf(buf)
            store_object(chunk['manifest'], _no_manifest=True)
        manifest['chunks'].append(chunk)
        
        pos = pos + this_chunk_size
            
    sha1 = hashsum.hexdigest()
    manifest['sha1'] = sha1
    manifest['size'] = size0
    return manifest

def _compute_local_file_sha1_and_manifest(path):
    algorithm = 'sha1'
    manifest = {
        'size': 0,
        'sha1': '',
        'chunks': []
    }
    if not os.path.exists(path):
        return None, None
    size0 = os.path.getsize(path)
    if (size0 > 1024 * 1024 * 100):
        print('Computing {} and manifest of {}'.format(algorithm, path))
    chunk_size = 10000000
    while True:
        if chunk_size <= 4000000:
            break
        num_chunks = math.ceil(size0 / chunk_size)
        if num_chunks > 100:
            chunk_size = chunk_size * 2
        elif num_chunks < 10:
            chunk_size = math.ceil(chunk_size / 2)
        else:
            break
    hashsum = getattr(hashlib, algorithm)()
    with open(path, 'rb') as file:
        pos = 0
        while pos < size0:
            
            this_chunk_size = min(chunk_size, size0 - pos)

            this_chunk_hashsum = getattr(hashlib, algorithm)()
            buf = file.read(this_chunk_size)
            this_chunk_hashsum.update(buf)
            
            hashsum.update(buf)
            
            chunk = {
                'start': pos,
                'end': pos + this_chunk_size,
                'sha1': this_chunk_hashsum.hexdigest()
            }
            if (pos == 0) and (this_chunk_size > 10000000):
                chunk['manifest'] = _compute_manifest_of_buf(buf)
                store_object(chunk['manifest'], _no_manifest=True)
            manifest['chunks'].append(chunk)
            
            pos = pos + this_chunk_size
            
    sha1 = hashsum.hexdigest()
    manifest['sha1'] = sha1
    manifest['size'] = size0
    return sha1, manifest

    
def store_text(text: str, basename: Union[str, None]=None, **kwargs) -> Union[str, None]:
    if basename is None:
        basename = 'file.txt'
    with TemporaryDirectory() as tmpdir:
        fname = tmpdir + '/text.txt'
        with open(fname, 'w') as f:
            f.write(text)
        return store_file(fname, basename=basename, **kwargs)

def store_object(object: dict, basename: Union[str, None]=None, indent: Union[int, None]=None, **kwargs) -> Union[str, None]:
    if basename is None:
        basename = 'file.json'
    txt = simplejson.dumps(object, indent=indent)
    return store_text(text=txt, basename=basename, **kwargs)

def store_npy(array: np.ndarray, basename: Union[str, None]=None, **kwargs) -> Union[str, None]:
    if basename is None:
        basename = 'file.npy'
    with TemporaryDirectory() as tmpdir:
        fname = tmpdir + '/array.npy'
        np.save(fname, array)
        return store_file(fname, basename=basename, **kwargs)

def store_dir(dirpath: str, label: Union[str, None]=None, git_annex_mode: bool=False, **kwargs):
    config = _load_config(**kwargs)
    if label is None:
        label = os.path.basename(dirpath)
    # X = _read_file_system_dir(dirpath, recursive=True, include_hashes=True, store_files=True, git_annex_mode=git_annex_mode, config=config)
    X = read_dir(dirpath, recursive=True, git_annex_mode=git_annex_mode, store_files=True, config=config)
    if not X:
        return None
    path1 = store_object(X, config=config)
    assert path1 is not None
    hash0, algorithm = _determine_file_hash_from_url(url=path1, config=config)
    return '{}dir://{}.{}'.format(algorithm, hash0, label)

def read_dir(path: str, *, recursive: bool=True, git_annex_mode: bool=False, store_files: bool=False, **kwargs):
    config = _load_config(**kwargs)
    if _is_hash_url(path):
        protocol, algorithm, hash0, additional_path = _parse_kachery_url(path)
        if not protocol.endswith('dir'):
            raise Exception('Not a directory: {}'.format(path))
        dd = load_object('{}://{}'.format(algorithm, hash0), config=config)
        if dd is None:
            return None
        if additional_path:
            list0 = additional_path.split('/')
        else:
            list0 = []
        ii = 0
        while ii < len(list0):
            assert dd is not None
            name0 = list0[ii]
            if name0 in dd['dirs']:
                dd = dd['dirs'][name0]
            elif name0 in dd['files']:
                raise Exception('Not a directory: {}'.format(path))
            else:
                return None
            ii = ii + 1
        if dd:
            if not recursive:
                for dname in dd['dirs']:
                    dd['dirs'][dname] = {}
        return dd
    else:
        return _read_file_system_dir(path, recursive=recursive, include_hashes=True, store_files=store_files, git_annex_mode=git_annex_mode, config=config)

def _compute_local_file_hash(path: str, *, algorithm: str, config: dict) -> Union[str, None]:
    return _hash_caches[algorithm].computeFileHash(path)

def _store_local_file_in_cache(path: str, *, hash: str, algorithm: str, config: dict) -> None:
    _, hash2 = _hash_caches[algorithm].copyFileToCache(path, use_hard_links=config['use_hard_links'], _known_hash=hash)
    if hash2 is None:
        raise Exception('Unable to store local file in cache: {}'.format(path))

def _find_file_locally(path: str, *, config: dict) -> Tuple[Union[str, None], Union[str, None], Union[str, None]]:
    if _is_hash_url(path):
        hash0, algorithm = _determine_file_hash_from_url(path, config=config)
        if not hash0:
            return None, None, None
        assert algorithm is not None
        ret = _hash_caches[algorithm].findFile(hash=hash0)
        if ret is not None:
            return ret, hash0, algorithm
        else:
            return None, None, None
    elif os.path.isfile(path):
        hash1 = _compute_local_file_hash(path, algorithm=config['algorithm'], config=config)
        return path, hash1, config['algorithm']
    else:
        return None, None, None

def _check_remote_file(hash_url: str, *, config: dict) -> Tuple[Union[str, None], Union[str, None], Union[str, None], Union[int, None]]:
    if _is_hash_url(hash_url):
        hash0, algorithm = _determine_file_hash_from_url(hash_url, config=config)
        if hash0 is None:
            return None, None, None, None
        assert algorithm is not None
        if hash0 == _hash_of_string('', algorithm=algorithm):
            # This is an empty file, we handle it differently because the server has trouble
            return None, algorithm, hash0, 0
            
        url_check: str = _form_check_url(hash=hash0, algorithm=algorithm, config=config)
        check_resp: dict = _http_get_json(url_check, use_cache_on_found=True)
        if not check_resp['success']:
            print('Warning: Problem checking for file: ' + check_resp['error'])
            return None, None, None, None
        if check_resp['found']:
            url_download = _form_download_url(hash=hash0, algorithm=algorithm, config=config)
            size = check_resp['size']
            return url_download, algorithm, hash0, size
        else:
            return None, None, None, None
    else:
        raise Exception('Unexpected')

def _get_password(x):
    if type(x) == str:
        return x
    elif type(x) == dict:
        if 'env' in x:
            env0 = x['env']
            if env0 in os.environ:
                return os.environ[env0]
            else:
                raise Exception('You need to set the {} environment variable'.format(env0))
        else:
            raise Exception('Unexpected password config')

def _form_download_url(*, algorithm: str, hash: str, config: dict) -> str:
    fr = config['fr']
    url = fr['url']
    channel = fr['channel']
    signature = _sha1_of_object(dict(
        algorithm=algorithm,
        hash=hash,
        name='download',
        password=_get_password(fr['password']),
    ))
    return '{}/get/{}/{}?channel={}&signature={}'.format(url, algorithm, hash, channel, signature)

def _form_check_url(*, algorithm: str, hash: str, config: dict) -> str:
    fr = config['fr']
    url = fr['url']
    channel = fr['channel']
    signature = _sha1_of_object(dict(
        algorithm=algorithm,
        hash=hash,
        name='check',
        password=_get_password(fr['password'])
    ))
    return '{}/check/{}/{}?channel={}&signature={}'.format(url, algorithm, hash, channel, signature)

def _form_upload_url(*, algorithm: str, hash: str, config: dict) -> str:
    to = config['to']
    url = to['url']
    channel = to['channel']
    signature = _sha1_of_object(dict(
        algorithm=algorithm,
        hash=hash,
        name='upload',
        password=_get_password(to['password'])
    ))
    return '{}/set/{}/{}?channel={}&signature={}'.format(url, algorithm, hash, channel, signature)

def _upload_local_file(path: str, *, hash: str, algorithm: str, config: dict) -> None:
    size0 = os.path.getsize(path)
    if size0 == 0:
        # don't upload an empty file. The server cannot handle it - and we'll just take care of this case separately
        return

    config2 = deepcopy(config)
    config2['fr'] = config2['to']
    url_ch, _, _, size_ch = _check_remote_file('{}://{}'.format(algorithm, hash), config=config2)
    if url_ch is not None:
        # already on the remote server
        if size_ch != size0:
            raise Exception('Unexpected: size of file on remote server does not match local file {} - {} <> {}'.format(path, size_ch, size0))
        return

    url0 = _form_upload_url(algorithm=algorithm, hash=hash, config=config)
    if size0 > 10000:
        print('Uploading to kachery --- ({}): {} -> {}'.format(_format_file_size(size0), path, url0))

    timer = time.time()
    resp_obj = _http_post_file_data(url0, path)
    elapsed = time.time() - timer

    if size0 > 10000:
        print('File uploaded ({}) in {} sec'.format(
            _format_file_size(size0), elapsed))

    if not resp_obj.get('success', False):
        raise Exception('Problem posting file data: ' + resp_obj.get('error', ''))

def _determine_file_hash_from_url(url: str, *, config: dict) -> Tuple[Union[str, None], Union[str, None]]:
    protocol, algorithm, hash0, additional_path = _parse_kachery_url(url)
    if not protocol.endswith('dir'):
        return hash0, algorithm
    dd = load_object('{}://{}'.format(algorithm, hash0))
    if dd is None:
        return None, None
    if additional_path:
        list0 = additional_path.split('/')
    else:
        list0 = []
    ii = 0
    while ii < len(list0):
        assert dd is not None
        name0 = list0[ii]
        if name0 in dd['dirs']:
            dd = dd['dirs'][name0]
        elif name0 in dd['files']:
            if ii + 1 == len(list0):
                hash1 = None
                algorithm1 = None
                for alg in ['sha1', 'md5']:
                    if alg in dd['files'][name0]:
                        hash1 = dd['files'][name0][alg]
                        algorithm1 = alg
                return hash1, algorithm1
            else:
                return None, None
        else:
            return None, None
        ii = ii + 1
    return None, None

def _parse_kachery_url(url: str) -> Tuple[str, str, str, str]:
    listA = url.split('?')
    list0 = listA[0].split('/')
    protocol = list0[0].replace(':', '')
    hash0 = list0[2]
    if '.' in hash0:
        hash0 = hash0.split('.')[0]
    additional_path = '/'.join(list0[3:])
    algorithm = None
    for alg in ['sha1', 'md5', 'key']:
        if protocol.startswith(alg):
            algorithm = alg
    if algorithm is None:
        raise Exception('Unexpected protocol: {}'.format(protocol))
    return protocol, algorithm, hash0, additional_path

def _sha1_of_string(txt: str) -> str:
    hh = hashlib.sha1(txt.encode('utf-8'))
    ret = hh.hexdigest()
    return ret

def _hash_of_string(txt: str, *, algorithm) -> str:
    hh1 = getattr(hashlib, algorithm)
    hh2 = hh1(txt.encode('utf-8'))
    ret = hh2.hexdigest()
    return ret


def _sha1_of_object(obj: object) -> str:
    txt = json.dumps(obj, sort_keys=True, separators=(',', ':'))
    return _sha1_of_string(txt)

def _http_get_json(url: str, use_cache_on_found: bool=False, verbose: Optional[bool]=False, retry_delays: Optional[List[float]]=None) -> dict:
    if use_cache_on_found:
        cache = getattr(_http_get_json, 'cache')
        if url in cache:
            return cache[url]
    timer = time.time()
    if retry_delays is None:
        retry_delays = [0.2, 0.5]
    if verbose is None:
        verbose = (os.environ.get('HTTP_VERBOSE', '') == 'TRUE')
    if verbose:
        print('_http_get_json::: ' + url)
    try:
        req = request.urlopen(url)
    except:
        if len(retry_delays) > 0:
            print('Retrying http request in {} sec: {}'.format(
                retry_delays[0], url))
            time.sleep(retry_delays[0])
            return _http_get_json(url, verbose=verbose, retry_delays=retry_delays[1:])
        else:
            return dict(success=False, error='Unable to open url: ' + url)
    try:
        ret = json.load(req)
    except:
        return dict(success=False, error='Unable to load json from url: ' + url)
    if verbose:
        print('Elapsed time for _http_get_json: {} {}'.format(time.time() - timer, url))
    elif use_cache_on_found:
        if ret['success'] and ret['found']:
            cache = getattr(_http_get_json, 'cache')
            cache[url] = ret
    return ret

def reset():
    setattr(_http_get_json, 'cache', dict())

reset()

# thanks: https://stackoverflow.com/questions/1094841/reusable-library-to-get-human-readable-version-of-file-size
def _format_file_size(size: Optional[int]) -> str:
    if not size:
        return 'Unknown'
    if size <= 1024:
        return '{} B'.format(size)
    return _sizeof_fmt(size)


def _sizeof_fmt(num: float, suffix: str='B') -> str:
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f %s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f %s%s" % (num, 'Yi', suffix)

def _http_post_file_data(url: str, fname: str, verbose: Optional[bool]=None) -> dict:
    timer = time.time()
    if verbose is None:
        verbose = (os.environ.get('HTTP_VERBOSE', '') == 'TRUE')
    if verbose:
        print('_http_post_file_data::: ' + fname)
    with open(fname, 'rb') as f:
        try:
            import requests
        except:
            raise Exception('Error importing requests')
        try:
            req = requests.post(url, data=f)
        except:
            raise Exception('Error posting file data.')
    if req.status_code != 200:
        return dict(
            success=False,
            error='Error posting file data: {} {}'.format(req.status_code, req.content.decode('utf-8'))
        )
    if verbose:
        print('Elapsed time for _http_post_file_Data: {}'.format(time.time() - timer))
    return json.loads(req.content)

def _http_post_json(url: str, data: dict, verbose: Optional[bool]=None) -> dict:
    timer = time.time()
    if verbose is None:
        verbose = (os.environ.get('HTTP_VERBOSE', '') == 'TRUE')
    if verbose:
        print('_http_post_json::: ' + url)
    try:
        import requests
    except:
        raise Exception('Error importing requests *')
    req = requests.post(url, json=data)
    if req.status_code != 200:
        return dict(
            success=False,
            error='Error posting json: {} {}'.format(req.status_code, req.content.decode('utf-8'))
        )
    if verbose:
        print('Elapsed time for _http_post_json: {}'.format(time.time() - timer))
    return json.loads(req.content)

def _read_file_system_dir(path: str, *, recursive: bool, include_hashes: bool, store_files: bool, git_annex_mode: bool, config: dict) -> Union[dict, None]:
    ret: dict = dict(
        files={},
        dirs={}
    )
    algorithm = config['algorithm']
    list0 = os.listdir(path)
    for name0 in list0:
        path0 = path + '/' + name0
        if git_annex_mode and os.path.islink(path0) and ('.git/annex/objects' in os.path.realpath(path0)):
            hash1, algorithm1, size1 = _get_info_from_git_annex_link(path0)
            ret['files'][name0] = dict(
                size=size1
            )
            ret['files'][name0][algorithm1] = hash1
        elif os.path.isfile(path0):
            ret['files'][name0] = dict(
                size=os.path.getsize(path0)
            )
            if include_hashes:
                hash1b = _compute_local_file_hash(path0, algorithm=algorithm, config=config)
                ret['files'][name0][algorithm] = hash1b
            if store_files:
                store_file(path0, git_annex_mode=git_annex_mode, config=config)
        elif os.path.isdir(path0):
            include = True
            if git_annex_mode:
                if name0 in ['.git', '.datalad']:
                    include = False
            if include:
                ret['dirs'][name0] = {}
                if recursive:
                    ret['dirs'][name0] = _read_file_system_dir(
                        path=path0, recursive=recursive, include_hashes=include_hashes, store_files=store_files, git_annex_mode=git_annex_mode, config=config)
    return ret

def _get_info_from_git_annex_link(path) -> Tuple[str, str, int]:
    path1 = os.path.realpath(path)
    # Example: /home/magland/data/najafi-2018-nwb/.git/annex/objects/Gx/pw/MD5E-s167484154--c8bc43bb1868301737797b09266c01a1.mat/MD5E-s167484154--c8bc43bb1868301737797b09266c01a1.mat
    str1 = path1.split('/')[-1]
    # Example: MD5E-s167484154--c8bc43bb1868301737797b09266c01a1.mat
    size0 = int(str1.split('-')[1][1:])
    if str1.split('-')[0] == 'MD5E':
        algorithm0='md5'
    else:
        raise Exception('Unexpected string in _get_info_from_git_annex_link:' + path1)
    hash0 = str1.split('-')[3].split('.')[0]
    return hash0, algorithm0, size0

def _load_bytes_from_local_file(local_fname: str, *, config: dict, start: Union[int, None]=None, end: Union[int, None]=None, write_to_stdout: bool=False) -> Union[bytes, None]:
    size0 = os.path.getsize(local_fname)
    if start is None:
        start = 0
    if end is None:
        end = size0
    if start < 0 or start > size0 or end < start or end > size0:
        raise Exception('Invalid start/end range for file of size {}: {} - {}'.format(size0, start, end))
    if start == end:
        return bytes()
    with open(local_fname, 'rb') as f:
        f.seek(start)
        if write_to_stdout:
            ii = start
            while ii < end:
                nn = min(end - ii, 4096)
                data0 = f.read(nn)
                ii = ii + nn
                sys.stdout.buffer.write(data0)
            return None
        else:
            return f.read(end-start)

def _load_bytes_from_remote_file(*, url: str, config: dict, size: int, start: Union[int, None]=None, end: Union[int, None]=None, write_to_stdout: bool=False) -> Union[bytes, None]:
    if start is None:
        start = 0
    if end is None:
        end = size
    if start < 0 or start > size or end < start or end > size:
        raise Exception('Invalid start/end range for file of size {}: {} - {}'.format(size, start, end))
    if start == end:
        return bytes()
    headers = {
        'Range': 'bytes={}-{}'.format(start, end-1)
    }
    bb = io.BytesIO()
    timer = time.time()
    if (end - start > 10000) and (not write_to_stdout):
        print('Downloading {} of {} (bytes {}-{})'.format(_format_file_size(end - start), url, start, end))
    try:
        import requests
    except:
        raise Exception('Error importing requests **')
    response = requests.get(url, headers=headers, stream=True)
    for chunk in response.iter_content(chunk_size=5120):
        if chunk:  # filter out keep-alive new chunks
            if write_to_stdout:
                sys.stdout.buffer.write(chunk)
            else:
                bb.write(chunk)
    elapsed = time.time() - timer
    if (end - start > 10000) and (not write_to_stdout):
        print('Downloaded {} in {} sec from {}'.format(_format_file_size(end - start), elapsed, url))

    if write_to_stdout:
        return None
    else:
        return bb.getvalue()

def _read_json_file(path: str, *, delete_on_error: bool=False) -> Union[dict, None]:
    with FileLock(path + '.lock', exclusive=False):
        try:
            with open(path) as f:
                return json.load(f)
        except:
            if delete_on_error:
                print('Warning: Unable to read or parse json file. Deleting: ' + path)
                try:
                    os.unlink(path)
                except:
                    print('Warning: unable to delete file: ' + path)
                    pass
            else:
                print('Warning: Unable to read or parse json file: ' + path)
            return None


def _write_json_file(obj: object, path: str) -> None:
    with FileLock(path + '.lock', exclusive=True):
        with open(path, 'w') as f:
            json.dump(obj, f)