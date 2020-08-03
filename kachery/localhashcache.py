import json
import os
import shutil
import hashlib
from shutil import copyfile
from .steady_download_and_compute_hash import steady_download_and_compute_hash
import random
import time
from .filelock import FileLock
# import mtlogging
from typing import Optional, List, Any, Dict, Tuple, Union

# TODO: implement cleanup() for LocalHashCache
# removing .record.json and .hints.json files that are no longer relevant

_global = dict(
    printed_warning = False
)

class LocalHashCache:
    def __init__(self, *, algorithm):
        self._directory = None
        self._algorithm = algorithm

    def directory(self) -> str:
        if self._directory:
            return self._directory
        else:
            if 'KACHERY_STORAGE_DIR' in os.environ:
                return os.path.join(str(os.getenv('KACHERY_STORAGE_DIR')), '{}'.format(self._algorithm))
            else:
                if not _global['printed_warning']:
                    print('WARNING: please use the KACHERY_STORAGE_DIR environment variable.')
                    _global['printed_warning'] = True
                if self._algorithm == 'sha1':
                    return os.getenv('SHA1_CACHE_DIR', os.getenv('KBUCKET_CACHE_DIR', '/tmp/sha1-cache'))
                elif self._algorithm == 'md5':
                    return os.getenv('MD5_CACHE_DIR', '/tmp/md5-cache')
                else:
                    raise Exception('Unexpected algorithm: {}'.format(self._algorithm))

    def directory_alt(self) -> Union[None, str]:
        if 'KACHERY_STORAGE_DIR_ALT' in os.environ:
            return os.path.join(str(os.getenv('KACHERY_STORAGE_DIR_ALT')), '{}'.format(self._algorithm))
        else:
            return None

    def setDirectory(self, directory: str) -> None:
        self._directory = directory

    def findFile(self, hash: str) -> Optional[str]:
        path = self._get_path_ext(
            hash=hash, create=False)
        # if file is available return it
        if os.path.exists(path):
            return path

        # check alternate location
        path_alt = self._get_path_alt_ext(
            hash=hash
        )
        # if file is available return it
        if os.path.exists(path_alt):
            return path_alt

        hints_fname = path + '.hints.json'
        # if path.hints.json exists then read it
        if os.path.exists(hints_fname):
            hints = _read_json_file(hints_fname, delete_on_error=True)
            if hints and ('files' in hints):
                files = hints['files']
                matching_files = []
                for file in files:
                    path0 = file['stat']['path']
                    if os.path.exists(path0) and os.path.isfile(path0):
                        stat_obj0 = _get_stat_object(path0)
                        if stat_obj0:
                            if (_stat_objects_match(stat_obj0, file['stat'])):
                                matching_files.append(file)
                if matching_files:
                    hints['files'] = matching_files
                    try:
                        _write_json_file(hints, hints_fname)
                    except:
                        print('Warning: problem writing hints file: ' + hints_fname)
                    return matching_files[0]['stat']['path']
                else:
                    _safe_remove_file(hints_fname)
            else:
                print(
                    'Warning: failed to load hints json file, or invalid file. Removing: ' + hints_fname)
                _safe_remove_file(hints_fname)
        kachery_bootstrap_mountaintools_dir = os.getenv('KACHERY_BOOTSTRAP_MOUNTAINTOOLS_DIR', None)
        if kachery_bootstrap_mountaintools_dir and self._algorithm == 'sha1':
            path_mt = os.path.join(kachery_bootstrap_mountaintools_dir, hash[0], hash[1:3], hash)
            if os.path.exists(path_mt):
                path_dest = self._get_path_ext(hash=hash, create=True)
                print('Copying file from mountaintools cache {} -> {}'.format(path_mt, path_dest))
                path_dest_tmp = path_dest + 'tmp_bootstrap_' + _random_string(8)
                shutil.copyfile(path_mt, path_dest_tmp)
                os.rename(path_dest_tmp, path_dest)
                return path_dest
        return None

    def find_file_by_code(self, *, code: str) -> Optional[str]:
        path = self._get_path_by_code(code=code, create=False)
        if os.path.exists(path):
            return path
        return None
    
    def store_file_by_code(self, *, code: str, data: bytes) -> str:
        path = self._get_path_by_code(code=code, create=True)
        with open(path, 'wb') as f:
            f.write(data)
        return path

    def downloadFile(self, url: str, hash: str, target_path: Optional[str]=None, size: Optional[int]=None, verbose: bool=False, show_progress: bool=False) -> Optional[str]:
        alternate_target_path = False
        if target_path is None:
            target_path = self._get_path(hash=hash, create=True)
        else:
            alternate_target_path = True

        path_tmp = target_path + '.downloading.' + _random_string(6)
        if (verbose) or (show_progress) or ((size is not None) and (size > 10000)):
            print(
                'Downloading file --- ({}): {} -> {}'.format(_format_file_size(size), url, target_path))

        timer = time.time()
        hash_b = steady_download_and_compute_hash(url=url, algorithm=self._algorithm, target_path=path_tmp)
        elapsed = time.time() - timer

        size_b = os.path.getsize(path_tmp)

        if size is not None:
            if size_b != size:
                _safe_remove_file(path_tmp)
                raise Exception(
                    'size of downloaded file does not match expected {} {} <> {}'.format(url, size_b, size))
        if hash_b != hash:
            #_safe_remove_file(path_tmp)
            print(path_tmp)
            raise Exception(
                'hash of downloaded file does not match expected {} {} <> {}'.format(url, hash_b, hash))
        if alternate_target_path:
            if os.path.exists(target_path):
                _safe_remove_file(target_path)
            _rename_file(path_tmp, target_path, remove_if_exists=True)
            self.reportFileHash(target_path, hash=hash)
        else:
            if not os.path.exists(target_path):
                _rename_file(path_tmp, target_path, remove_if_exists=False)
            else:
                _safe_remove_file(path_tmp)
        
        if (verbose) or (show_progress) or ((size is not None) and size > 10000):
            print('Downloaded file ({}) in {} sec.'.format(_format_file_size(size), elapsed))

        return target_path

    def moveFileToCache(self, path: str) -> str:
        hash0 = self.computeFileHash(path)
        assert hash0 is not None
        path0 = self._get_path(hash0, create=True)
        if os.path.exists(path0):
            if path != path0:
                _safe_remove_file(path)
        else:
            tmp_fname = path0 + '.copying.' + _random_string(6)
            _rename_or_copy(path, tmp_fname)
            _rename_file(tmp_fname, path0, remove_if_exists=False)

        return path0

    # @mtlogging.log()
    def copyFileToCache(self, path: str, use_hard_links=False, _known_hash=None) -> Tuple[str, str]:
        if _known_hash is not None:
            hash0 = _known_hash
        else:
            hash0 = self.computeFileHash(path)
        assert hash0 is not None
        path0 = self._get_path(hash0, create=True)
        if not os.path.exists(path0):
            tmp_path = path0 + '.copying.' + _random_string(6)
            if use_hard_links:
                os.link(path, tmp_path)
            else:
                copyfile(path, tmp_path)
            _rename_file(tmp_path, path0, remove_if_exists=False)
        return path0, hash0

    # @mtlogging.log()
    def computeFileHash(self, path: str, _known_hash: str = None, _cache_only: bool = False) -> Optional[str]:
        if os.path.getsize(path) < 100000:
            # if it is a small file, we can compute the hash directory
            # this is important when the KACHERY_STORAGE_DIR is not a remote file system
            return _compute_file_hash(path, algorithm=self._algorithm)
        path = os.path.abspath(path)
        basename = os.path.basename(path)
        if len(basename) == _length_of_hash_for_algorithm(self._algorithm):
            # suspect it is itself a file in the cache
            if self._get_path(hash=basename) == path:
                # in that case we don't need to compute
                return basename

        aa = _get_stat_object(path)
        aa_sha1 = _compute_string_sha1(json.dumps(aa, sort_keys=True))

        path0 = self._get_path(aa_sha1, create=True) + '.record.json'
        if not _known_hash:
            if os.path.exists(path0):
                obj = _read_json_file(path0, delete_on_error=True)
                if obj:
                    bb = obj['stat']
                    if _stat_objects_match(aa, bb):
                        if obj.get(self._algorithm, None):
                            return str(obj[self._algorithm])

        if _known_hash is None:
            if _cache_only:
                return None
            hash1 = _compute_file_hash(path, algorithm=self._algorithm)
        else:
            hash1 = _known_hash

        if not hash1:
            return None

        obj: Dict[str, Union[str, dict, None]] = dict(
            stat=aa
        )
        obj[self._algorithm] = hash1
        try:
            _write_json_file(obj, path0)
        except:
            print('Warning: problem writing .record.json file: ' + path0)

        path1 = self._get_path(hash=hash1, create=True, directory=self.directory()) + '.hints.json'
        hints: Union[dict, None] = None
        if os.path.exists(path1):
            hints = _read_json_file(path1, delete_on_error=True)
        if hints is None:
            hints = dict(files=[])
        hints['files'].append(obj)
        try:
            _write_json_file(hints, path1)
        except:
            print('Warning: problem writing .hints.json file: ' + path1)
        # todo: use hints for findFile
        return hash1

    def reportFileHash(self, path: str, hash: str) -> None:
        self.computeFileHash(path, _known_hash=hash)

    def _get_path(self, hash: str, *, create: bool=True, directory: Optional[str]=None) -> str:
        return str(self._get_path_ext(hash=hash, create=create, directory=directory))

    def _get_path_ext(self, hash: str, *, create: bool=True, directory: Optional[str]=None) -> str:
        if not directory:
            directory = self.directory()
        path1 = os.path.join(hash[0:2], hash[2:4], hash[4:6])
        path0 = os.path.join(str(directory), path1)
        if create:
            if not os.path.exists(path0):
                try:
                    os.makedirs(path0)
                except:
                    if not os.path.exists(path0):
                        raise Exception('Unable to make directory: ' + path0)
        return os.path.join(path0, hash)
    def _get_path_alt_ext(self, hash: str, *, directory: Optional[str]=None) -> str:
        if not directory:
            directory = self.directory_alt()
        path1 = os.path.join(hash[0:2], hash[2:4], hash[4:6])
        path0 = os.path.join(str(directory), path1)
        return os.path.join(path0, hash)
    def _get_path_by_code(self, *, code: str, create: bool) -> str:
        directory = self.directory()
        path1 = os.path.join(code[0], code[1:3])
        path0 = os.path.join(str(directory), path1)
        if create:
            if not os.path.exists(path0):
                try:
                    os.makedirs(path0)
                except:
                    if not os.path.exists(path0):
                        raise Exception('Unable to make directory: ' + path0)
        return os.path.join(path0, code)

# @mtlogging.log()
def _compute_file_hash(path: str, algorithm: str) -> Optional[str]:
    if not os.path.exists(path):
        return None
    if (os.path.getsize(path) > 1024 * 1024 * 100):
        print('Computing {} of {}'.format(algorithm, path))
    BLOCKSIZE = 65536
    hashsum = getattr(hashlib, algorithm)()
    with open(path, 'rb') as file:
        buf = file.read(BLOCKSIZE)
        while len(buf) > 0:
            hashsum.update(buf)
            buf = file.read(BLOCKSIZE)
    return hashsum.hexdigest()


def _get_stat_object(fname: str) -> Optional[Dict]:
    try:
        stat0 = os.stat(fname)
        obj = dict(
            path=fname,
            size=stat0.st_size,
            ino=stat0.st_ino,
            mtime=stat0.st_mtime,
            ctime=stat0.st_ctime
        )
        return obj
    except:
        return None


def _stat_objects_match(aa: object, bb: object) -> bool:
    str1 = json.dumps(aa, sort_keys=True)
    str2 = json.dumps(bb, sort_keys=True)
    return (str1 == str2)


def _compute_string_sha1(txt: str) -> str:
    hash_object = hashlib.sha1(txt.encode('utf-8'))
    return hash_object.hexdigest()


def _safe_remove_file(fname: str) -> None:
    try:
        os.remove(fname)
    except:
        print('Warning: unable to remove file that we thought existed: ' + fname)


# @mtlogging.log()
def _read_json_file(path: str, *, delete_on_error: bool=False) -> Any:
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


def _rename_or_copy(path1: str, path2: str) -> None:
    if os.path.abspath(path1) == os.path.abspath(path2):
        return
    try:
        os.rename(path1, path2)
    except:
        try:
            shutil.copyfile(path1, path2)
        except:
            raise Exception('Problem renaming or copying file: {} -> {}'.format(path1, path2))


# @mtlogging.log()
def _rename_file(path1: str, path2: str, remove_if_exists: bool) -> None:
    if os.path.abspath(path1) == os.path.abspath(path2):
        return
    if os.path.exists(path2):
        if remove_if_exists:
            try:
                os.unlink(path2)
            except:
                # maybe it was removed by someone else
                pass
        else:
            # already exists, let's just let it be
            return
    try:
        os.rename(path1, path2)
    except:
        if os.path.exists(path2):
            if not remove_if_exists:
                # all good
                return
            raise Exception('Problem renaming file: {} -> {}'.format(path1, path2))
        else:
            raise Exception('Problem renaming file:: {} -> {}'.format(path1, path2))


# thanks: https://stackoverflow.com/questions/1094841/reusable-library-to-get-human-readable-version-of-file-size

def _format_file_size(size: Optional[int]) -> str:
    if not size:
        return 'Unknown'
    if size <= 1024:
        return '{} B'.format(size)
    return _sizeof_fmt(size)


def _sizeof_fmt(num: int, suffix='B') -> str:
    num_float = float(num)
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num_float) < 1024.0:
            return "%3.1f %s%s" % (num_float, unit, suffix)
        num_float /= 1024.0
    return "%.1f %s%s" % (num_float, 'Yi', suffix)


def _random_string(num_chars: int) -> str:
    chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
    return ''.join(random.choice(chars) for _ in range(num_chars))

def _length_of_hash_for_algorithm(algorithm):
    if algorithm == 'sha1':
        return 40
    elif algorithm == 'md5':
        return 32
    else:
        raise Exception('Unexpected algorithm: {}'.format(algorithm))