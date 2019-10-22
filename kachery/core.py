import os
import numpy as np
from typing import Union, Tuple, Optional, List
import simplejson
import json
import hashlib
import tempfile
import time
import requests
import urllib.request as request
from .sha1cache import Sha1Cache

_global_config=dict(
    url=os.getenv('KACHERY_URL', None),
    channel=os.getenv('KACHERY_CHANNEL', None),
    password=os.getenv('KACHERY_PASSWORD', None),
    use_remote=False,
    use_remote_only=False
)

_sha1_cache = Sha1Cache()

def set_config(*,
        use_remote: Union[bool, None]=None,
        use_remote_only: Union[bool, None]=None,
        channel: Union[str, None]=None,
        password: Union[str, None]=None,
        url: Union[str, None]=None
) -> None:
    if use_remote is not None:
        _global_config['use_remote'] = use_remote
    if use_remote_only is not None:
        _global_config['use_remote_only'] = use_remote_only
    if channel is not None:
        _global_config['channel'] = channel
    if password is not None:
        _global_config['password'] = password
    if url is not None:
        _global_config['url'] = url

def get_config() -> dict:
    return _load_config()

def _load_config(**kwargs) -> dict:
    if 'config' in kwargs:
        ret = kwargs['config']
    else:
        ret = dict()
        for key, val in _global_config.items():
            ret[key] = val
    for key, val in kwargs.items():
        ret[key] = val
    return ret

def load_file(path: str, **kwargs)-> Union[str, None]:
    config = _load_config(**kwargs)
    if path.startswith('sha1://') or path.startswith('sha1dir://'):
        if not config['use_remote_only']:
            ret = _find_file_locally(path, config=config)
            if ret:
                return ret
        if config['use_remote'] or config['use_remote_only']:
            url0, sha1, size0 = _check_remote_file(path, config=config)
            if url0:
                assert sha1 is not None
                assert size0 is not None
                return _sha1_cache.downloadFile(url=url0, sha1=sha1, size=size0)
            else:
                return None
        return None
    else:
        if os.path.isfile(path):
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

def store_file(path: str, basename: Union[str, None]=None, **kwargs) -> Union[str, None]:
    if basename is None:
        basename = os.path.basename(path)
    config = _load_config(**kwargs)
    sha1 = _compute_local_file_sha1(path, config=config)
    if not sha1:
        raise Exception('Unable to compute SHA-1 of file: {}'.format(path))
    if not config['use_remote_only']:
        _store_local_file_in_cache(path, sha1=sha1, config=config)
    if config['use_remote'] or config['use_remote_only']:
        _upload_local_file(path, sha1=sha1, config=config)
    return 'sha1://{}/{}'.format(sha1, basename)
    
    
def store_text(text: str, basename: Union[str, None]=None, **kwargs) -> Union[str, None]:
    if basename is None:
        basename = 'file.txt'
    with tempfile.NamedTemporaryFile() as tmpfile:
        with open(tmpfile.name, 'w') as f:
            f.write(text)
        return store_file(tmpfile.name, basename=basename, **kwargs)

def store_object(object: dict, basename: Union[str, None]=None, indent: Union[int, None]=None, **kwargs) -> Union[str, None]:
    if basename is None:
        basename = 'file.json'
    txt = simplejson.dumps(object, indent=indent)
    return store_text(text=txt, basename=basename, **kwargs)

def store_npy(array: np.ndarray, basename: Union[str, None]=None, **kwargs) -> Union[str, None]:
    if basename is None:
        basename = 'file.npy'
    with tempfile.NamedTemporaryFile(suffix='.npy') as tmpfile:
        np.save(tmpfile.name, array)
        return store_file(tmpfile.name, basename=basename, **kwargs)

def store_dir(dirpath: str, label: Union[str, None]=None, **kwargs):
    if label is None:
        label = os.path.basename(dirpath)
    raise Exception('Not yet implemented')

def read_dir(path: str, **kwargs):
    raise Exception('Not yet implemented')

def _compute_local_file_sha1(path: str, *, config: dict) -> Union[str, None]:
    return _sha1_cache.computeFileSha1(path)

def _store_local_file_in_cache(path: str, *, sha1: str, config: dict) -> None:
    local_path, sha1 = _sha1_cache.copyFileToCache(path)
    if sha1 is None:
        raise Exception('Unable to store local file in cache: {}'.format(path))

def _find_file_locally(path: str, *, config: dict) -> Union[str, None]:
    if path.startswith('sha1://') or path.startswith('sha1dir://'):
        sha1 = _determine_file_sha1_from_url(path, config=config)
        return _sha1_cache.findFile(sha1=sha1)
    elif os.path.isfile(path):
        return path
    else:
        return None

def _check_remote_file(sha1_url: str, *, config: dict) -> Tuple[Union[str, None], Union[str, None], Union[int, None]]:
    if sha1_url.startswith('sha1://') or sha1_url.startswith('sha1dir://'):
        sha1: str = _determine_file_sha1_from_url(sha1_url, config=config)
        url_check: str = _form_check_url(sha1=sha1, config=config)
        check_resp: dict = _http_get_json(url_check)
        if not check_resp['success']:
            print('Warning: Problem checking for file: ' + check_resp['error'])
            return None, None, None
        if check_resp['found']:
            url_download = _form_download_url(sha1=sha1, config=config)
            size = check_resp['size']
            return url_download, sha1, size
        else:
            return None, None, None
    else:
        raise Exception('Unexpected')

def _form_download_url(*, sha1: str, config: dict) -> str:
    url = config['url']
    channel = config['channel']
    signature = _sha1_of_object(dict(
        name='download',
        password=config['password'],
        sha1=sha1
    ))
    return '{}/get/sha1/{}?channel={}&signature={}'.format(url, sha1, channel, signature)

def _form_check_url(*, sha1: str, config: dict) -> str:
    url = config['url']
    channel = config['channel']
    signature = _sha1_of_object(dict(
        name='check',
        password=config['password'],
        sha1=sha1
    ))
    return '{}/check/sha1/{}?channel={}&signature={}'.format(url, sha1, channel, signature)

def _form_upload_url(*, sha1: str, config: dict) -> str:
    url = config['url']
    channel = config['channel']
    signature = _sha1_of_object(dict(
        name='upload',
        password=config['password'],
        sha1=sha1
    ))
    return '{}/set/sha1/{}?channel={}&signature={}'.format(url, sha1, channel, signature)

def _upload_local_file(path: str, *, sha1: str, config: dict) -> None:
    if not config['url']:
        raise Exception('Missing url config parameter for uploading to remote server')
    if not config['channel']:
        raise Exception('Missing channel config parameter for uploading to remote server')
    if not config['password']:
        raise Exception('Missing password config parameter for uploading to remote server')
    size0 = os.path.getsize(path)

    url_ch, sha1_ch, size_ch = _check_remote_file('sha1://{}'.format(sha1), config=config)
    if url_ch is not None:
        # already on the remote server
        if size_ch != size0:
            raise Exception('Unexpected: size of file on remote server does not match local file {} - {} <> {}'.format(path, size_ch, size0))
        return

    url0 = _form_upload_url(sha1=sha1, config=config)
    if size0 > 10000:
        print('Uploading to kachery --- ({}): {} -> {}'.format(_format_file_size(size0), path, url))

    timer = time.time()
    resp_obj = _http_post_file_data(url0, path)
    elapsed = time.time() - timer

    if size0 > 10000:
        print('File uploaded ({}) in {} sec'.format(
            _format_file_size(size0), elapsed))

    if not resp_obj.get('success', False):
        raise Exception('Problem posting file data: ' + resp_obj.get('error', ''))

def _determine_file_sha1_from_url(url: str, *, config: dict) -> str:
    protocol, sha1, additional_path = _parse_sha1_url(url)
    if protocol == 'sha1':
        return sha1
    elif protocol == 'sha1dir':
        raise Exception('This case not yet implemented')
    else:
        raise Exception('Unexpected protocol in url {}: {}'.format(url, protocol))

def _parse_sha1_url(url: str) -> Tuple[str, str, str]:
    list0 = url.split('/')
    protocol = list0[0].replace(':', '')
    sha1 = list0[2]
    additional_path = '/'.join(list0[3:])
    return protocol, sha1, additional_path

def _sha1_of_string(txt: str) -> str:
    hh = hashlib.sha1(txt.encode('utf-8'))
    ret = hh.hexdigest()
    return ret


def _sha1_of_object(obj: object) -> str:
    txt = json.dumps(obj, sort_keys=True, separators=(',', ':'))
    return _sha1_of_string(txt)

def _http_get_json(url: str, verbose: Optional[bool]=False, retry_delays: Optional[List[float]]=None) -> dict:
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
    return ret

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
            obj = requests.post(url, data=f)
        except:
            raise Exception('Error posting file data.')
    if obj.status_code != 200:
        return dict(
            success=False,
            error='Error posting file data: {} {}'.format(obj.status_code, obj.content.decode('utf-8'))
        )
    if verbose:
        print('Elapsed time for _http_post_file_Data: {}'.format(time.time() - timer))
    return json.loads(obj.content)