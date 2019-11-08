import string
import random
import hashlib
import os
# import requests
import urllib
from typing import Union
import time

def steady_download_and_compute_hash(url: str, algorithm: str, target_path: str) -> str:
    remote = urllib.request.urlopen(url)
    str0 = ''.join(random.sample(string.ascii_lowercase, 8))
    path_tmp = target_path + '.tmp.' + str0

    hh = getattr(hashlib, algorithm)()
    with open(path_tmp, 'wb') as f:
        while True:
            chunk = remote.read(4096)
            if not chunk:
                break
            hh.update(chunk)
            f.write(chunk)
    os.rename(path_tmp, target_path)
    hash0 = hh.hexdigest()
    return hash0

## somehow this was not always working -- some bits were wrong for large files!
def old_steady_download_and_compute_hash(url: str, algorithm: str, target_path: str, chunk_size: int=1024 * 1024 * 40) -> str:
    response = requests.head(url)
    size_bytes = int(response.headers['content-length'])
    str0 = ''.join(random.sample(string.ascii_lowercase, 8))
    path_tmp = target_path + '.tmp.' + str0
    try:
        hh = getattr(hashlib, algorithm)()
        with open(path_tmp, 'wb') as f:
            for ii in range(0, size_bytes, chunk_size):
                jj = ii + chunk_size
                if jj > size_bytes:
                    jj = size_bytes
                headers = {
                    'Range': 'bytes={}-{}'.format(ii, jj - 1)
                }
                response = requests.get(url, headers=headers, stream=True)
                for chunk in response.iter_content(chunk_size=5120):
                    if chunk:  # filter out keep-alive new chunks
                        hh.update(chunk)
                        f.write(chunk)
        os.rename(path_tmp, target_path)
        hash0 = hh.hexdigest()
        return hash0
    except:
        if os.path.exists(path_tmp):
            os.remove(path_tmp)
        raise
