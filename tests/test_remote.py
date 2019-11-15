import kachery as ka
import numpy as np

def test_remote():
    ka.set_config(
        to='default_readwrite',
        fr='default_readwrite'
    )
    
    for alg in ['sha1', 'md5']:
        ka.set_config(algorithm=alg)
        for pass0 in range(1, 3):
            if pass0 == 1:
                ka.set_config(from_remote_only=True, to_remote_only=True)
            elif pass0 == 2:
                ka.set_config(from_remote_only=False, to_remote_only=False)
            _test_store_text('abctest2')
            _test_store_object(dict(a=1, b=2, c=[1, 2, 3, 4]))
            _test_store_npy(np.ones((12, 14)))
    
    a = ka.load_text('sha1://906faceaf874dd64e81de0048f36f4bab0f1f171')
    print(a)

def _test_store_text(val: str):
    x = ka.store_text(val)
    assert x
    val2 = ka.load_text(x)
    assert val == val2

def _test_store_object(val: dict):
    x = ka.store_object(val)
    assert x
    val2 = ka.load_object(x)
    assert val == val2

def _test_store_npy(val: np.ndarray):
    x = ka.store_npy(val)
    assert x
    val2 = ka.load_npy(x)
    assert np.array_equal(val, val2)

if __name__ == '__main__':
    test_remote()