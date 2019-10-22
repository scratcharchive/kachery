import kachery as ka
import numpy as np

def test_local():
    print('Running test_local')
    _test_store_text('abctest')
    _test_store_object(dict(a=1, b=2, c=[1, 2, 3]))
    _test_store_npy(np.ones((12, 12)))
    print('Finished test_local')

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
    test_local()