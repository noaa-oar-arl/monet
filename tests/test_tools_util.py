import numpy as np
import pytest

from monet.util import tools


def test_search_listinlist_basic():
    a1 = [1, 2, 3]
    a2 = [2, 3, 4]
    idx1, idx2 = tools.search_listinlist(a1, a2)
    # idx1: indices in a1 where matches were found
    # idx2: indices in a2 where matches were found
    assert list(idx1) == [1, 2]
    assert list(idx2) == [0, 1]


def test_linregress_basic():
    x = np.array([1, 2, 3, 4])
    y = np.array([2, 4, 6, 8])
    try:
        slope, intercept, r_value, p_value = tools.linregress(x, y)
        assert np.isclose(slope, 2.0)
        assert np.isclose(intercept, 0.0)
    except ImportError:
        pytest.skip("statsmodels not installed")


def test_findclosest():
    lst = [1, 3, 7, 10]
    value = 5
    idx, closest = tools.findclosest(lst, value)
    assert idx == 1 or idx == 2
    assert closest in [3, 7]
