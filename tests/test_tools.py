import numpy as np
import pytest

from monet.util.tools import search_listinlist


def test_search_listinlist_correctness():
    """Tests the correctness of the search_listinlist function."""
    array1 = np.array([1, 2, 3, 4, 5])
    array2 = np.array([3, 5, 6, 7, 8])
    index1, index2 = search_listinlist(array1, array2)
    np.testing.assert_array_equal(index1, np.array([2, 4]))
    np.testing.assert_array_equal(index2, np.array([0, 1]))


@pytest.mark.skipif("benchmark" not in dir(), reason="pytest-benchmark not available")
def test_search_listinlist_benchmark(benchmark):
    """Benchmarks the search_listinlist function."""
    array1 = np.arange(1000)
    array2 = np.arange(500, 1500)
    benchmark(search_listinlist, array1, array2)
