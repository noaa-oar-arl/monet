import numpy as np
import monet_stats as stats


def test_matchedcompressed():
    a1 = np.array([1, 2, 3, 4])
    a2 = np.array([2, 3, 4, 5])
    result = stats.matchedcompressed(a1, a2)
    assert isinstance(result, tuple)
    assert len(result) == 2


def test_matchmasks():
    a1 = np.array([1, 0, 1, 0])
    a2 = np.array([0, 1, 1, 0])
    result = stats.matchmasks(a1, a2)
    assert isinstance(result, tuple)
    assert len(result) == 2


def test_circlebias_m():
    b = np.array([1, 2, 3])
    result = stats.circlebias_m(b)
    assert isinstance(result, np.ndarray)
    assert np.allclose(result, b)


def test_circlebias():
    b = np.array([1, 2, 3])
    result = stats.circlebias(b)
    assert isinstance(result, np.ndarray)
    assert np.allclose(result, b)
