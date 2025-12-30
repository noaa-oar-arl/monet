import numpy as np
import monet_stats as stats


def test_MNB():
    obs = np.array([1, 2, 3])
    mod = np.array([1, 2, 2.5])
    result = stats.MNB(obs, mod)
    assert isinstance(result, float)


def test_MNE():
    obs = np.array([1, 2, 3])
    mod = np.array([1, 2, 2.5])
    result = stats.MNE(obs, mod)
    assert isinstance(result, float)


def test_RMSE():
    obs = np.array([1, 2, 3])
    mod = np.array([1, 2, 2.5])
    rmse = stats.STDO(obs, mod)
    assert isinstance(rmse, float)
