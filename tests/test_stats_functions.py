
import numpy as np
import pytest
from monet.util import stats
import dask.array as da
import dask.config
import gc

# Set Dask scheduler to single-threaded for all tests
dask.config.set(scheduler="single-threaded")

# Helper to convert numpy arrays to dask arrays
def to_dask(arr):
    return da.from_array(arr, chunks=arr.shape)

def test_MB():
    obs = np.array([1, 2, 3, 4, 5])
    mod = np.array([2, 2, 3, 4, 6])
    result = stats.MB(obs, mod)
    assert np.isclose(result, np.mean(mod - obs))

    # Dask version
    obs_d = to_dask(obs)
    mod_d = to_dask(mod)
    result_d = stats.MB(obs_d, mod_d)
    result_d = da.compute(result_d)[0]
    assert np.isclose(result_d, np.mean(mod - obs))
    del obs_d, mod_d, result_d
    gc.collect()

def test_NMB():
    obs = np.array([1, 2, 3, 4, 5])
    mod = np.array([2, 2, 3, 4, 6])
    result = stats.NMB(obs, mod)
    assert np.isclose(result, 13.333333333333334)

    obs_d = to_dask(obs)
    mod_d = to_dask(mod)
    result_d = stats.NMB(obs_d, mod_d)
    result_d = da.compute(result_d)[0]
    assert np.isclose(result_d, 13.333333333333334)
    del obs_d, mod_d, result_d
    gc.collect()

def test_RMSE():
    obs = np.array([1, 2, 3, 4, 5])
    mod = np.array([2, 2, 3, 4, 6])
    result = stats.RMSE(obs, mod)
    assert np.isclose(result, np.sqrt(0.4))

    obs_d = to_dask(obs)
    mod_d = to_dask(mod)
    result_d = stats.RMSE(obs_d, mod_d)
    result_d = da.compute(result_d)[0]
    assert np.isclose(result_d, np.sqrt(0.4))
    del obs_d, mod_d, result_d
    gc.collect()

def test_IOA():
    obs = np.array([1, 2, 3, 4, 5])
    mod = np.array([2, 2, 3, 4, 6])
    result = stats.IOA(obs, mod)
    assert 0 <= result <= 1

    obs_d = to_dask(obs)
    mod_d = to_dask(mod)
    result_d = stats.IOA(obs_d, mod_d)
    result_d = da.compute(result_d)[0]
    assert 0 <= result_d <= 1
    del obs_d, mod_d, result_d
    gc.collect()

def test_FE():
    obs = np.array([1, 2, 3, 4, 5])
    mod = np.array([2, 2, 3, 4, 6])
    result = stats.FE(obs, mod)
    assert result >= 0

    obs_d = to_dask(obs)
    mod_d = to_dask(mod)
    result_d = stats.FE(obs_d, mod_d)
    result_d = da.compute(result_d)[0]
    assert result_d >= 0
    del obs_d, mod_d, result_d
    gc.collect()

def test_NME():
    obs = np.array([1, 2, 3, 4, 5])
    mod = np.array([2, 2, 3, 4, 6])
    result = stats.NME(obs, mod)
    assert result >= 0

    obs_d = to_dask(obs)
    mod_d = to_dask(mod)
    result_d = stats.NME(obs_d, mod_d)
    result_d = da.compute(result_d)[0]
    assert result_d >= 0
    del obs_d, mod_d, result_d
    gc.collect()

def test_R2():
    obs = np.array([1, 2, 3, 4, 5])
    mod = np.array([2, 2, 3, 4, 6])
    result = stats.R2(obs, mod)
    assert 0 <= result <= 1

    obs_d = to_dask(obs)
    mod_d = to_dask(mod)
    result_d = stats.R2(obs_d, mod_d)
    result_d = da.compute(result_d)[0]
    assert 0 <= result_d <= 1
    del obs_d, mod_d, result_d
    gc.collect()

def test_STDO():
    obs = np.array([1, 2, 3, 4, 5])
    mod = np.array([2, 2, 3, 4, 6])
    result = stats.STDO(obs, mod)
    assert np.isclose(result, np.std(obs))

    obs_d = to_dask(obs)
    mod_d = to_dask(mod)
    result_d = stats.STDO(obs_d, mod_d)
    result_d = da.compute(result_d)[0]
    assert np.isclose(result_d, np.std(obs))
    del obs_d, mod_d, result_d
    gc.collect()

def test_STDP():
    obs = np.array([1, 2, 3, 4, 5])
    mod = np.array([2, 2, 3, 4, 6])
    result = stats.STDP(obs, mod)
    assert np.isclose(result, np.std(mod))

    obs_d = to_dask(obs)
    mod_d = to_dask(mod)
    result_d = stats.STDP(obs_d, mod_d)
    result_d = da.compute(result_d)[0]
    assert np.isclose(result_d, np.std(mod))
    del obs_d, mod_d, result_d
    gc.collect()

def test_MNB():
    obs = np.array([1, 2, 3, 4, 5])
    mod = np.array([2, 2, 3, 4, 6])
    result = stats.MNB(obs, mod)
    expected = np.mean((mod - obs) / obs) * 100.0
    assert np.isclose(result, expected)

    obs_d = to_dask(obs)
    mod_d = to_dask(mod)
    result_d = stats.MNB(obs_d, mod_d)
    result_d = da.compute(result_d)[0]
    assert np.isclose(result_d, expected)
    del obs_d, mod_d, result_d
    gc.collect()

def test_MNE():
    obs = np.array([1, 2, 3, 4, 5])
    mod = np.array([2, 2, 3, 4, 6])
    result = stats.MNE(obs, mod)
    expected = np.mean(np.abs(mod - obs) / obs) * 100.0
    assert np.isclose(result, expected)

    obs_d = to_dask(obs)
    mod_d = to_dask(mod)
    result_d = stats.MNE(obs_d, mod_d)
    result_d = da.compute(result_d)[0]
    assert np.isclose(result_d, expected)
    del obs_d, mod_d, result_d
    gc.collect()

def test_MdnNB():
    obs = np.array([1, 2, 3, 4, 5])
    mod = np.array([2, 2, 3, 4, 6])
    result = stats.MdnNB(obs, mod)
    expected = np.median((mod - obs) / obs) * 100.0
    assert np.isclose(result, expected)

    obs_d = to_dask(obs)
    mod_d = to_dask(mod)
    result_d = stats.MdnNB(obs_d, mod_d)
    result_d = da.compute(result_d)[0]
    assert np.isclose(result_d, expected)
    del obs_d, mod_d, result_d
    gc.collect()

def test_MdnNE():
    obs = np.array([1, 2, 3, 4, 5])
    mod = np.array([2, 2, 3, 4, 6])
    result = stats.MdnNE(obs, mod)
    expected = np.median(np.abs(mod - obs) / obs) * 100.0
    assert np.isclose(result, expected)

    obs_d = to_dask(obs)
    mod_d = to_dask(mod)
    result_d = stats.MdnNE(obs_d, mod_d)
    result_d = da.compute(result_d)[0]
    assert np.isclose(result_d, expected)
    del obs_d, mod_d, result_d
    gc.collect()

def test_NO():
    obs = np.array([1, 2, 3, 4, 5])
    mod = np.array([2, 2, 3, 4, 6])
    result = stats.NO(obs, mod)
    assert result == 5

    obs_d = to_dask(obs)
    mod_d = to_dask(mod)
    result_d = stats.NO(obs_d, mod_d)
    result_d = da.compute(result_d)[0]
    assert result_d == 5
    del obs_d, mod_d, result_d
    gc.collect()

def test_NP():
    obs = np.array([1, 2, 3, 4, 5])
    mod = np.array([2, 2, 3, 4, 6])
    result = stats.NP(obs, mod)
    assert result == 5

    obs_d = to_dask(obs)
    mod_d = to_dask(mod)
    result_d = stats.NP(obs_d, mod_d)
    result_d = da.compute(result_d)[0]
    assert result_d == 5
    del obs_d, mod_d, result_d
    gc.collect()

def test_MO():
    obs = np.array([1, 2, 3, 4, 5])
    mod = np.array([2, 2, 3, 4, 6])
    result = stats.MO(obs, mod)
    assert np.isclose(result, np.mean(obs))

    obs_d = to_dask(obs)
    mod_d = to_dask(mod)
    result_d = stats.MO(obs_d, mod_d)
    result_d = da.compute(result_d)[0]
    assert np.isclose(result_d, np.mean(obs))
    del obs_d, mod_d, result_d
    gc.collect()

def test_MP():
    obs = np.array([1, 2, 3, 4, 5])
    mod = np.array([2, 2, 3, 4, 6])
    result = stats.MP(obs, mod)
    assert np.isclose(result, np.mean(mod))

    obs_d = to_dask(obs)
    mod_d = to_dask(mod)
    result_d = stats.MP(obs_d, mod_d)
    result_d = da.compute(result_d)[0]
    assert np.isclose(result_d, np.mean(mod))
    del obs_d, mod_d, result_d
    gc.collect()

def test_MdnO():
    obs = np.array([1, 2, 3, 4, 5])
    mod = np.array([2, 2, 3, 4, 6])
    result = stats.MdnO(obs, mod)
    assert np.isclose(result, np.median(obs))

    obs_d = to_dask(obs)
    mod_d = to_dask(mod)
    result_d = stats.MdnO(obs_d, mod_d)
    result_d = da.compute(result_d)[0]
    assert np.isclose(result_d, np.median(obs))
    del obs_d, mod_d, result_d
    gc.collect()

def test_MdnP():
    obs = np.array([1, 2, 3, 4, 5])
    mod = np.array([2, 2, 3, 4, 6])
    result = stats.MdnP(obs, mod)
    assert np.isclose(result, np.median(mod))

    obs_d = to_dask(obs)
    mod_d = to_dask(mod)
    result_d = stats.MdnP(obs_d, mod_d)
    result_d = da.compute(result_d)[0]
    assert np.isclose(result_d, np.median(mod))
    del obs_d, mod_d, result_d
    gc.collect()

def test_RM():
    obs = np.array([1, 2, 3, 4, 5])
    mod = np.array([2, 2, 3, 4, 6])
    result = stats.RM(obs, mod)
    expected = np.mean(obs / mod)
    assert np.isclose(result, expected)

    obs_d = to_dask(obs)
    mod_d = to_dask(mod)
    result_d = stats.RM(obs_d, mod_d)
    result_d = da.compute(result_d)[0]
    assert np.isclose(result_d, expected)
    del obs_d, mod_d, result_d
    gc.collect()

def test_RMdn():
    obs = np.array([1, 2, 3, 4, 5])
    mod = np.array([2, 2, 3, 4, 6])
    result = stats.RMdn(obs, mod)
    expected = np.median(obs / mod)
    assert np.isclose(result, expected)

    obs_d = to_dask(obs)
    mod_d = to_dask(mod)
    result_d = stats.RMdn(obs_d, mod_d)
    result_d = da.compute(result_d)[0]
    assert np.isclose(result_d, expected)
    del obs_d, mod_d, result_d
    gc.collect()

# Additional tests for uncovered stats functions
def test_NMdnGE():
    obs = np.array([1, 2, 3, 4, 5])
    mod = np.array([2, 2, 3, 4, 6])
    result = stats.NMdnGE(obs, mod)
    assert np.isfinite(result)

    obs_d = to_dask(obs)
    mod_d = to_dask(mod)
    result_d = stats.NMdnGE(obs_d, mod_d)
    result_d = da.compute(result_d)[0]
    assert np.isfinite(result_d)
    del obs_d, mod_d, result_d
    gc.collect()

def test_NOP():
    obs = np.array([1, 2, 3, 4, 5])
    mod = np.array([2, 2, 3, 4, 6])
    result = stats.NOP(obs, mod)
    assert result == 5

    obs_d = to_dask(obs)
    mod_d = to_dask(mod)
    result_d = stats.NOP(obs_d, mod_d)
    result_d = da.compute(result_d)[0]
    assert result_d == 5
    del obs_d, mod_d, result_d
    gc.collect()

def test_MdnB():
    obs = np.array([1, 2, 3, 4, 5])
    mod = np.array([2, 2, 3, 4, 6])
    result = stats.MdnB(obs, mod)
    assert np.isclose(result, np.median(mod - obs))

    obs_d = to_dask(obs)
    mod_d = to_dask(mod)
    result_d = stats.MdnB(obs_d, mod_d)
    result_d = da.compute(result_d)[0]
    assert np.isclose(result_d, np.median(mod - obs))
    del obs_d, mod_d, result_d
    gc.collect()

def test_FB():
    obs = np.array([1, 2, 3, 4, 5])
    mod = np.array([2, 2, 3, 4, 6])
    result = stats.FB(obs, mod)
    assert np.isfinite(result)

    obs_d = to_dask(obs)
    mod_d = to_dask(mod)
    result_d = stats.FB(obs_d, mod_d)
    result_d = da.compute(result_d)[0]
    assert np.isfinite(result_d)
    del obs_d, mod_d, result_d
    gc.collect()

def test_ME():
    obs = np.array([1, 2, 3, 4, 5])
    mod = np.array([2, 2, 3, 4, 6])
    result = stats.ME(obs, mod)
    assert np.isclose(result, np.mean(np.abs(mod - obs)))

    obs_d = to_dask(obs)
    mod_d = to_dask(mod)
    result_d = stats.ME(obs_d, mod_d)
    result_d = da.compute(result_d)[0]
    assert np.isclose(result_d, np.mean(np.abs(mod - obs)))
    del obs_d, mod_d, result_d
    gc.collect()

def test_MdnE():
    obs = np.array([1, 2, 3, 4, 5])
    mod = np.array([2, 2, 3, 4, 6])
    result = stats.MdnE(obs, mod)
    assert np.isclose(result, np.median(np.abs(mod - obs)))

    obs_d = to_dask(obs)
    mod_d = to_dask(mod)
    result_d = stats.MdnE(obs_d, mod_d)
    result_d = da.compute(result_d)[0]
    assert np.isclose(result_d, np.median(np.abs(mod - obs)))
    del obs_d, mod_d, result_d
    gc.collect()

def test_NME_m():
    obs = np.array([1, 2, 3, 4, 5])
    mod = np.array([2, 2, 3, 4, 6])
    result = stats.NME_m(obs, mod)
    assert np.isfinite(result)

    obs_d = to_dask(obs)
    mod_d = to_dask(mod)
    result_d = stats.NME_m(obs_d, mod_d)
    result_d = da.compute(result_d)[0]
    assert np.isfinite(result_d)
    del obs_d, mod_d, result_d
    gc.collect()

def test_NME_m_ABS():
    obs = np.array([1, 2, 3, 4, 5])
    mod = np.array([2, 2, 3, 4, 6])
    result = stats.NME_m_ABS(obs, mod)
    assert np.isfinite(result)

    obs_d = to_dask(obs)
    mod_d = to_dask(mod)
    result_d = stats.NME_m_ABS(obs_d, mod_d)
    result_d = da.compute(result_d)[0]
    assert np.isfinite(result_d)
    del obs_d, mod_d, result_d
    gc.collect()

def test_NMdnE():
    obs = np.array([1, 2, 3, 4, 5])
    mod = np.array([2, 2, 3, 4, 6])
    result = stats.NMdnE(obs, mod)
    assert np.isfinite(result)

    obs_d = to_dask(obs)
    mod_d = to_dask(mod)
    result_d = stats.NMdnE(obs_d, mod_d)
    result_d = da.compute(result_d)[0]
    assert np.isfinite(result_d)
    del obs_d, mod_d, result_d
    gc.collect()

def test_USUTPB():
    obs = np.array([1, 2, 3, 4, 5])
    mod = np.array([2, 2, 3, 4, 6])
    result = stats.USUTPB(obs, mod)
    assert np.isfinite(result)

    obs_d = to_dask(obs)
    mod_d = to_dask(mod)
    result_d = stats.USUTPB(obs_d, mod_d)
    result_d = da.compute(result_d)[0]
    assert np.isfinite(result_d)
    del obs_d, mod_d, result_d
    gc.collect()

def test_USUTPE():
    obs = np.array([1, 2, 3, 4, 5])
    mod = np.array([2, 2, 3, 4, 6])
    result = stats.USUTPE(obs, mod)
    assert np.isfinite(result)

    obs_d = to_dask(obs)
    mod_d = to_dask(mod)
    result_d = stats.USUTPE(obs_d, mod_d)
    result_d = da.compute(result_d)[0]
    assert np.isfinite(result_d)
    del obs_d, mod_d, result_d
    gc.collect()

def test_MNPB():
    obs = np.array([[1, 2], [3, 4]])
    mod = np.array([[2, 2], [3, 5]])
    result = stats.MNPB(obs, mod, paxis=1)
    assert np.isfinite(result)

    obs_d = to_dask(obs)
    mod_d = to_dask(mod)
    result_d = stats.MNPB(obs_d, mod_d, paxis=1)
    result_d = da.compute(result_d)[0]
    assert np.isfinite(result_d)
    del obs_d, mod_d, result_d
    gc.collect()

def test_MdnNPB():
    obs = np.array([[1, 2], [3, 4]])
    mod = np.array([[2, 2], [3, 5]])
    result = stats.MdnNPB(obs, mod, paxis=1)
    assert np.isfinite(result)

    obs_d = to_dask(obs)
    mod_d = to_dask(mod)
    result_d = stats.MdnNPB(obs_d, mod_d, paxis=1)
    result_d = da.compute(result_d)[0]
    assert np.isfinite(result_d)
    del obs_d, mod_d, result_d
    gc.collect()

def test_MNPE():
    obs = np.array([[1, 2], [3, 4]])
    mod = np.array([[2, 2], [3, 5]])
    result = stats.MNPE(obs, mod, paxis=1)
    assert np.isfinite(result)

    obs_d = to_dask(obs)
    mod_d = to_dask(mod)
    result_d = stats.MNPE(obs_d, mod_d, paxis=1)
    result_d = da.compute(result_d)[0]
    assert np.isfinite(result_d)
    del obs_d, mod_d, result_d
    gc.collect()

def test_MdnNPE():
    obs = np.array([[1, 2], [3, 4]])
    mod = np.array([[2, 2], [3, 5]])
    result = stats.MdnNPE(obs, mod, paxis=1)
    assert np.isfinite(result)

    obs_d = to_dask(obs)
    mod_d = to_dask(mod)
    result_d = stats.MdnNPE(obs_d, mod_d, paxis=1)
    result_d = da.compute(result_d)[0]
    assert np.isfinite(result_d)
    del obs_d, mod_d, result_d
    gc.collect()

def test_scores():
    obs = np.array([0, 1, 1, 0, 1])
    mod = np.array([0, 1, 0, 0, 1])
    a, b, c, d = stats.scores(obs, mod, minval=0.5, maxval=1.5)
    # a: hit, b: miss, c: false alarm, d: correct negative
    # Actual output for this input: a=2, b=1, c=0, d=2
    assert a == 2 and b == 1 and c == 0 and d == 2

    obs_d = to_dask(obs)
    mod_d = to_dask(mod)
    a_d, b_d, c_d, d_d = stats.scores(obs_d, mod_d, minval=0.5, maxval=1.5)
    a_d, b_d, c_d, d_d = da.compute(a_d, b_d, c_d, d_d)
    assert a_d == 2 and b_d == 1 and c_d == 0 and d_d == 2
    del obs_d, mod_d, a_d, b_d, c_d, d_d
    gc.collect()

def test_CSI():
    obs = np.array([0, 1, 1, 0, 1])
    mod = np.array([0, 1, 0, 0, 1])
    result = stats.CSI(obs, mod, minval=0.5, maxval=1.5)
    # CSI = hits / (hits + misses + false alarms)
    # Actual output for this input: a=2, b=1, c=0
    assert np.isclose(result, 2 / (2 + 1 + 0))

    obs_d = to_dask(obs)
    mod_d = to_dask(mod)
    result_d = stats.CSI(obs_d, mod_d, minval=0.5, maxval=1.5)
    result_d = da.compute(result_d)[0]
    assert np.isclose(result_d, 2 / (2 + 1 + 0))
    del obs_d, mod_d, result_d
    gc.collect()

def test_HSS():
    obs = np.array([0, 1, 1, 0, 1])
    mod = np.array([0, 1, 0, 0, 1])
    result = stats.HSS(obs, mod, minval=0.5, maxval=1.5)
    # HSS should be between -1 and 1
    assert -1 <= result <= 1

    obs_d = to_dask(obs)
    mod_d = to_dask(mod)
    result_d = stats.HSS(obs_d, mod_d, minval=0.5, maxval=1.5)
    result_d = da.compute(result_d)[0]
    assert -1 <= result_d <= 1
    del obs_d, mod_d, result_d
    gc.collect()

def test_ETS():
    obs = np.array([0, 1, 1, 0, 1])
    mod = np.array([0, 1, 0, 0, 1])
    result = stats.ETS(obs, mod, minval=0.5, maxval=1.5)
    # ETS should be between -1/3 and 1
    assert -1/3 <= result <= 1

    obs_d = to_dask(obs)
    mod_d = to_dask(mod)
    result_d = stats.ETS(obs_d, mod_d, minval=0.5, maxval=1.5)
    result_d = da.compute(result_d)[0]
    assert -1/3 <= result_d <= 1
    del obs_d, mod_d, result_d
    gc.collect()
