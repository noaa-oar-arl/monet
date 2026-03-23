import numpy as np
import pytest

from monet.util.stats import MAE, MAPE, MSE, SMAPE, scores


def test_scores():
    # Due to our low bias, we miss one
    obs = np.linspace(0, 1, 21)
    mod = obs - 0.1  # low bias
    a, b, c, d = scores(obs, mod, minval=0.5)
    assert a == 9 and b == 1 and c == 0 and d == 11

    # All good
    a, b, c, d = scores([1, 1], [1, 1], minval=0)
    assert a == 2 and b == 0 and c == 0 and d == 0

    # All miss
    a, b, c, d = scores([1, 1], [-1, -1], minval=0)
    assert a == 0 and b == 2 and c == 0 and d == 0

    # All false alarm
    a, b, c, d = scores([-1, -1], [1, 1], minval=0)
    assert a == 0 and b == 0 and c == 2 and d == 0

    # All correct negative
    a, b, c, d = scores([-1, -1], [-1, -1], minval=0)
    assert a == 0 and b == 0 and c == 0 and d == 2

    # Same but mix
    a, b, c, d = scores([-1, 1], [-1, 1], minval=0)
    assert a == 1 and b == 0 and c == 0 and d == 1
    a, b, c, d = scores([1, -1], [1, -1], minval=0)
    assert a == 1 and b == 0 and c == 0 and d == 1

    # Opposite
    a, b, c, d = scores([1, -1], [-1, 1], minval=0)
    assert a == 0 and b == 1 and c == 1 and d == 0
    a, b, c, d = scores([-1, 1], [1, -1], minval=0)
    assert a == 0 and b == 1 and c == 1 and d == 0

    # No pairs
    a, b, c, d = scores([np.nan, np.nan], [np.nan, np.nan], minval=0)
    assert a == 0 and b == 0 and c == 0 and d == 0
    a, b, c, d = scores([np.nan, 1], [1, np.nan], minval=0)
    assert a == 0 and b == 0 and c == 0 and d == 0

    # Some pairs after NaN dropping
    a, b, c, d = scores([np.nan, 1], [1, 1], minval=0)
    assert a == 1 and b == 0 and c == 0 and d == 0


def test_mae_basic():
    obs = np.array([1.0, 3.0, 5.0])
    mod = np.array([2.0, 1.0, 9.0])
    # Absolute errors are [1, 2, 4], mean is 7/3.
    assert np.isclose(MAE(obs, mod), 7.0 / 3.0)


@pytest.mark.parametrize("invalid", [np.nan, np.inf, -np.inf])
def test_mae_obs_invalid(invalid):
    obs = np.array([invalid, 2.0, 3.0])
    mod = np.array([1.0, 1.0, 1.0])
    assert np.isclose(MAE(obs, mod), 1.5)


def test_mse_basic():
    obs = np.array([1.0, 3.0, 5.0])
    mod = np.array([2.0, 1.0, 9.0])
    # Squared errors are [1, 4, 16], mean is 7.
    assert np.isclose(MSE(obs, mod), 7.0)


@pytest.mark.parametrize("invalid", [np.nan, np.inf, -np.inf])
def test_mse_obs_invalid(invalid):
    obs = np.array([invalid, 2.0, 3.0])
    mod = np.array([1.0, 1.0, 1.0])
    assert np.isclose(MSE(obs, mod), 2.5)


def test_mape_masks_zero_observations():
    obs = np.array([0.0, 2.0])
    mod = np.array([5.0, 1.0])

    expected = 50.0
    # First element is undefined for MAPE and should be excluded.
    out = MAPE(obs, mod)
    assert np.isclose(out, expected)

    # With the divide-with-where method, the fill for where obs is 0 is undefined
    # (could be inf, could be 0, could be something else)
    # "Note that if an uninitialized out array is created via the default out=None,
    # locations within it where the condition is False will remain uninitialized."
    # Here we set it to be 0, but the original method did not set it.
    # You could also use NaN and then take nanmean, then the result would be correct.
    old = np.ma.mean(
        np.ma.abs(
            np.divide(
                obs - mod,
                obs,
                where=obs != 0,
                out=np.zeros_like(obs),
            ),
        )
        * 100,
    )
    assert np.isclose(old, expected / 2.0)


@pytest.mark.parametrize("invalid", [np.nan, np.inf, -np.inf])
def test_mape_obs_invalid(invalid):
    obs = np.array([invalid, 2.0])
    mod = np.array([5.0, 1.0])
    assert np.isclose(MAPE(obs, mod), 50.0)


def test_smape_masks_both_zero():
    obs = np.array([0.0, 2.0])
    mod = np.array([0.0, 1.0])

    # For the valid pair, sMAPE = 2*|2-1|/(|2|+|1|)*100 = 66.666...
    expected = 200.0 / 3.0
    out = SMAPE(obs, mod)
    assert np.isclose(out, expected)

    # divide-with-where alone does not mask invalid denominator points.
    # If those points are pre-filled (here with 0), they still get averaged in.
    denom = np.abs(obs) + np.abs(mod)
    frac_divide_where = np.divide(
        2 * np.abs(obs - mod),
        denom,
        where=denom != 0,
        out=np.zeros_like(denom),
    )
    wrong = np.mean(frac_divide_where * 100)
    assert np.isclose(wrong, expected / 2.0)
    assert not np.isclose(wrong, out)


@pytest.mark.parametrize("invalid", [np.nan, np.inf, -np.inf])
def test_smape_obs_invalid(invalid):
    obs = np.array([invalid, 2.0])
    mod = np.array([0.0, 1.0])
    assert np.isclose(SMAPE(obs, mod), 200.0 / 3.0)


@pytest.mark.parametrize("func", [MAE, MSE, MAPE, SMAPE])
def test_all_invalid_returns_nan(func):
    obs = np.array([np.nan, np.nan])
    mod = np.array([1.0, 2.0])
    result = func(obs, mod)
    assert np.isnan(result)
    assert np.isscalar(result), "should be scalar, not 0-d or 1-el array"


@pytest.mark.parametrize(
    ("func", "obs", "mod", "expected"),
    [
        (MAE, [1.0, 3.0, 5.0], [2.0, 1.0, 9.0], 7.0 / 3.0),
        (MSE, [1.0, 3.0, 5.0], [2.0, 1.0, 9.0], 7.0),
        (MAPE, [0.0, 2.0], [5.0, 1.0], 50.0),
        (SMAPE, [0.0, 2.0], [0.0, 1.0], 200.0 / 3.0),
    ],
)
def test_accepts_array_like_inputs(func, obs, mod, expected):
    assert np.isclose(func(obs, mod), expected)
