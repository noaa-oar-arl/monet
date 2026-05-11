from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
import xarray as xr

try:
    import dask.array as da
    import dask.dataframe as dd

    has_dask = True
except ImportError:
    has_dask = False

from monet.util.combinetool import pair


@pytest.fixture(autouse=True)
def mock_regridding_available(monkeypatch):
    """Ensure regridding backend is seen as available for tests that use mocks."""
    import monet.accessors.base
    import monet.util.resample

    monkeypatch.setattr(monet.accessors.base, "has_xregrid", True)
    monkeypatch.setattr(monet.util.resample, "has_xregrid", True)


@pytest.fixture
def sample_model():
    nx, ny = 10, 10
    data = np.random.rand(24, ny, nx)
    times = pd.date_range("2023-01-01", periods=24, freq="h")
    lats = np.linspace(30, 50, ny)
    lons = np.linspace(-120, -70, nx)

    ds = xr.Dataset(
        data_vars={"ozone": (("time", "y", "x"), data)},
        coords={
            "time": times,
            "latitude": (("y", "x"), np.meshgrid(lons, lats)[1]),
            "longitude": (("y", "x"), np.meshgrid(lons, lats)[0]),
        },
    )
    return ds


@pytest.fixture
def sample_obs_df():
    times = pd.date_range("2023-01-01", periods=24, freq="h")
    df = pd.DataFrame(
        {
            "time": np.repeat(times, 2),
            "siteid": ["A", "B"] * 24,
            "latitude": [35.0, 45.0] * 24,
            "longitude": [-100.0, -80.0] * 24,
            "obs_val": np.random.rand(48),
        }
    )
    return df


@pytest.fixture
def sample_obs_ds(sample_obs_df):
    return sample_obs_df.set_index(["time", "siteid"]).to_xarray()


def test_pair_xarray_eager(sample_model, sample_obs_ds):
    with patch("monet.util.resample.resample") as mock_resample:
        # Create a dummy result from resample
        # It should have the same dimensions as the target (time, x)
        dummy_data = np.random.rand(24, 2)
        mock_resample.return_value = xr.Dataset(
            data_vars={"ozone": (("time", "x"), dummy_data)},
            coords={"time": sample_obs_ds.time, "x": [0, 1], "siteid": (("x"), ["A", "B"])},
        )

        result = pair(sample_model, sample_obs_ds)

        assert isinstance(result, xr.Dataset)
        assert "ozone" in result.data_vars
        assert "obs_val" in result.data_vars
        assert result.ozone.shape == (24, 2)
        mock_resample.assert_called_once()


@pytest.mark.skipif(not has_dask, reason="dask not installed")
def test_pair_xarray_lazy(sample_model, sample_obs_ds):
    # Make model lazy
    sample_model = sample_model.chunk({"time": 6})

    with patch("monet.util.resample.resample") as mock_resample:
        # Create a dummy lazy result
        dummy_data = da.random.random((24, 2), chunks=(6, 2))
        mock_resample.return_value = xr.Dataset(
            data_vars={"ozone": (("time", "x"), dummy_data)}, coords={"time": sample_obs_ds.time, "x": [0, 1]}
        )

        result = pair(sample_model, sample_obs_ds)

        assert isinstance(result, xr.Dataset)
        assert result.ozone.chunks is not None
        assert result.ozone.chunks[0][0] == 6


def test_pair_pandas_eager(sample_model, sample_obs_df):
    with patch("monet.util.resample.resample") as mock_resample:
        # Resample is called for unique locations (2 sites)
        dummy_data = np.random.rand(24, 2)
        mock_resample.return_value = xr.Dataset(
            data_vars={"ozone": (("time", "x"), dummy_data)},
            coords={"time": sample_model.time, "x": [0, 1], "siteid": (("x"), ["A", "B"])},
        )

        result = pair(sample_model, sample_obs_df)

        assert isinstance(result, pd.DataFrame)
        assert "ozone" in result.columns
        assert len(result) == len(sample_obs_df)


@pytest.mark.skipif(not has_dask, reason="dask not installed")
def test_pair_dask_lazy(sample_model, sample_obs_df):
    # Make model lazy
    sample_model = sample_model.chunk({"time": 6})
    # Make obs lazy
    obs_ddf = dd.from_pandas(sample_obs_df, npartitions=2)

    with patch("monet.util.resample.resample") as mock_resample:
        dummy_data = da.random.random((24, 2), chunks=(6, 2))
        mock_resample.return_value = xr.Dataset(
            data_vars={"ozone": (("time", "x"), dummy_data)},
            coords={"time": sample_model.time, "x": [0, 1], "siteid": (("x"), ["A", "B"])},
        )

        result = pair(sample_model, obs_ddf)

        assert isinstance(result, dd.DataFrame)
        # Check that it's still lazy
        assert "ozone" in result.columns
        computed = result.compute()
        assert len(computed) == len(sample_obs_df)
        assert not computed.ozone.isnull().all()


def test_accessor_pair(sample_model, sample_obs_df):
    with patch("monet.util.resample.resample") as mock_resample:
        dummy_data = np.random.rand(24, 2)
        mock_resample.return_value = xr.Dataset(
            data_vars={"ozone": (("time", "x"), dummy_data)},
            coords={"time": sample_model.time, "x": [0, 1], "siteid": (("x"), ["A", "B"])},
        )

        # Test DataArray accessor
        res1 = sample_model.ozone.monet.pair(sample_obs_df)
        assert isinstance(res1, pd.DataFrame)

        # Test Dataset accessor
        res2 = sample_model.monet.pair(sample_obs_df)
        assert isinstance(res2, pd.DataFrame)

        # Test Pandas accessor
        res3 = sample_obs_df.monet.pair(sample_model)
        assert isinstance(res3, pd.DataFrame)


def test_pair_gridded_to_gridded(sample_model):
    # Create another gridded dataset (obs)
    nx, ny = 5, 5
    data = np.random.rand(24, ny, nx)
    times = pd.date_range("2023-01-01", periods=24, freq="h")
    lats = np.linspace(35, 45, ny)
    lons = np.linspace(-110, -90, nx)

    obs_gridded = xr.Dataset(
        data_vars={"obs_ozone": (("time", "y", "x"), data)},
        coords={
            "time": times,
            "latitude": (("y", "x"), np.meshgrid(lons, lats)[1]),
            "longitude": (("y", "x"), np.meshgrid(lons, lats)[0]),
        },
    )

    # Pair gridded to gridded
    with patch("monet.util.resample.resample") as mock_resample:
        # Resample should return data on the obs grid
        dummy_data = np.random.rand(24, ny, nx)
        mock_resample.return_value = xr.Dataset(data_vars={"ozone": (("time", "y", "x"), dummy_data)}, coords=obs_gridded.coords)

        result = pair(sample_model, obs_gridded)

        assert isinstance(result, xr.Dataset)
        assert "ozone" in result.data_vars
        assert "obs_ozone" in result.data_vars
        assert result.ozone.shape == (24, 5, 5)


def test_pair_pandas_obs_lazy_model(sample_model, sample_obs_df):
    if not has_dask:
        pytest.skip("dask not installed")
    # Make model lazy
    sample_model = sample_model.chunk({"time": 6})

    with patch("monet.util.resample.resample") as mock_resample:
        dummy_data = da.random.random((24, 2), chunks=(6, 2))
        mock_resample.return_value = xr.Dataset(
            data_vars={"ozone": (("time", "x"), dummy_data)},
            coords={"time": sample_model.time, "x": [0, 1], "siteid": (("x"), ["A", "B"])},
        )

        result = pair(sample_model, sample_obs_df)

        # Result should be dask because model was dask
        assert isinstance(result, dd.DataFrame)
        assert "ozone" in result.columns
        assert len(result.compute()) == len(sample_obs_df)


def test_pair_dataframe_interp_time(sample_model, sample_obs_df):
    with patch("monet.util.resample.resample") as mock_resample:
        dummy_data = np.random.rand(24, 2)
        mock_resample.return_value = xr.Dataset(
            data_vars={"ozone": (("time", "x"), dummy_data)},
            coords={"time": sample_model.time, "x": [0, 1], "siteid": (("x"), ["A", "B"])},
        )

        # Request time interpolation
        result = pair(sample_model, sample_obs_df, interp_time=True)

        assert isinstance(result, pd.DataFrame)
        assert "ozone" in result.columns
        # resample should have been called, and then interp
        mock_resample.assert_called_once()
