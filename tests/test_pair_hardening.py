from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
import xarray as xr

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
    nx, ny = 5, 5
    data = np.random.rand(24, ny, nx)
    times = pd.date_range("2023-01-01", periods=24, freq="h")
    lats = np.linspace(30, 50, ny)
    lons = np.linspace(-120, -70, nx)

    ds = xr.Dataset(
        data_vars={"ozone": (("time", "y", "x"), data), "temp": (("time", "y", "x"), data + 10)},
        coords={
            "time": times,
            "latitude": (("y", "x"), np.meshgrid(lons, lats)[1]),
            "longitude": (("y", "x"), np.meshgrid(lons, lats)[0]),
        },
    )
    return ds


def test_pair_missing_time(sample_model):
    df = pd.DataFrame({"lat": [35.0], "lon": [-100.0], "obs": [1.0]})
    with pytest.raises(AttributeError, match="Could not detect 'time' column"):
        pair(sample_model, df)


def test_pair_missing_latlon(sample_model):
    df = pd.DataFrame({"time": [pd.Timestamp("2023-01-01")], "obs": [1.0]})
    # The error message comes from the accessor validation
    with pytest.raises(AttributeError, match="Must have latitude and longitude columns"):
        pair(sample_model, df)


def test_pair_suffix_application(sample_model):
    # Create obs df with same column names as model
    times = pd.date_range("2023-01-01", periods=2, freq="h")
    df = pd.DataFrame(
        {"time": times, "latitude": [35.0, 45.0], "longitude": [-100.0, -80.0], "ozone": [0.5, 0.6], "temp": [250.0, 260.0]}
    )

    with patch("monet.util.resample.resample") as mock_resample:
        # Mock resample output
        dummy_data = np.random.rand(2, 2)
        mock_resample.return_value = xr.Dataset(
            data_vars={"ozone": (("time", "x"), dummy_data), "temp": (("time", "x"), dummy_data + 10)},
            coords={"time": sample_model.time[:2], "x": [0, 1]},
        )

        result = pair(sample_model, df, suffix="_mod")

        assert "ozone" in result.columns  # Obs
        assert "ozone_mod" in result.columns  # Model
        assert "temp" in result.columns  # Obs
        assert "temp_mod" in result.columns  # Model


def test_pair_preserve_siteid(sample_model):
    times = pd.date_range("2023-01-01", periods=2, freq="h")
    df = pd.DataFrame(
        {
            "time": times,
            "latitude": [35.0, 45.0],
            "longitude": [-100.0, -80.0],
            "siteid": ["SiteA", "SiteB"],
            "extra_col": ["val1", "val2"],
        }
    )

    with patch("monet.util.resample.resample") as mock_resample:
        dummy_data = np.random.rand(2, 2)
        mock_resample.return_value = xr.Dataset(
            data_vars={"ozone": (("time", "x"), dummy_data)},
            coords={"time": sample_model.time[:2], "x": [0, 1], "siteid": (("x"), ["SiteA", "SiteB"])},
        )

        result = pair(sample_model, df)

        assert "siteid" in result.columns
        assert "extra_col" in result.columns
        assert result.siteid.tolist() == ["SiteA", "SiteB"]
        assert result.extra_col.tolist() == ["val1", "val2"]


def test_pair_empty_df(sample_model):
    df = pd.DataFrame(columns=["time", "latitude", "longitude", "obs"])
    # For an empty DataFrame, unique_locs will be empty.
    # We should expect a graceful failure or an informative error from downstream
    # if we try to remap to zero points.
    with patch("monet.util.resample.resample") as mock_resample:
        mock_resample.return_value = xr.Dataset()
        # ESMF/xregrid might raise ValueError for empty target coordinates
        with pytest.raises((ValueError, IndexError, AttributeError)):
            pair(sample_model, df)


def test_pair_single_point(sample_model):
    df = pd.DataFrame({"time": [pd.Timestamp("2023-01-01")], "latitude": [35.0], "longitude": [-100.0], "obs": [1.0]})

    with patch("monet.util.resample.resample") as mock_resample:
        dummy_data = np.array([[0.5]])
        mock_resample.return_value = xr.Dataset(
            data_vars={"ozone": (("time", "x"), dummy_data)}, coords={"time": [pd.Timestamp("2023-01-01")], "x": [0]}
        )

        result = pair(sample_model, df)
        assert len(result) == 1
        assert "ozone" in result.columns
        assert result.ozone.iloc[0] == 0.5
