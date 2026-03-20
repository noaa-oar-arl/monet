# Mocking xregrid/pytspack if not present to test logic flow
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from monet.util.combinetool import pair
from monet.util.resample import resample

try:
    import xregrid  # noqa: F401

    has_xregrid = True
except ImportError:
    has_xregrid = False

try:
    import dask.array as da
    import dask.dataframe as dd

    has_dask = True
except ImportError:
    has_dask = False


def test_resample_aero_protocol(monkeypatch):
    # Always mock for consistency in protocol testing
    import monet.accessors.base
    import monet.util.resample

    mock_regridder = MagicMock()
    mock_regridder.side_effect = lambda x: x  # Identity for testing
    monkeypatch.setattr(monet.util.resample, "Regridder", MagicMock(return_value=mock_regridder), raising=False)
    monkeypatch.setattr(monet.util.resample, "has_xregrid", True)
    monkeypatch.setattr(monet.accessors.base, "has_xregrid", True)

    """Verify resample follows Aero Protocol: NumPy and Dask consistency."""
    # Create source data
    nx, ny = 20, 10
    lon = np.linspace(0, 359, nx)
    lat = np.linspace(-90, 90, ny)
    lons, lats = np.meshgrid(lon, lat)
    data = np.random.rand(ny, nx)

    source = xr.Dataset({"var": (("y", "x"), data)}, coords={"latitude": (("y", "x"), lats), "longitude": (("y", "x"), lons)})

    # Create target grid
    target_lon = np.linspace(0, 359, 10)
    target_lat = np.linspace(-90, 90, 5)
    t_lons, t_lats = np.meshgrid(target_lon, target_lat)
    target = xr.Dataset(coords={"latitude": (("y", "x"), t_lats), "longitude": (("y", "x"), t_lons)})

    # Eager run
    out_eager = resample(source, target, method="nearest")
    assert isinstance(out_eager["var"].data, np.ndarray)
    assert "history" in out_eager.attrs

    # Lazy run
    if has_dask:
        source_lazy = source.chunk({"x": 5, "y": 5})
        out_lazy = resample(source_lazy, target, method="nearest")
        assert isinstance(out_lazy["var"].data, da.Array)

        # Compare results
        xr.testing.assert_allclose(out_eager, out_lazy.compute())


def test_pair_aero_protocol(monkeypatch):
    # Always mock for consistency in protocol testing
    import monet.accessors.base
    import monet.util.resample

    mock_regridder_cls = MagicMock()
    monkeypatch.setattr(monet.util.resample, "Regridder", mock_regridder_cls, raising=False)
    monkeypatch.setattr(monet.util.resample, "has_xregrid", True)
    monkeypatch.setattr(monet.accessors.base, "has_xregrid", True)

    mock_regridder = MagicMock()
    mock_regridder_cls.return_value = mock_regridder

    # Mock Regridder to return the source data mapped to target points
    def mock_apply(source):
        # Target was passed to Regridder(source, target, ...)
        target = mock_regridder_cls.call_args[0][1]
        # Create a result dataset with target's structure
        res = xr.Dataset(coords=target.coords)
        for var in source.data_vars:
            if not source[var].dims:
                res[var] = source[var]
                continue
            # Mock: mapping source to target.
            # Preserves non-spatial dimensions of source.
            preserved_dims = [d for d in source[var].dims if d not in ["x", "y", "lat", "lon", "node", "latitude", "longitude"]]
            # For this mock, we just take the first spatial point and broadcast to target spatial structure
            spatial_dims = [d for d in source[var].dims if d not in preserved_dims]
            indexers = {d: 0 for d in spatial_dims if d in source[var].dims}
            data = source[var].isel(indexers)
            # Now broadcast preserved dims with target
            res[var] = data.broadcast_like(target)
        return res

    mock_regridder.side_effect = mock_apply
    """Verify pair follows Aero Protocol: NumPy and Dask consistency."""
    # Create model data
    nx, ny = 10, 5
    lon = np.linspace(0, 359, nx)
    lat = np.linspace(-90, 90, ny)
    lons, lats = np.meshgrid(lon, lat)
    data = np.random.rand(ny, nx)
    model = xr.Dataset(
        {"temp": (("time", "y", "x"), data[None, ...])},
        coords={"time": [pd.to_datetime("2023-01-01")], "latitude": (("y", "x"), lats), "longitude": (("y", "x"), lons)},
    )

    # Create obs data (DataFrame)
    obs_df = pd.DataFrame(
        {
            "latitude": [0, 10, 20],
            "longitude": [0, 10, 20],
            "time": pd.to_datetime(["2023-01-01", "2023-01-01", "2023-01-01"]),
            "siteid": ["S1", "S2", "S3"],
            "obs_val": [1, 2, 3],
        }
    )

    # Eager run
    paired_eager = pair(model, obs_df, method="nearest")
    assert isinstance(paired_eager, pd.DataFrame)
    assert "temp" in paired_eager.columns

    # Lazy run (Dask model)
    if has_dask:
        model_lazy = model.chunk({"x": 5})
        paired_lazy = pair(model_lazy, obs_df, method="nearest")
        # Since model is dask, internal remapping is lazy, resulting in Dask DataFrame
        assert isinstance(paired_lazy, dd.DataFrame)

        # If obs is dask
        obs_dd = dd.from_pandas(obs_df, npartitions=1)
        paired_dd = pair(model_lazy, obs_dd, method="nearest")
        assert isinstance(paired_dd, dd.DataFrame)

        # Compare results
        res_lazy = paired_lazy.compute().reset_index(drop=True)[paired_eager.columns]
        # Ignore dtype mismatch for string columns as Dask/Pandas might differ in backend (pyarrow vs object)
        pd.testing.assert_frame_equal(paired_eager, res_lazy, check_dtype=False)


def test_ugrid_detection():
    """Verify UGRID detection logic."""
    from monet.accessors.base import BaseAccessor
    from monet.util.coards_tools import is_ugrid_compliant

    ds = xr.Dataset()
    ds["mesh"] = ((), 0)
    ds["mesh"].attrs["cf_role"] = "mesh_topology"

    assert is_ugrid_compliant(ds) is True

    ds_monet = BaseAccessor._dataset_to_monet(ds)
    assert ds_monet.attrs["mio_has_ugrid"] is True
    assert ds_monet.attrs["ugrid_mesh"] == "mesh"


def test_ugrid_pairing_smoke(monkeypatch):
    # Always mock for consistency in protocol testing
    import monet.accessors.base
    import monet.util.resample

    mock_regridder_cls = MagicMock()
    monkeypatch.setattr(monet.util.resample, "Regridder", mock_regridder_cls, raising=False)
    monkeypatch.setattr(monet.util.resample, "has_xregrid", True)
    monkeypatch.setattr(monet.accessors.base, "has_xregrid", True)

    mock_regridder = MagicMock()
    mock_regridder_cls.return_value = mock_regridder

    def mock_apply(source):
        target = mock_regridder_cls.call_args[0][1]
        res = xr.Dataset(coords=target.coords)
        for var in source.data_vars:
            if not source[var].dims:
                res[var] = source[var]
                continue
            preserved_dims = [d for d in source[var].dims if d not in ["x", "y", "lat", "lon", "node", "latitude", "longitude"]]
            spatial_dims = [d for d in source[var].dims if d not in preserved_dims]
            indexers = {d: 0 for d in spatial_dims if d in source[var].dims}
            data = source[var].isel(indexers)
            res[var] = data.broadcast_like(target)
        return res

    mock_regridder.side_effect = mock_apply
    """Smoke test for UGRID to DataFrame pairing."""
    # Create a minimal UGRID-like dataset
    n_nodes = 100
    lon = np.random.rand(n_nodes) * 360
    lat = np.random.rand(n_nodes) * 180 - 90
    data = np.random.rand(n_nodes)

    ds = xr.Dataset(
        {"temp": (("time", "node"), data[None, ...])},
        coords={"time": [pd.to_datetime("2023-01-01")], "lon": (("node",), lon), "lat": (("node",), lat)},
    )
    ds["mesh"] = ((), 0)
    ds["mesh"].attrs["cf_role"] = "mesh_topology"
    ds["mesh"].attrs["node_coordinates"] = "lon lat"

    obs_df = pd.DataFrame(
        {"latitude": [0, 45], "longitude": [0, 180], "siteid": ["A", "B"], "time": pd.to_datetime(["2023-01-01", "2023-01-01"])}
    )

    # This should now work without calling remap_nearest_unstructured
    paired = pair(ds, obs_df, method="nearest")
    assert "temp" in paired.columns
    assert len(paired) == 2


def test_apply_aero_numpy():
    """Verify _apply_aero works with NumPy/Scalars."""
    from monet.util.aero import _apply_aero

    def dummy_logic(x, y):
        return x + y

    res = _apply_aero(dummy_logic, 10, 5, name="test")
    assert res == 15


def test_apply_aero_xarray_lazy():
    """Verify _apply_aero works with Lazy (Dask) xarray objects."""
    pytest.importorskip("dask.array")

    from monet.util.aero import _apply_aero

    def dummy_logic(x, y):
        return x + y

    da1 = xr.DataArray(np.array([1, 2, 3]), dims="x", name="a").chunk({"x": 2})
    da2 = xr.DataArray(np.array([4, 5, 6]), dims="x", name="b").chunk({"x": 2})

    res = _apply_aero(dummy_logic, da1, da2, name="lazy_addition")

    assert isinstance(res, xr.DataArray)
    assert res.chunks is not None
    # Ensure it's still lazy
    assert hasattr(res.data, "dask")

    # Compute and verify
    assert np.array_equal(res.compute().values, [5, 7, 9])
    assert "lazy_addition" in res.attrs.get("history", "")
