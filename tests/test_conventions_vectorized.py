import numpy as np
import xarray as xr

from monet.util.coards_tools import is_curvilinear_grid, monet_to_coards
from monet.util.conventions import (
    detect_grid_type,
    find_coords,
    get_ugrid_coords,
    get_ugrid_info,
    update_history,
)


def test_rectilinear_detection():
    """Test detection of rectilinear grids with eager and lazy data."""
    # Eager
    lat = np.linspace(-90, 90, 18)
    lon = np.linspace(-180, 180, 36)
    data = np.random.rand(18, 36)
    ds = xr.Dataset({"data": (("lat", "lon"), data)}, coords={"lat": lat, "lon": lon})

    assert detect_grid_type(ds) == "rectilinear"
    assert find_coords(ds, "latitude").name == "lat"
    assert find_coords(ds, "longitude").name == "lon"

    # Lazy
    ds_lazy = ds.chunk({"lat": 5, "lon": 5})
    assert detect_grid_type(ds_lazy) == "rectilinear"
    assert find_coords(ds_lazy, "latitude").name == "lat"
    # Ensure it's still lazy
    assert hasattr(ds_lazy.data.data, "dask")


def test_curvilinear_detection():
    """Test detection of curvilinear grids with eager and lazy data."""
    lon_vals, lat_vals = np.meshgrid(np.linspace(-180, 180, 36), np.linspace(-90, 90, 18))
    data = np.random.rand(18, 36)
    ds = xr.Dataset({"data": (("y", "x"), data)}, coords={"latitude": (("y", "x"), lat_vals), "longitude": (("y", "x"), lon_vals)})

    assert detect_grid_type(ds) == "curvilinear"

    # Lazy
    ds_lazy = ds.chunk({"y": 5, "x": 5})
    assert detect_grid_type(ds_lazy) == "curvilinear"
    assert find_coords(ds_lazy, "latitude").name == "latitude"
    assert hasattr(ds_lazy.latitude.data, "dask")


def test_ugrid_detection():
    """Test detection and processing of UGRID unstructured meshes."""
    n_nodes = 100
    lat = np.random.uniform(-90, 90, n_nodes)
    lon = np.random.uniform(-180, 180, n_nodes)
    data = np.random.rand(n_nodes)
    ds = xr.Dataset({"data": (("node"), data)}, coords={"lat_node": (("node"), lat), "lon_node": (("node"), lon)})
    ds["mesh"] = xr.DataArray(
        0, attrs={"cf_role": "mesh_topology", "topology_dimension": 2, "node_coordinates": "lon_node lat_node"}
    )
    ds.data.attrs["mesh"] = "mesh"
    ds.data.attrs["location"] = "node"

    assert detect_grid_type(ds) == "unstructured"
    info = get_ugrid_info(ds)
    assert info["mesh_name"] == "mesh"
    assert "lat_node" in info["node_coordinates"]
    assert "lon_node" in info["node_coordinates"]

    # Get coords
    ulat, ulon = get_ugrid_coords(ds, ds.data)
    assert ulat.name == "lat_node"
    assert ulon.name == "lon_node"

    # Lazy
    ds_lazy = ds.chunk({"node": 20})
    assert detect_grid_type(ds_lazy) == "unstructured"
    ulat_l, ulon_l = get_ugrid_coords(ds_lazy, ds_lazy.data)
    assert hasattr(ulat_l.data, "dask")


def test_coards_to_netcdf_lazy():
    """Verify that COARDS to NetCDF conversion maintains laziness."""
    import dask.array as da

    from monet.accessors.base import BaseAccessor

    lat = np.linspace(-90, 90, 18)
    lon = np.linspace(-180, 180, 36)
    data = np.random.rand(18, 36)

    # Explicitly make coordinates lazy by using non-dimension names for coordinates
    data_lazy = da.from_array(data, chunks=(9, 18))
    ds_lazy = xr.Dataset({"data": (("y_dim", "x_dim"), data_lazy)})
    ds_lazy.coords["lat"] = (("y_dim"), da.from_array(lat, chunks=9))
    ds_lazy.coords["lon"] = (("x_dim"), da.from_array(lon, chunks=18))

    # Eager version
    ds_eager = xr.Dataset({"data": (("y_dim", "x_dim"), data)})
    ds_eager.coords["lat"] = (("y_dim"), lat)
    ds_eager.coords["lon"] = (("x_dim"), lon)

    # This should be lazy
    res = BaseAccessor._coards_to_netcdf(ds_lazy, lat_name="lat", lon_name="lon")

    assert "latitude" in res.coords
    assert "longitude" in res.coords
    assert res.latitude.dims == ("y", "x")

    # Verify laziness of the generated 2D coords
    assert hasattr(res.latitude.data, "dask")
    assert hasattr(res.longitude.data, "dask")

    # Compare with eager
    res_eager = BaseAccessor._coards_to_netcdf(ds_eager, lat_name="lat", lon_name="lon")
    xr.testing.assert_allclose(res.compute(), res_eager)


def test_update_history():
    """Verify history attribute updates."""
    ds = xr.Dataset()
    ds = update_history(ds, "Test message")
    assert "Test message" in ds.attrs["history"]

    # Double update
    ds = update_history(ds, "Second message")
    assert "Test message" in ds.attrs["history"]
    assert "Second message" in ds.attrs["history"]


def test_case_insensitive_find_coords():
    """Verify find_coords handles case sensitivity."""
    ds = xr.Dataset({"data": (("x"), [1])}, coords={"LAT": (("x"), [10]), "LON": (("x"), [20])})
    assert find_coords(ds, "latitude").name == "LAT"
    assert find_coords(ds, "longitude").name == "LON"


def test_non_spatial_dims():
    """Verify non-spatial dimension identification."""
    from monet.util.conventions import get_non_spatial_dims

    ds = xr.Dataset(
        {"data": (("time", "lev", "lat", "lon"), np.random.rand(2, 3, 4, 5))},
        coords={
            "time": [1, 2],
            "lev": [1, 2, 3],
            "lat": [10, 20, 30, 40],
            "lon": [100, 110, 120, 130, 140],
        },
    )

    non_spatial = get_non_spatial_dims(ds)
    assert "time" in non_spatial
    assert "lev" in non_spatial
    assert "lat" not in non_spatial
    assert "lon" not in non_spatial


def test_coards_tools_vectorized():
    """Verify coards_tools functions are Vectorized compliant and correct."""

    # 1. is_curvilinear_grid
    # Rectilinear
    lat = np.linspace(-90, 90, 18)
    lon = np.linspace(-180, 180, 36)
    lon_2d, lat_2d = np.meshgrid(lon, lat)
    # MONET convention uses y, x as dimension names for 2D grids
    ds_rect = xr.Dataset(
        {"data": (("y", "x"), np.random.rand(18, 36))},
        coords={"latitude": (("y", "x"), lat_2d), "longitude": (("y", "x"), lon_2d)},
    )
    assert not is_curvilinear_grid(ds_rect)

    # Curvilinear (add small perturbation)
    lat_curv = lat_2d.copy()
    lat_curv[0, 0] += 1.0
    ds_curv = xr.Dataset(
        {"data": (("y", "x"), np.random.rand(18, 36))},
        coords={"latitude": (("y", "x"), lat_curv), "longitude": (("y", "x"), lon_2d)},
    )
    assert is_curvilinear_grid(ds_curv)

    # Lazy Curvilinear
    ds_curv_lazy = ds_curv.chunk({"y": 5, "x": 5})
    assert is_curvilinear_grid(ds_curv_lazy)

    # 2. monet_to_coards
    # Should convert 2D rect to 1D lat/lon
    ds_coards = monet_to_coards(ds_rect)
    assert "lat" in ds_coards.coords
    assert ds_coards.lat.ndim == 1

    # Lazy version
    ds_rect_lazy = ds_rect.chunk({"y": 5, "x": 5})
    ds_coards_lazy = monet_to_coards(ds_rect_lazy)
    assert "lat" in ds_coards_lazy.coords
    assert hasattr(ds_coards_lazy.lat.data, "dask")
