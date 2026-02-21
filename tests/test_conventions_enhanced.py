import numpy as np
import pandas as pd
import xarray as xr

from monet.util.tools import get_giorgi_region_df


def test_ugrid_face_coordinates_detection():
    # Create a UGRID dataset with face_coordinates
    ds = xr.Dataset()
    ds["mesh"] = xr.DataArray(
        0, attrs={"cf_role": "mesh_topology", "face_coordinates": "face_lon face_lat", "node_coordinates": "node_lon node_lat"}
    )
    ds["face_lat"] = xr.DataArray([0.0, 10.0], dims="face")
    ds["face_lon"] = xr.DataArray([0.0, 20.0], dims="face")
    ds["node_lat"] = xr.DataArray([0.0, 5.0, 10.0], dims="node")
    ds["node_lon"] = xr.DataArray([0.0, 5.0, 10.0], dims="node")

    # DataArray on faces
    da = xr.DataArray(np.random.rand(2), dims="face", name="test_var")
    ds["test_var"] = da

    # Check detection on Dataset
    lat_name, lon_name = ds.monet._detect_latlon_names(ds)
    # Since node_coordinates comes first in our search order in base.py?
    # Wait, I used: for attr in ["node_coordinates", "face_coordinates", "edge_coordinates"]:
    assert lat_name == "node_lat"
    assert lon_name == "node_lon"

    # If we only have face_coordinates
    ds2 = xr.Dataset()
    ds2["mesh"] = xr.DataArray(
        0,
        attrs={
            "cf_role": "mesh_topology",
            "face_coordinates": "face_lon face_lat",
        },
    )
    ds2["face_lat"] = xr.DataArray([0.0, 10.0], dims="face")
    ds2["face_lon"] = xr.DataArray([0.0, 20.0], dims="face")

    lat_name, lon_name = ds2.monet._detect_latlon_names(ds2)
    assert lat_name == "face_lat"
    assert lon_name == "face_lon"


def test_ugrid_dataarray_mesh_attr():
    ds = xr.Dataset()
    ds["mesh"] = xr.DataArray(0, attrs={"cf_role": "mesh_topology", "node_coordinates": "node_lon node_lat"})
    ds["node_lat"] = xr.DataArray([0.0, 10.0], dims="node")
    ds["node_lon"] = xr.DataArray([0.0, 20.0], dims="node")
    ds["test_var"] = xr.DataArray([1.0, 2.0], dims="node", attrs={"mesh": "mesh"})

    da = ds.test_var
    # Even if da doesn't have the topology variable, it has the 'mesh' attribute
    # In my improved _detect_ugrid, it returns "mesh"
    # But _detect_latlon_names needs to find the topology variable to read its attributes.
    # If it's a DataArray, it doesn't have other variables unless they are coordinates.

    # If we add node_lat/lon as coordinates to the DataArray
    da = da.assign_coords(node_lat=ds.node_lat, node_lon=ds.node_lon)

    # My improved _detect_ugrid for DataArray:
    # if "mesh" in ds.attrs: return ds.attrs["mesh"]
    # But ds[mesh_var] will fail if ds is a DataArray and mesh_var is not in coords.

    # Let's see what happens.
    lat_name, lon_name = da.monet._detect_latlon_names(da)
    # It should fall back to LAT_NAMES search in coords if it can't find topology
    assert lat_name == "node_lat"
    assert lon_name == "node_lon"


def test_cf_units_detection():
    ds = xr.Dataset()
    ds["my_lat"] = xr.DataArray([0.0, 10.0], dims="lat", attrs={"units": "degrees_north"})
    ds["my_lon"] = xr.DataArray([0.0, 20.0], dims="lon", attrs={"units": "degrees_east"})
    ds["test_var"] = xr.DataArray([[1.0, 2.0], [3.0, 4.0]], dims=("lat", "lon"))

    lat_name, lon_name = ds.monet._detect_latlon_names(ds)
    assert lat_name == "my_lat"
    assert lon_name == "my_lon"


def test_giorgi_region_non_standard_names():
    ds = xr.Dataset()
    ds["my_lat"] = xr.DataArray([0.0, 10.0], dims="lat", attrs={"units": "degrees_north"})
    ds["my_lon"] = xr.DataArray([0.0, 20.0], dims="lon", attrs={"units": "degrees_east"})
    ds["test_var"] = xr.DataArray([[1.0, 2.0], [3.0, 4.0]], dims=("lat", "lon"))

    # This should now work because get_giorgi_region_df uses the accessor
    ds_out = get_giorgi_region_df(ds)
    assert "GIORGI_INDEX" in ds_out.variables
    assert "GIORGI_ACRO" in ds_out.variables


def test_dataframe_convention_awareness():
    df = pd.DataFrame({"LAT": [40.0], "LON": [-80.0], "obs": [1.0]})
    # get_giorgi_region_df should work with "LAT"/"LON"
    df_out = get_giorgi_region_df(df)
    assert "GIORGI_INDEX" in df_out.columns
    assert df_out.iloc[0]["GIORGI_ACRO"] == "ENA"
