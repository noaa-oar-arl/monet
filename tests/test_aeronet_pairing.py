import numpy as np
import pandas as pd
import pytest
import xarray as xr

from monet.util.combinetool import pair

# Ensure monetio and xregrid/esmpy are available for these tests
aeronet = pytest.importorskip("monetio.obs.aeronet")
pytest.importorskip("xregrid")
pytest.importorskip("esmpy")


def generate_mock_aeronet(filename, n_sites=1):
    header = [
        "AERONET Version 3",
        "AOD Level 1.5",
        "Contact: barry.baker@noaa.gov",
        "Site=Various, Lat=0.0, Lon=0.0, Elev=0.0",
        "Data_Format=Column_Names_In_Next_Line",
    ]

    dates = pd.date_range("2023-07-01", periods=5, freq="h")
    data = []
    for s in range(n_sites):
        site_name = f"Site_{s}"
        lat = 40.0 + s
        lon = -100.0 - s
        for i, d in enumerate(dates):
            data.append(
                {
                    "AERONET_Site": site_name,
                    "Date(dd:mm:yyyy)": d.strftime("%d:%m:%Y"),
                    "Time(hh:mm:ss)": d.strftime("%H:%M:%S"),
                    "Day_of_Year": d.dayofyear + d.hour / 24.0,
                    "Day_of_Year(Fraction)": d.dayofyear + d.hour / 24.0,
                    "AOD_1020nm": 0.1,
                    "AOD_870nm": 0.12,
                    "AOD_675nm": 0.15,
                    "AOD_500nm": 0.2,
                    "AOD_440nm": 0.25,
                    "440-870_Angstrom_Exponent": 1.5,
                    "Site_Latitude(degrees)": lat,
                    "Site_Longitude(degrees)": lon,
                    "Site_Elevation(m)": 100.0,
                }
            )
    df = pd.DataFrame(data)
    with open(filename, "w") as f:
        for line in header:
            f.write(line + "\n")
        df.to_csv(f, index=False)


@pytest.fixture
def aeronet_obs_multi(tmp_path):
    """Load AERONET data with multiple sites using a temporary file."""
    fn = tmp_path / "mock_aeronet_multi.txt"
    generate_mock_aeronet(str(fn), n_sites=3)
    df = aeronet.add_local(str(fn))
    return df


@pytest.fixture
def cf_grid():
    """Create a global CF convention grid."""
    lat = np.linspace(-90, 90, 18)
    lon = np.linspace(-180, 180, 36)
    data = np.random.rand(1, 18, 36)
    times = [pd.to_datetime("2023-07-01")]
    ds = xr.Dataset(
        {"model_aod": (("time", "latitude", "longitude"), data)}, coords={"time": times, "latitude": lat, "longitude": lon}
    )
    ds.latitude.attrs["standard_name"] = "latitude"
    ds.latitude.attrs["units"] = "degrees_north"
    ds.longitude.attrs["standard_name"] = "longitude"
    ds.longitude.attrs["units"] = "degrees_east"
    return ds


@pytest.fixture
def ugrid_grid():
    """Create a global UGRID convention grid."""
    n_nodes = 1000
    lat_nodes = np.random.uniform(-90, 90, n_nodes)
    lon_nodes = np.random.uniform(-180, 180, n_nodes)
    data = np.random.rand(1, n_nodes)
    times = [pd.to_datetime("2023-07-01")]
    ds = xr.Dataset(
        {"model_aod": (("time", "node"), data)},
        coords={"time": times, "latitude": (("node"), lat_nodes), "longitude": (("node"), lon_nodes)},
    )
    ds["mesh"] = xr.DataArray(
        0, attrs={"cf_role": "mesh_topology", "node_coordinates": "longitude latitude", "topology_dimension": 2}
    )
    ds.model_aod.attrs["mesh"] = "mesh"
    ds.latitude.attrs["standard_name"] = "latitude"
    ds.latitude.attrs["units"] = "degrees_north"
    ds.longitude.attrs["standard_name"] = "longitude"
    ds.longitude.attrs["units"] = "degrees_east"
    return ds


def test_pair_multi_sites(aeronet_obs_multi, cf_grid, ugrid_grid):
    """Test pairing multiple AERONET sites with both CF and UGRID grids."""
    # CF Grid
    paired_cf = pair(cf_grid, aeronet_obs_multi, method="nearest")
    assert isinstance(paired_cf, pd.DataFrame)
    assert len(paired_cf) == len(aeronet_obs_multi)
    assert "model_aod" in paired_cf.columns
    assert set(paired_cf.siteid.unique()) == {"Site_0", "Site_1", "Site_2"}

    # UGRID Grid
    paired_ugrid = pair(ugrid_grid, aeronet_obs_multi, method="nearest")
    assert isinstance(paired_ugrid, pd.DataFrame)
    assert len(paired_ugrid) == len(aeronet_obs_multi)
    assert "model_aod" in paired_ugrid.columns
    assert set(paired_ugrid.siteid.unique()) == {"Site_0", "Site_1", "Site_2"}


def test_pair_and_mask(aeronet_obs_multi, cf_grid):
    """Test pairing and then applying a mask to the result."""
    paired = pair(cf_grid, aeronet_obs_multi, method="nearest")

    # Apply giorgi mask
    # This should work without geopandas/rasterio because we have cached the mask in the repo
    masked = paired.monet.get_region("giorgi")

    assert "giorgi" in masked.columns
    assert not masked.giorgi.isnull().all()
    # Kansas (40, -100) is in Central North America (CNA)
    assert "CNA" in masked.giorgi.unique()

    # Apply land mask
    masked = paired.monet.is_land(return_xarray=True)
    # Kansas is land, so values should remain
    assert not masked.model_aod.isnull().all()
