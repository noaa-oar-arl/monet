"""Masking and region utilities for MONET."""

import datetime
import io
import os
import typing as t
import zipfile

import numpy as np
import requests
import xarray as xr

# Optional dependencies for building masks
try:
    import geopandas as gpd
    import rasterio
    from rasterio import features
    from shapely.geometry import box

    HAS_REGIONS_DEPS = True
except ImportError:
    HAS_REGIONS_DEPS = False

# Default cache directory for MONET masks
MONET_CACHE_DIR = os.path.join(os.path.expanduser("~"), ".monet", "masks")


class RegionDefinitions:
    """Definitions and downloaders for various region datasets."""

    @staticmethod
    def get_giorgi_regions() -> t.Any:
        """Returns the 21 standard Giorgi (IPCC) box regions.

        Returns
        -------
        geopandas.GeoDataFrame
            GeoDataFrame containing Giorgi regions.
        """
        if not HAS_REGIONS_DEPS:
            raise ImportError("geopandas and shapely are required for get_giorgi_regions. Install 'monet[regions]'.")

        regions = {
            "AUS": (110, 155, -45, -11),
            "AMZ": (-80, -50, -20, 12),
            "CAM": (-118, -83, 10, 30),
            "CAS": (40, 75, 30, 50),
            "CNA": (-105, -85, 30, 50),
            "EAF": (22, 52, -12, 18),
            "EAS": (100, 145, 20, 50),
            "ENA": (-85, -60, 25, 50),
            "MED": (-10, 40, 30, 48),
            "NAS": (40, 180, 50, 70),
            "NEU": (-10, 40, 48, 75),
            "SAF": (10, 52, -35, -12),
            "SAH": (-20, 40, 18, 30),
            "SAS": (65, 100, 5, 30),
            "SEA": (95, 155, -11, 20),
            "SSA": (-60, -20, -56, -20),
            "TIB": (75, 100, 30, 50),
            "WAF": (-20, 22, -12, 18),
            "WNA": (-130, -105, 30, 60),
        }
        geoms = [box(minx, miny, maxx, maxy) for minx, maxx, miny, maxy in regions.values()]
        return gpd.GeoDataFrame({"region": list(regions.keys()), "geometry": geoms}, crs="EPSG:4326")

    @staticmethod
    def download_ipcc_ar6(output_dir: str = MONET_CACHE_DIR) -> str | None:
        """Downloads IPCC AR6 Reference Regions (v4) polygons.

        Parameters
        ----------
        output_dir : str, optional
            Directory to save the data.

        Returns
        -------
        str or None
            Path to the downloaded shapefile.
        """
        url = "https://github.com/SantanderMetGroup/ATLAS/raw/main/reference-regions/IPCC-WGI-reference-regions-v4.zip"
        return RegionDefinitions._fetch_shapefile(
            url, "IPCC-WGI-reference-regions-v4.zip", "IPCC-WGI-reference-regions-v4.shp", output_dir
        )

    @staticmethod
    def download_epa_ecoregions(output_dir: str = MONET_CACHE_DIR) -> str | None:
        """Downloads US EPA Level 2 Ecoregions.

        Parameters
        ----------
        output_dir : str, optional
            Directory to save the data.

        Returns
        -------
        str or None
            Path to the downloaded shapefile.
        """
        url = "https://gaftp.epa.gov/EPADataCommons/ORD/Ecoregions/cec_na/na_cec_eco_l2.zip"
        return RegionDefinitions._fetch_shapefile(url, "na_cec_eco_l2.zip", "NA_CEC_Eco_Level2.shp", output_dir)

    @staticmethod
    def download_timezones(output_dir: str = MONET_CACHE_DIR) -> str | None:
        """Downloads Natural Earth Time Zones.

        Parameters
        ----------
        output_dir : str, optional
            Directory to save the data.

        Returns
        -------
        str or None
            Path to the downloaded shapefile.
        """
        url = "https://naciscdn.org/naturalearth/10m/cultural/ne_10m_time_zones.zip"
        return RegionDefinitions._fetch_shapefile(url, "ne_10m_time_zones.zip", "ne_10m_time_zones.shp", output_dir)

    @staticmethod
    def get_epa_admin_regions(output_dir: str = MONET_CACHE_DIR) -> t.Any:
        """Generates EPA Administrative Regions (1-10).

        Parameters
        ----------
        output_dir : str, optional
            Directory to save the data.

        Returns
        -------
        geopandas.GeoDataFrame
            GeoDataFrame containing EPA Administrative Regions.
        """
        if not HAS_REGIONS_DEPS:
            raise ImportError("geopandas is required for get_epa_admin_regions. Install 'monet[regions]'.")

        url = "https://naciscdn.org/naturalearth/10m/cultural/ne_10m_admin_1_states_provinces.zip"
        shp_path = RegionDefinitions._fetch_shapefile(
            url, "ne_10m_admin_1_states_provinces.zip", "ne_10m_admin_1_states_provinces.shp", output_dir
        )
        if not shp_path:
            return None

        gdf = gpd.read_file(shp_path)
        gdf = gdf[gdf["admin"] == "United States of America"].copy()

        epa_map = {
            "Connecticut": "EPA_01",
            "Maine": "EPA_01",
            "Massachusetts": "EPA_01",
            "New Hampshire": "EPA_01",
            "Rhode Island": "EPA_01",
            "Vermont": "EPA_01",
            "New Jersey": "EPA_02",
            "New York": "EPA_02",
            "Puerto Rico": "EPA_02",
            "United States Virgin Islands": "EPA_02",
            "Delaware": "EPA_03",
            "District of Columbia": "EPA_03",
            "Maryland": "EPA_03",
            "Pennsylvania": "EPA_03",
            "Virginia": "EPA_03",
            "West Virginia": "EPA_03",
            "Alabama": "EPA_04",
            "Florida": "EPA_04",
            "Georgia": "EPA_04",
            "Kentucky": "EPA_04",
            "Mississippi": "EPA_04",
            "North Carolina": "EPA_04",
            "South Carolina": "EPA_04",
            "Tennessee": "EPA_04",
            "Illinois": "EPA_05",
            "Indiana": "EPA_05",
            "Michigan": "EPA_05",
            "Minnesota": "EPA_05",
            "Ohio": "EPA_05",
            "Wisconsin": "EPA_05",
            "Arkansas": "EPA_06",
            "Louisiana": "EPA_06",
            "New Mexico": "EPA_06",
            "Oklahoma": "EPA_06",
            "Texas": "EPA_06",
            "Iowa": "EPA_07",
            "Kansas": "EPA_07",
            "Missouri": "EPA_07",
            "Nebraska": "EPA_07",
            "Colorado": "EPA_08",
            "Montana": "EPA_08",
            "North Dakota": "EPA_08",
            "South Dakota": "EPA_08",
            "Utah": "EPA_08",
            "Wyoming": "EPA_08",
            "Arizona": "EPA_09",
            "California": "EPA_09",
            "Hawaii": "EPA_09",
            "Nevada": "EPA_09",
            "Guam": "EPA_09",
            "American Samoa": "EPA_09",
            "Northern Mariana Islands": "EPA_09",
            "Alaska": "EPA_10",
            "Idaho": "EPA_10",
            "Oregon": "EPA_10",
            "Washington": "EPA_10",
        }
        gdf["epa_region"] = gdf["name"].map(epa_map)
        return gdf.dropna(subset=["epa_region"])

    @staticmethod
    def download_land(output_dir: str = MONET_CACHE_DIR) -> str | None:
        """Downloads Natural Earth Land Polygons (110m).

        Parameters
        ----------
        output_dir : str, optional
            Directory to save the data.

        Returns
        -------
        str or None
            Path to the downloaded shapefile.
        """
        url = "https://naciscdn.org/naturalearth/110m/physical/ne_110m_land.zip"
        return RegionDefinitions._fetch_shapefile(url, "ne_110m_land.zip", "ne_110m_land.shp", output_dir)

    @staticmethod
    def _fetch_shapefile(url: str, zip_name: str, shp_name: str, output_dir: str) -> str | None:
        """Internal helper to fetch and unzip shapefiles."""
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        shp_path = os.path.join(output_dir, shp_name)
        if os.path.exists(shp_path):
            return shp_path

        print(f"Downloading {url}...")
        try:
            r = requests.get(url, stream=True)
            r.raise_for_status()
            z = zipfile.ZipFile(io.BytesIO(r.content))
            z.extractall(output_dir)
            print("Download complete.")
            return shp_path
        except Exception as e:
            print(f"Error downloading {url}: {e}")
            return None


class MaskBuilder:
    """Builder for generating raster masks from polygons."""

    def __init__(self, resolution: float = 0.05):
        """Initialize MaskBuilder.

        Parameters
        ----------
        resolution : float, default: 0.05
            Resolution of the mask in degrees.
        """
        self.resolution = resolution

    def build(self, gdf: t.Any, output_path: str, region_column: str) -> None:
        """Build a raster mask from a GeoDataFrame.

        Parameters
        ----------
        gdf : geopandas.GeoDataFrame
            Input polygons.
        output_path : str
            Path to save the compressed .npz mask.
        region_column : str
            Name of the column containing region identifiers.
        """
        if not HAS_REGIONS_DEPS:
            raise ImportError("geopandas and rasterio are required for MaskBuilder. Install 'monet[regions]'.")

        print(f"Building mask for {len(gdf)} regions into {output_path}...")
        if gdf.crs != "EPSG:4326":
            gdf = gdf.to_crs("EPSG:4326")

        unique_regions = gdf[region_column].unique()
        name_to_id = {name: i + 1 for i, name in enumerate(unique_regions)}
        id_to_name = {v: k for k, v in name_to_id.items()}
        gdf["mask_id"] = gdf[region_column].map(name_to_id)

        lat_min, lat_max, lon_min, lon_max = -90, 90, -180, 180
        height = int((lat_max - lat_min) / self.resolution)
        width = int((lon_max - lon_min) / self.resolution)
        transform = rasterio.transform.from_bounds(lon_min, lat_min, lon_max, lat_max, width, height)

        mask_arr = features.rasterize(
            shapes=((geom, val) for geom, val in zip(gdf.geometry, gdf["mask_id"])),
            out_shape=(height, width),
            transform=transform,
            fill=0,
            dtype=np.uint16,
        )
        np.savez_compressed(
            output_path, mask=mask_arr, lat_min=lat_min, lon_min=lon_min, resolution=self.resolution, region_map=id_to_name
        )
        print("Done.")


class EarthMask:
    """Reader for pre-computed masks with Xarray/Dask support."""

    def __init__(self, npz_path: str):
        """Initialize EarthMask.

        Parameters
        ----------
        npz_path : str
            Path to the compressed .npz mask.
        """
        data = np.load(npz_path, allow_pickle=True)
        self.mask = data["mask"]
        self.lat_min = float(data["lat_min"])
        self.lon_min = float(data["lon_min"])
        self.resolution = float(data["resolution"])
        self.region_map = data["region_map"].item()
        self.height, self.width = self.mask.shape

    def query(self, lat: t.Any, lon: t.Any) -> t.Any:
        """Query the mask for given latitude and longitude.

        Supports both scalar and array inputs. Handles longitude wrapping.

        Parameters
        ----------
        lat : array-like or float
            Latitude values.
        lon : array-like or float
            Longitude values.

        Returns
        -------
        array-like or object
            Region names at the given coordinates.
        """
        lat_arr = np.asarray(lat)
        lon_arr = np.asarray(lon)

        # Wrap longitudes to [-180, 180)
        lon_arr = (lon_arr + 180) % 360 - 180

        lat_idx = self.height - 1 - ((lat_arr - self.lat_min) / self.resolution).astype(int)
        lon_idx = ((lon_arr - self.lon_min) / self.resolution).astype(int)

        safe_lat = np.clip(lat_idx, 0, self.height - 1)
        safe_lon = np.clip(lon_idx, 0, self.width - 1)
        ids = self.mask[safe_lat, safe_lon]

        # Use a vectorized lookup
        if ids.ndim == 0:
            return self.region_map.get(ids.item(), None)

        lookup = np.vectorize(lambda i: self.region_map.get(i, None), otypes=[object])
        return lookup(ids)


def get_mask(mask_name: str, resolution: float = 0.05) -> EarthMask:
    """Get an EarthMask by name, building it if it doesn't exist.

    Parameters
    ----------
    mask_name : str
        Name of the mask (e.g., 'giorgi', 'ipcc_ar6', 'epa_eco', 'timezones', 'epa_admin', 'land').
    resolution : float, default: 0.05
        Resolution to use if building the mask.

    Returns
    -------
    EarthMask
        The requested mask.
    """
    mask_path = os.path.join(MONET_CACHE_DIR, f"{mask_name}_{resolution}.npz")

    if not os.path.exists(mask_path):
        if not HAS_REGIONS_DEPS:
            raise ImportError(f"Mask '{mask_name}' not found and optional dependencies for building it are missing.")

        builder = MaskBuilder(resolution=resolution)
        if mask_name == "giorgi":
            builder.build(RegionDefinitions.get_giorgi_regions(), mask_path, "region")
        elif mask_name == "ipcc_ar6":
            shp = RegionDefinitions.download_ipcc_ar6()
            if shp:
                builder.build(gpd.read_file(shp), mask_path, "Acronym")
        elif mask_name == "epa_eco":
            shp = RegionDefinitions.download_epa_ecoregions()
            if shp:
                builder.build(gpd.read_file(shp), mask_path, "NA_L2NAME")
        elif mask_name == "timezones":
            shp = RegionDefinitions.download_timezones()
            if shp:
                builder.build(gpd.read_file(shp), mask_path, "tz_name1st")
        elif mask_name == "epa_admin":
            gdf = RegionDefinitions.get_epa_admin_regions()
            if gdf is not None:
                builder.build(gdf, mask_path, "epa_region")
        elif mask_name == "land":
            shp = RegionDefinitions.download_land()
            if shp:
                gdf = gpd.read_file(shp)
                gdf["is_land"] = "land"
                builder.build(gdf, mask_path, "is_land")
        else:
            raise ValueError(f"Unknown mask: {mask_name}")

    return EarthMask(mask_path)


def query_mask(
    obj: xr.DataArray | xr.Dataset | t.Any, mask_name: str, resolution: float = 0.05, new_var: str | None = None
) -> t.Any:
    """Query a mask for an Xarray or Pandas object.

    Parameters
    ----------
    obj : xarray.DataArray, xarray.Dataset, or pandas.DataFrame
        Object containing latitude and longitude.
    mask_name : str
        Name of the mask to query.
    resolution : float, default: 0.05
        Resolution of the mask.
    new_var : str, optional
        Name of the new variable/column to create. Defaults to mask_name.

    Returns
    -------
    Object of same type as obj
        The object with the added mask information.
    """
    mask = get_mask(mask_name, resolution=resolution)
    if new_var is None:
        new_var = mask_name

    if hasattr(obj, "monet"):
        lat = obj.monet.lat
        lon = obj.monet.lon
    elif hasattr(obj, "latitude") and hasattr(obj, "longitude"):
        lat = obj.latitude
        lon = obj.longitude
    else:
        raise ValueError("Could not find latitude and longitude in object.")

    if isinstance(obj, xr.DataArray | xr.Dataset):
        res = xr.apply_ufunc(
            mask.query,
            lat,
            lon,
            dask="parallelized",
            output_dtypes=[object],
        )
        if isinstance(obj, xr.Dataset):
            obj[new_var] = res
            out = obj
        else:
            res.name = new_var
            out = res

        # Update history
        curr_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        history = out.attrs.get("history", "")
        out.attrs["history"] = history + f"\n{curr_time} > Queried {mask_name} mask via monet.util.mask.query_mask"
        return out
    else:
        # Assume Pandas
        obj[new_var] = mask.query(lat, lon)
        return obj
