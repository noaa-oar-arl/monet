"""Pandas accessor for MONET functionality."""

import numpy as np
import pandas as pd
import xarray as xr

from .base import BaseAccessor, has_xregrid


@pd.api.extensions.register_dataframe_accessor("monet")
class MONETAccessorPandas(BaseAccessor):
    """Pandas DataFrame accessor for MONET functionality.

    This accessor adds MONET-specific methods to pandas DataFrames.
    """

    def __init__(self, pandas_obj):
        """Initialize the accessor.

        Parameters
        ----------
        pandas_obj : pandas.DataFrame
            The pandas DataFrame this accessor will work with.
        """
        self._validate(pandas_obj)
        self._obj = pandas_obj

    @staticmethod
    def _validate(obj: pd.DataFrame):
        """Verify there is a column for latitude and longitude.

        Parameters
        ----------
        obj : pandas.DataFrame
            Object to validate.

        Raises
        ------
        AttributeError
            If obj does not have latitude and longitude columns.
        """
        # More flexible validation to support common variants before rename_for_monet
        lat_names = ["latitude", "lat", "Latitude", "Lat", "LAT"]
        lon_names = ["longitude", "lon", "Longitude", "Lon", "LON"]

        has_lat = any(name in obj.columns for name in lat_names)
        has_lon = any(name in obj.columns for name in lon_names)

        if not (has_lat and has_lon):
            raise AttributeError("Must have latitude and longitude columns.")

    @property
    def center(self) -> tuple[float, float]:
        """The geographic center point of this DataFrame.

        .. note::

           Currently just the lon and lat mean values,
           not necessarily representative of the geographic center.

        Returns
        -------
        tuple
            (lon, lat)
        """
        # Use detected names
        lat_names = ["latitude", "lat", "Latitude", "Lat", "LAT"]
        lon_names = ["longitude", "lon", "Longitude", "Lon", "LON"]

        lat_col = next((c for c in lat_names if c in self._obj.columns), None)
        lon_col = next((c for c in lon_names if c in self._obj.columns), None)

        if lat_col is None or lon_col is None:
            raise AttributeError("Could not detect latitude and longitude columns.")

        lat = self._obj[lat_col]
        lon = self._obj[lon_col]
        return (float(lon.mean()), float(lat.mean()))

    def _get_latlon_cols(self) -> tuple[str, str]:
        """Get the detected latitude and longitude column names."""
        lat_names = ["latitude", "lat", "Latitude", "Lat", "LAT"]
        lon_names = ["longitude", "lon", "Longitude", "Lon", "LON"]

        lat_col = next((c for c in lat_names if c in self._obj.columns), None)
        lon_col = next((c for c in lon_names if c in self._obj.columns), None)

        if lat_col is None or lon_col is None:
            raise AttributeError("Could not detect latitude and longitude columns.")
        return lat_col, lon_col

    def to_ascii2nc_df(
        self,
        grib_code=126,
        height_msl=0.0,
        column="aod_550nm",
        message_type="ADPUPA",
        pressure=1000.0,
        qc=None,
        height_agl=None,
    ):
        """Convert DataFrame to MET ASCII2NC format.

        Parameters
        ----------
        grib_code : int, default: 126
            GRIB code for the variable.
        height_msl : float or str, default: 0.0
            Height above mean sea level (meters).
        column : str, default: "aod_550nm"
            Column name from which to get observation values.
        message_type : str, default: "ADPUPA"
            Message type for MET.
        pressure : float or str, default: 1000.0
            Pressure level (mb).
        qc : str, optional
            Quality control value. If None, defaults to 0.
        height_agl : float or str, optional
            Height above ground level (meters). If None, defaults to 0.

        Returns
        -------
        pandas.DataFrame
            DataFrame formatted for MET ASCII2NC.
        """
        df = self._obj
        df["ascii2nc_time"] = df.time.dt.strftime("%Y%m%d_%H%M%S")
        df["ascii2nc_gribcode"] = int(grib_code)
        if isinstance(height_msl, str):
            df["ascii2nc_elevation"] = df[height_msl]
        else:
            df["ascii2nc_elevation"] = height_msl
        df["ascii2nc_message"] = message_type
        if isinstance(pressure, str):
            df["ascii2nc_pressure"] = df[pressure]
        else:
            df["ascii2nc_pressure"] = pressure
        df["ascii2nc_value"] = df[column]
        if qc is None:
            df["ascii2nc_qc"] = "0"
            df.loc[df["ascii2nc_value"].isnull(), "ascii2nc_qc"] = "1"
        else:
            df["ascii2nc_qc"] = "0"
        if height_agl is None:
            df["ascii2nc_height_agl"] = df["ascii2nc_elevation"]
        elif isinstance(height_agl, str):
            df["ascii2nc_height_agl"] = df[height_agl]
        else:
            df["ascii2nc_height_agl"] = height_agl
        lat_col, lon_col = self._get_latlon_cols()
        out = df[
            [
                "ascii2nc_message",
                "siteid",
                "ascii2nc_time",
                lat_col,
                lon_col,
                "ascii2nc_elevation",
                "ascii2nc_gribcode",
                "ascii2nc_pressure",
                "ascii2nc_height_agl",
                "ascii2nc_qc",
                "ascii2nc_value",
            ]
        ]
        out = out.rename(
            dict(
                ascii2nc_message="typ",
                siteid="sid",
                ascii2nc_time="vld",
                **{lat_col: "lat", lon_col: "lon"},
                ascii2nc_elevation="elv",
                ascii2nc_gribcode="var",
                ascii2nc_pressure="lvl",
                ascii2nc_height_agl="lvl",
                ascii2nc_qc="qc",
                ascii2nc_value="obs",
            ),
            axis=1,
        )
        out = out.astype(dict(typ=str, sid=str, vld=str, var=str, qc=str))
        return out

    def to_ascii2nc_list(self, **kwargs):
        """Convert DataFrame to MET ASCII2NC list format.

        Parameters
        ----------
        **kwargs : dict
            Keyword arguments passed to to_ascii2nc_df().

        Returns
        -------
        list of list
            List of lists formatted for MET ASCII2NC.
        """
        out = self.to_ascii2nc_df(**kwargs)
        return out.values.tolist()

    @staticmethod
    def rename_for_monet(df: pd.DataFrame) -> pd.DataFrame:
        """Rename latitude and longitude columns in the DataFrame to MONET standard.

        Parameters
        ----------
        df : pandas.DataFrame
            DataFrame to rename columns in.

        Returns
        -------
        pandas.DataFrame
            DataFrame with renamed latitude/longitude columns.
        """
        out = df.copy()
        col_map = None
        if "lat" in out.columns and "lon" in out.columns:
            col_map = {"lat": "latitude", "lon": "longitude"}
        elif "Latitude" in out.columns and "Longitude" in out.columns:
            col_map = {"Latitude": "latitude", "Longitude": "longitude"}
        elif "Lat" in out.columns and "Lon" in out.columns:
            col_map = {"Lat": "latitude", "Lon": "longitude"}
        elif "LAT" in out.columns and "LON" in out.columns:
            col_map = {"LAT": "latitude", "LON": "longitude"}

        if col_map:
            out = out.rename(columns=col_map)

        # If neither, but already correct, do nothing
        # If neither, but only one present, add missing as NaN
        if "latitude" not in out.columns:
            out["latitude"] = np.nan
        if "longitude" not in out.columns:
            out["longitude"] = np.nan

        # Reorder columns to put latitude/longitude first if present
        cols = list(out.columns)
        for c in ["latitude", "longitude"]:
            if c in cols:
                cols.insert(0, cols.pop(cols.index(c)))
        out = out[cols]
        return out

    def get_sparse_SwathDefinition(self):
        """Creates a ``pyreample.geometry.SwathDefinition`` for a single point.

        Returns
        -------
        pyreample.geometry.SwathDefinition
            SwathDefinition object for data points.
        """
        raise NotImplementedError("This function relies on pyresample which has been removed.")

    def _df_to_da(self, d: pd.DataFrame | None = None) -> xr.Dataset:  # TODO: should be `to_ds` or `to_xarray`
        """Convert DataFrame to xarray.
        Preserves detected spatial columns as coordinates.

        Parameters
        ----------
        d : pandas.DataFrame, optional
            To use instead of self.

        Returns
        -------
        xarray.Dataset
        """
        index_name = "index"
        if d is None:
            d = self._obj.copy()
        else:
            d = d.copy()

        # Avoid issues with Arrow-backed strings
        for col in d.columns:
            if pd.api.types.is_string_dtype(d[col]) and not pd.api.types.is_numeric_dtype(d[col]):
                d[col] = np.asarray(d[col], dtype=object)
        if pd.api.types.is_string_dtype(d.index) and not pd.api.types.is_numeric_dtype(d.index):
            d.index = pd.Index(np.asarray(d.index, dtype=object), name=d.index.name)

        if d.index.name is not None:
            index_name = d.index.name
        ds = d.to_xarray().rename({index_name: "x"}).expand_dims("y")
        if "time" in ds.data_vars.keys():
            ds["time"] = ds.time.squeeze()  # it is only 1D

        # Detect spatial columns to set as coords
        lat_names = ["latitude", "lat", "Latitude", "Lat", "LAT"]
        lon_names = ["longitude", "lon", "Longitude", "Lon", "LON"]
        lat_col = next((c for c in lat_names if c in ds.data_vars), None)
        lon_col = next((c for c in lon_names if c in ds.data_vars), None)

        coords_to_set = []
        if lat_col:
            coords_to_set.append(lat_col)
        if lon_col:
            coords_to_set.append(lon_col)
        if coords_to_set:
            ds = ds.set_coords(coords_to_set)

        return ds

    def remap_nearest(
        self,
        df: pd.DataFrame,
        radius_of_influence: float = 1e5,
        combine: bool = False,
    ) -> pd.DataFrame:
        """Remap data using nearest neighbor interpolation (xregrid).

        Parameters
        ----------
        df : pandas.DataFrame
            DataFrame to remap.
        radius_of_influence : float, default: 1e5
            Search radius in meters (unused in xregrid).
        combine : bool, default: False
            Whether to combine the remapped data with the original data.

        Returns
        -------
        pandas.DataFrame
            Remapped DataFrame.
        """
        if not has_xregrid:
            raise ImportError("xregrid (with esmpy) is required for this functionality")

        from ..util.resample import resample

        source_data = self.rename_for_monet(df)
        target_data = self.rename_for_monet(self._obj)

        # make fake index
        source_data = self._make_fake_index_var(source_data)
        source_data_da = self._df_to_da(source_data)
        target_data_da = self._df_to_da(target_data)

        # Use xregrid to resample
        da_source = source_data_da["monet_fake_index"]
        res = resample(da_source, target_data_da, method="nearest")

        r = res
        r.name = "monet_fake_index"

        # now merge back from original DataFrame
        q = r.compute()
        v = q.to_dataframe()

        # Ensure we have the fake index column to merge on
        if "monet_fake_index" not in v.columns:
            # It might be in the index if xarray conversion put it there
            v = v.reset_index()

        result = v.merge(source_data, how="left", on="monet_fake_index").drop("monet_fake_index", axis=1)

        # Restore index if it was lost
        result.index = target_data.index

        if combine:
            columns_to_use = result.columns.difference(target_data.columns)
            return pd.merge(
                target_data,
                result[columns_to_use],
                left_index=True,
                right_index=True,
                how="outer",
            )
        else:
            return result

    def cftime_to_datetime64(self, col: str | None = None) -> pd.DataFrame:
        """Convert cftime column to numpy datetime64.

        Parameters
        ----------
        col : str, optional
            Name of the column to convert. If None, tries to detect the time column.

        Returns
        -------
        pandas.DataFrame
            DataFrame with converted time column.
        """
        df = self._obj.copy()

        def cf_to_dt64(x):
            try:
                return pd.to_datetime(x.strftime("%Y-%m-%d %H:%M:%S"))
            except AttributeError:
                return x

        if col is None:  # assume 'time' is the column name to transform
            col = "time"

        if col in df.columns:
            df[col] = df[col].apply(cf_to_dt64)
        return df

    def _make_fake_index_var(self, df):
        """Create a fake index variable for the DataFrame.

        Parameters
        ----------
        df : pandas.DataFrame
            DataFrame to create index for.

        Returns
        -------
        pandas.DataFrame
            DataFrame with fake index column added.
        """
        from numpy import arange

        # column = df.columns[0]
        fake_index = arange(len(df))
        column_name = "monet_fake_index"
        r = pd.Series(fake_index.astype(float), index=df.index)
        r.name = column_name
        df[column_name] = r
        return df

    def quick_facet_time_map(self, *args, **kwargs):
        """
        Faceted map plotting is not supported for Pandas DataFrames.
        """
        raise NotImplementedError("Faceted map plotting is only available for xarray DataArray and Dataset accessors.")

    def plot_points_map(
        self,
        lon_col="longitude",
        lat_col="latitude",
        projection=None,
        color="C0",
        marker="o",
        size=40,
        edgecolor="k",
        alpha=0.8,
        map_kws=None,
        figsize=(8, 6),
        dpi=150,
        title=None,
        export_path=None,
        export_formats=None,
        **kwargs,
    ):
        """
        Plot points from this DataFrame on a Cartopy map.
        """
        from ..plots.cartopy_utils import plot_points_map

        return plot_points_map(
            self._obj,
            lon_col=lon_col,
            lat_col=lat_col,
            projection=projection,
            color=color,
            marker=marker,
            size=size,
            edgecolor=edgecolor,
            alpha=alpha,
            map_kws=map_kws,
            figsize=figsize,
            dpi=dpi,
            title=title,
            export_path=export_path,
            export_formats=export_formats,
            **kwargs,
        )

    def pair(self, model, **kwargs):
        """Pair this DataFrame with model data.

        Parameters
        ----------
        model : xarray.Dataset or xarray.DataArray
            Model data to pair with.
        **kwargs : dict
            Additional arguments passed to `monet.pair`.

        Returns
        -------
        pandas.DataFrame or dask.dataframe.DataFrame
            The DataFrame with paired model data.
        """
        from ..util.combinetool import pair

        return pair(model, self._obj, **kwargs)

    def plot_lines_map(
        self,
        lon_col="longitude",
        lat_col="latitude",
        group_col=None,
        projection=None,
        color="C0",
        linewidth=2,
        alpha=0.8,
        map_kws=None,
        figsize=(8, 6),
        dpi=150,
        title=None,
        export_path=None,
        export_formats=None,
        **kwargs,
    ):
        """
        Plot lines from this DataFrame on a Cartopy map. Optionally group by a column.
        """
        from ..plots.cartopy_utils import plot_lines_map

        return plot_lines_map(
            self._obj,
            lon_col=lon_col,
            lat_col=lat_col,
            group_col=group_col,
            projection=projection,
            color=color,
            linewidth=linewidth,
            alpha=alpha,
            map_kws=map_kws,
            figsize=figsize,
            dpi=dpi,
            title=title,
            export_path=export_path,
            export_formats=export_formats,
            **kwargs,
        )
