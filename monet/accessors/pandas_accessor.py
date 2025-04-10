"""Pandas accessor for MONET functionality."""

import numpy as np
import pandas as pd
import xarray as xr

from .base import BaseAccessor, has_pyresample

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
    def _validate(obj):
        """Verify there is a column ``'latitude'`` and a column ``'longitude'``.

        Parameters
        ----------
        obj : pandas.DataFrame
            Object to validate.

        Raises
        ------
        AttributeError
            If obj does not have 'latitude' and 'longitude' columns.
        """
        if "latitude" not in obj.columns or "longitude" not in obj.columns:
            raise AttributeError("Must have 'latitude' and 'longitude'.")

    @property
    def center(self):
        """The geographic center point of this DataFrame.

        .. note::

           Currently just the lon and lat mean values,
           not necessarily representative of the geographic center.

        Returns
        -------
        tuple
            (lon, lat)
        """
        lat = self._obj.latitude
        lon = self._obj.longitude
        return (float(lon.mean()), float(lat.mean()))

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
        out = df[
            [
                "ascii2nc_message",
                "siteid",
                "ascii2nc_time",
                "latitude",
                "longitude",
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
                latitude="lat",
                longitude="lon",
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

    def rename_for_monet(self, df=None):
        """Rename latitude and longitude columns in the DataFrame.

        Parameters
        ----------
        df : pandas.DataFrame, optional
            To use instead of self.

        Returns
        -------
        pandas.DataFrame
            DataFrame with renamed latitude/longitude columns.
        """
        if df is None:
            df = self._obj
        if "lat" in df.columns:
            df = df.rename({"lat": "latitude", "lon": "longitude"})
        elif "Latitude" in df.columns:
            df = df.rename({"Latitude": "latitude", "Longitude": "longitude"})
        elif "Lat" in df.columns:
            df = df.rename({"Lat": "latitude", "Lon": "longitude"})
        elif "LAT" in df.columns:
            df = df.rename({"LAT": "latitude", "LON": "longitude"})
        return df

    def get_sparse_SwathDefinition(self):
        """Creates a ``pyreample.geometry.SwathDefinition`` for a single point.

        Returns
        -------
        pyreample.geometry.SwathDefinition
            SwathDefinition object for data points.
        """
        if not has_pyresample:
            raise ImportError("pyresample is required for this functionality")

        df = self.rename_for_monet(self._obj)
        from ..util.interp_util import nearest_point_swathdefinition as npsd
        return npsd(latitude=df.latitude.values, longitude=df.longitude.values)

    def _df_to_da(self, d=None):  # TODO: should be `to_ds` or `to_xarray`
        """Convert DataFrame to xarray.

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
            d = self._obj
        if d.index.name is not None:
            index_name = d.index.name
        ds = d.to_xarray().rename({index_name: "x"}).expand_dims("y")
        if "time" in ds.data_vars.keys():
            ds["time"] = ds.time.squeeze()  # it is only 1D
        if "latitude" in ds.data_vars.keys():
            ds = ds.set_coords(["latitude", "longitude"])
        return ds

    def remap_nearest(
        self,
        df,
        radius_of_influence=1e5,
        combine=False,
    ):
        """Remap data using nearest neighbor interpolation.

        Parameters
        ----------
        df : pandas.DataFrame
            DataFrame to remap.
        radius_of_influence : float, default: 1e5
            Search radius in meters.
        combine : bool, default: False
            Whether to combine the remapped data with the original data.

        Returns
        -------
        pandas.DataFrame
            Remapped DataFrame.
        """
        if not has_pyresample:
            raise ImportError("pyresample is required for this functionality")

        source_data = self.rename_for_monet(df)
        target_data = self.rename_for_monet(self._obj)
        # make fake index
        source_data = self._make_fake_index_var(source_data)
        source_data_da = self._df_to_da(source_data)
        target_data_da = self._df_to_da(target_data)
        source = source_data_da.monet._get_CoordinateDefinition(source_data_da)
        target = target_data_da.monet._get_CoordinateDefinition(target_data_da)
        res = pr.kd_tree.XArrayResamplerNN(
            source, target, radius_of_influence=radius_of_influence
        )
        res.get_neighbour_info()
        # interpolate just the make_fake_index variable
        r = res.get_sample_from_neighbour_info(source_data_da.monet_fake_index)
        r.name = "monet_fake_index"
        # now merge back from original DataFrame
        q = r.compute()
        v = q.squeeze().to_dataframe()
        result = v.merge(source_data, how="left", on="monet_fake_index").drop(
            "monet_fake_index", axis=1
        )
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

    def cftime_to_datetime64(self, col=None):
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
        df = self._obj

        def cf_to_dt64(x):
            return pd.to_datetime(x.strftime("%Y-%m-%d %H:%M:%S"))

        if col is None:  # assume 'time' is the column name to transform
            col = "time"
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
