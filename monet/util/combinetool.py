import xarray as xr
from pandas import Series, merge_asof
import pandas as pd
import numpy as np


def combine_da_to_df(da, df, *, merge=True, **kwargs):
    """Combine xarray data with point observations in a dataframe.

    Uses pyresample via ``.monet.remap_nearest``.

    Parameters
    ----------
    da : xarray.DataArray or xarray.Dataset
        Data to be interpolated to target grid points.
        Can be unstructured-grid data
        (detected by checking ``'mio_has_unstructured_grid'`` attribute).
    df : pandas.DataFrame
        Data on target points. Must contain 'latitude', 'longitude', and 'siteid' columns.
    merge : bool, default: True
        Whether to merge interpolated `da` data with original `df`.
        If False, return interpolated `da` data only.
    **kwargs : dict
        Passed to pyresample's neighbor lookup.

    Returns
    -------
    pandas.DataFrame
        DataFrame with combined model and observational data.
    """
    radius_of_influence = kwargs.pop('radius_of_influence', 12e4)
    suffix = kwargs.pop('suffix', '_new')

    target_da = df.drop_duplicates(subset=["siteid"]).dropna(
        subset=["latitude", "longitude", "siteid"]
    )

    # Rename lat/lon columns if needed
    if "lat" in target_da.columns:
        target_da = target_da.rename(columns={"lat": "latitude", "lon": "longitude"})
    elif "Lat" in target_da.columns:
        target_da = target_da.rename(columns={"Lat": "latitude", "Lon": "longitude"})
    elif "LAT" in target_da.columns:
        target_da = target_da.rename(columns={"LAT": "latitude", "LON": "longitude"})

    # Convert to xarray for remapping
    if not hasattr(target_da, 'monet'):
        # If someone passes a pandas DataFrame without the monet accessor registered
        from ..accessors.pandas_accessor import MONETAccessorPandas
        pd.api.extensions.register_dataframe_accessor("monet")(MONETAccessorPandas)

    target_data_da = target_da.monet._df_to_da()

    # Add if statement for unstructured grid output
    if da.attrs.get("mio_has_unstructured_grid", False):
        da_interped = target_data_da.monet.remap_nearest_unstructured(da).compute()
    else:
        da_interped = target_data_da.monet.remap_nearest(da,
                                                        radius_of_influence=radius_of_influence,
                                                        **kwargs).compute()

    da_interped["siteid"] = (("x"), target_da.siteid)
    da_interped_df = da_interped.to_dataframe().reset_index()
    cols = pd.Series(da_interped_df.columns)

    drop_cols = cols.loc[cols.isin(["x", "y", "z", "latitude", "longitude"])]
    da_interped_df.drop(drop_cols, axis=1, inplace=True)

    # Handle column naming conflicts
    if isinstance(da, xr.DataArray):
        if da.name in df.columns:
            da_interped_df.rename(columns={da.name: da.name + suffix}, inplace=True)
    else:  # Dataset
        dup_names = [name for name in da.data_vars.keys() if name in df.columns]
        if len(dup_names) > 0:
            for name in dup_names:
                da_interped_df.rename(columns={name: name + suffix}, inplace=True)

    if merge:
        df.reset_index(drop=True)
        da_interped_df.reset_index(drop=True)
        final_df = df.merge(da_interped_df, on=["time", "siteid"], how="left")
        return final_df
    else:
        return da_interped_df


def combine_da_to_da(source, target, *, merge=True, interp_time=False, **kwargs):
    """Combine source data array with target data array using nearest-neighbor interpolation.

    Uses pyresample nearest-neighbor via ``.monet.remap_nearest``.

    Parameters
    ----------
    source : xarray.DataArray or xarray.Dataset
        Gridded source data to be interpolated.
    target : xarray.DataArray or xarray.Dataset
        Target point observation data with locations.
    merge : bool, default: True
        If True, merge interpolated source data with target data.
        If False, return only the interpolated source data.
    interp_time : bool, default: False
        Whether to linearly interpolate to the target's time coordinates.
    **kwargs : dict
        Passed to :meth:`~monet.monet_accessor.MONETAccessor.remap_nearest`.

    Returns
    -------
    xarray.Dataset
        Combined or interpolated dataset.
    """
    from ..monet_accessor import _dataset_to_monet

    output = target.monet.remap_nearest(source, **kwargs)

    if interp_time:
        output = output.interp(time=target.time)

    if merge:
        output = xr.merge([_dataset_to_monet(target), output])

    return output


def _rename_latlon(ds):
    """Rename latitude/longitude coordinates to ensure consistent naming.

    Parameters
    ----------
    ds : xarray.Dataset
        Dataset with lat/lon or latitude/longitude coordinates.

    Returns
    -------
    xarray.Dataset
        Dataset with renamed coordinates if needed.
    """
    if "latitude" in ds.coords:
        return ds.rename({"latitude": "lat", "longitude": "lon"})
    elif "lat" in ds.coords:
        return ds.rename({"lat": "latitude", "lon": "longitude"})
    else:
        return ds


def combine_da_to_df_xesmf(da, df, suffix=None, **kwargs):
    """Combine point data with a DataArray using xESMF.

    Parameters
    ----------
    da : xarray.DataArray
        DataArray containing model data.
    df : pandas.DataFrame
        DataFrame containing point observations.
    suffix : str, default: None
        Suffix to add to the variable names to prevent column name conflicts.
    **kwargs : dict
        Additional keyword arguments for xESMF regridding.

    Returns
    -------
    pandas.DataFrame
        DataFrame with combined model and observation data.
    """
    try:
        import xesmf
    except ImportError:
        raise ImportError("xesmf is required for this functionality")

    from ..util.interp_util import lonlat_to_xesmf
    from ..util.resample import resample_xesmf

    # Default suffix
    if suffix is None:
        suffix = '_xesmf'

    # Make a copy of the DataFrame
    target = df.copy()

    # Rename lat/lon columns if needed
    if "lat" in target.columns:
        target = target.rename(columns={"lat": "latitude", "lon": "longitude"})
    elif "Lat" in target.columns:
        target = target.rename(columns={"Lat": "latitude", "Lon": "longitude"})
    elif "LAT" in target.columns:
        target = target.rename(columns={"LAT": "latitude", "LON": "longitude"})

    # Create xESMF compatible dataset for the point locations
    point_ds = lonlat_to_xesmf(
        longitude=target.longitude.values,
        latitude=target.latitude.values
    )

    # Rename coordinates for xESMF
    da_renamed = da.copy()
    if 'latitude' in da.coords:
        da_renamed = da_renamed.rename({'latitude': 'lat', 'longitude': 'lon'})

    # Use xESMF to resample the data
    result = resample_xesmf(da_renamed, point_ds, **kwargs)

    # Convert to DataFrame
    if isinstance(result, xr.DataArray):
        varname = result.name if result.name is not None else "model_data"
        sdf = pd.DataFrame({varname + suffix: result.values.ravel()}, index=target.index)
    else:  # Dataset
        sdf = pd.DataFrame(index=target.index)
        for varname, datavar in result.data_vars.items():
            sdf[varname + suffix] = datavar.values.ravel()

    # Merge with original DataFrame
    return pd.concat([target, sdf], axis=1)


def combine_da_to_df_xesmf_strat(da, daz, df, **kwargs):
    """Combine xarray data with dataframe observations using xESMF and vertical interpolation.

    This function does horizontal interpolation via xESMF and then vertical
    interpolation using stratify.

    Parameters
    ----------
    da : xarray.DataArray
        Source data to be interpolated.
    daz : xarray.DataArray
        Vertical coordinate data with same shape as `da`.
    df : pandas.DataFrame
        Target dataframe with 'latitude', 'longitude', 'time', and 'altitude' columns.
    **kwargs : dict
        Passed to :func:`~monet.util.resample.resample_xesmf`
        (and then to ``xesmf.Regridder``).

    Returns
    -------
    pandas.DataFrame
        Combined dataframe with original and interpolated data.

    Raises
    ------
    RuntimeError
        If da and daz have different shapes.
    """
    from ..util.interp_util import constant_1d_xesmf
    from ..util.resample import resample_xesmf

    try:
        if da.shape != daz.shape:
            raise RuntimeError
    except RuntimeError:
        print("da and daz must be of the same shape")
        print("da shape= ", da.shape, "daz shape= ", daz.shape)
        return -1

    target = constant_1d_xesmf(longitude=df.longitude.values, latitude=df.latitude.values)

    # check to rename 'latitude' and 'longitude' for xe.Regridder
    da = _rename_latlon(da)
    daz = _rename_latlon(daz)
    da_interped = resample_xesmf(da, target, **kwargs)  # interpolate fields
    daz_interped = resample_xesmf(daz, target, **kwargs)
    # check to change 'lat' 'lon' back
    da_interped = _rename_latlon(da_interped)
    daz_interped = _rename_latlon(daz_interped)

    # sort aircraft target altitudes and call stratfiy from resample to do vertical interpolation
    daz_interped_xyz = daz_interped.monet.stratify(sorted(df["altitude"]), daz_interped, axis=1)
    da_interped_xyz = da_interped.monet.stratify(sorted(df["altitude"]), daz_interped, axis=1)
    da_interped_xyz.name = da.name
    daz_interped_xyz.name = "altitude"
    df_interped_xyz = da_interped_xyz.to_dataframe().reset_index()
    dfz_interped_xyz = daz_interped_xyz.to_dataframe().reset_index()

    df_interped_xyz.insert(0, "altitude", dfz_interped_xyz["altitude"], allow_duplicates=True)

    cols = Series(df_interped_xyz.columns)
    drop_cols = cols.loc[cols.isin(["x", "y", "z"])]
    df_interped_xyz.drop(drop_cols, axis=1, inplace=True)
    if da.name in df.columns:
        df_interped_xyz.rename(columns={da.name: da.name + "_new"}, inplace=True)
        print(df_interped_xyz.keys())

    final_df = merge_asof(
        df,
        df_interped_xyz,
        by=["latitude", "longitude", "altitude"],
        on="time",
        direction="nearest",
    )
    return final_df


def combine_da_to_height_profile(da, dset, *, radius_of_influence=12e3):
    """Combine xarray data with a 2D height profile dataset.

    Parameters
    ----------
    da : xarray.DataArray
        Source data to be interpolated.
    dset : xarray.Dataset
        Target dataset with dimensions (time, z) and 'longitude', 'latitude' coordinates.
    radius_of_influence : float, default: 12000.0
        Search radius in meters for the nearest neighbor interpolation.

    Returns
    -------
    xarray.Dataset
        Target dataset with the interpolated variable added.
    """
    lon, lat = dset.longitude, dset.latitude
    da_interped = da.monet.nearest_latlon(lon=lon, lat=lat, radius_of_influence=radius_of_influence)

    # FIXME: interp to height here

    dset[da.name] = da_interped

    return dset
