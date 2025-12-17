import pandas as pd
import xarray as xr
from pandas import Series, merge_asof


def combine_da_to_df(da, df, *, merge=True, **kwargs):
    """Combine xarray data with point observations in a dataframe.

    Interpolates gridded data to observation points using nearest neighbor
    interpolation, then merges with the original observation data.

    Parameters
    ----------
    da : xarray.DataArray or xarray.Dataset
        Gridded data to be interpolated to target points.
        Can be unstructured-grid data
        (detected by checking ``'mio_has_unstructured_grid'`` attribute).
    df : pandas.DataFrame
        Point observations with 'latitude', 'longitude', and 'siteid' columns.
    merge : bool, default True
        If True, merge interpolated values with the original DataFrame.
        If False, return only the interpolated values.
    **kwargs : dict
        Passed to regridding backend.

    Returns
    -------
    pandas.DataFrame
        DataFrame with interpolated model values at observation locations,
        either merged with original data (if merge=True) or standalone.
    """
    radius_of_influence = kwargs.pop("radius_of_influence", 12e4) # unused
    suffix = kwargs.pop("suffix", "_new")

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
    if not hasattr(target_da, "monet"):
        # If someone passes a pandas DataFrame without the monet accessor registered
        from ..accessors.pandas_accessor import MONETAccessorPandas

        pd.api.extensions.register_dataframe_accessor("monet")(MONETAccessorPandas)

    target_data_da = target_da.monet._df_to_da()

    # Add if statement for unstructured grid output
    if da.attrs.get("mio_has_unstructured_grid", False):
        # Fallback to nearest neighbor or implement proper unstructured regrid if monet-regrid supports it
        # For now, using remap which uses monet-regrid
        da_interped = target_data_da.monet.remap(da, method="nearest", **kwargs).compute()
    else:
        da_interped = target_data_da.monet.remap(
            da, method="nearest", **kwargs
        ).compute()

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
    """Combine gridded data with point observation data in xarray format.

    Interpolates source gridded data to target point locations using nearest neighbor
    interpolation, with optional time interpolation and merging.

    Parameters
    ----------
    source : xarray.DataArray or xarray.Dataset
        Gridded data to interpolate from.
    target : xarray.DataArray or xarray.Dataset
        Point observation data with target coordinates.
    merge : bool, default True
        If True, merge interpolated values with the original target data.
        If False, return only the interpolated values.
    interp_time : bool, default False
        If True, linearly interpolate to the times in target.
    **kwargs : dict
        Additional arguments passed to remap.

    Returns
    -------
    xarray.Dataset
        Dataset with interpolated source data at target locations,
        either merged with original target data (if merge=True) or standalone.
    """
    from ..monet_accessor import _dataset_to_monet

    output = target.monet.remap(source, method="nearest", **kwargs)

    if interp_time:
        output = output.interp(time=target.time)

    if merge:
        output = xr.merge([_dataset_to_monet(target), output])

    return output


def _rename_latlon(ds):
    """Standardize latitude/longitude coordinate names.

    Converts between 'latitude'/'longitude' and 'lat'/'lon' naming conventions.

    Parameters
    ----------
    ds : xarray.Dataset
        Dataset with latitude and longitude coordinates to rename.

    Returns
    -------
    xarray.Dataset
        Dataset with standardized coordinate names.
    """
    if "latitude" in ds.coords:
        return ds.rename({"latitude": "lat", "longitude": "lon"})
    elif "lat" in ds.coords:
        return ds.rename({"lat": "latitude", "lon": "longitude"})
    else:
        return ds


def combine_da_to_df_xesmf(da, df, *, suffix=None, **kwargs):
    """Combine xarray data array `da` with spatial information
    point observations in dataframe `df`, returning a new dataframe.

    Parameters
    ----------
    da : xarray.DataArray or xarray.Dataset
        Data to be interpolated to target grid points.
    df : pandas.DataFrame
        DataFrame containing point observations.
    suffix : str, default: None
        Suffix to add to the variable names to prevent column name conflicts.
    **kwargs : dict
        Additional keyword arguments for regridding.

    Returns
    -------
    pandas.DataFrame
        DataFrame with combined model and observation data.
    """

    if df.empty:
        return df

    from ..util.interp_util import lonlat_to_xesmf
    from ..util.resample import resample

    # Default suffix
    if suffix is None:
        suffix = "_xesmf"

    # Make a copy of the DataFrame
    target = df.copy()

    # Rename lat/lon columns if needed
    if "lat" in target.columns:
        target = target.rename(columns={"lat": "latitude", "lon": "longitude"})
    elif "Lat" in target.columns:
        target = target.rename(columns={"Lat": "latitude", "Lon": "longitude"})
    elif "LAT" in target.columns:
        target = target.rename(columns={"LAT": "latitude", "LON": "longitude"})

    # Create compatible dataset for the point locations
    point_ds = lonlat_to_xesmf(longitude=target.longitude.values, latitude=target.latitude.values)

    # Use resample (monet-regrid) to resample the data
    # Note: monet-regrid might expect 2D coords, lonlat_to_xesmf creates 2D meshgrid or similar
    result = resample(da, point_ds, **kwargs)

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
    """Combine vertical profile data and surface observations.

    Parameters
    ----------
    da : xarray.DataArray
        Data to interpolate.
    daz : xarray.DataArray
        Vertical coordinate data array
    df : pandas.DataFrame
        DataFrame containing surface observations with lat/lon coordinates
    **kwargs
        Additional arguments passed to regridder

    Returns
    -------
    pandas.DataFrame
        Combined data frame with interpolated model values at observation points
    """
    from ..util.interp_util import constant_1d_xesmf
    from ..util.resample import resample

    try:
        if da.shape != daz.shape:
            raise RuntimeError
    except RuntimeError:
        print("da and daz must be of the same shape")
        print("da shape= ", da.shape, "daz shape= ", daz.shape)
        return -1

    target = constant_1d_xesmf(longitude=df.longitude.values, latitude=df.latitude.values)

    da_interped = resample(da, target, **kwargs)  # interpolate fields
    daz_interped = resample(daz, target, **kwargs)

    # Ensure daz_interped and da_interped are xarray.DataArray before using .monet
    if not hasattr(daz_interped, "monet"):
        daz_interped = xr.DataArray(daz_interped)
    if not hasattr(da_interped, "monet"):
        da_interped = xr.DataArray(da_interped)

    # sort aircraft target altitudes and call stratfiy from resample to do vertical interpolation
    # resample_stratify from monet accessor
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
    """This function will combine an xarray.DataArray to a 2d dataset with
    dimensions (time,z)

    Parameters
    ----------
    da : xarray.DataArray
    dset : xarray.Dataset
        Dataset containing vertical profile observations
    radius_of_influence : float, optional
        Search radius for nearest neighbor interpolation in meters.
        Default is 12km.

    Returns
    -------
    xarray.Dataset
        Combined dataset with interpolated model values at observation heights
    """
    # from ..util.interp_util import nearest_point_swathdefinition
    lon, lat = dset.longitude, dset.latitude
    # target_grid = nearest_point_swathdefinition(longitude=lon, latitude=lat)
    da_interped = da.monet.nearest_latlon(lon=lon, lat=lat, radius_of_influence=radius_of_influence)

    # FIXME: interp to height here

    dset[da.name] = da_interped

    return dset


def combine_grid_to_point_esmf(
    grid_data, point_df, method="bilinear", locstream_kwargs=None, regrid_kwargs=None
):
    """Combine gridded data with point observations using ESMF LocStream.

    Deprecated as ESMF dependency is removed.
    """
    raise NotImplementedError("This function relies on ESMF which has been removed.")
