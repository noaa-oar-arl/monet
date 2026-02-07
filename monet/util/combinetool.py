import numpy as np
import pandas as pd
import xarray as xr
from pandas import Series, merge_asof

try:
    import dask.dataframe as dd

    has_dask_df = True
except ImportError:
    has_dask_df = False


def pair(model, obs, *, method="nearest", interp_time=False, suffix="_model", merge=True, **kwargs):
    """Unified interface for pairing model and observation data.

    Supports xarray (Dataset/DataArray) and DataFrame (pandas/dask) objects.
    Maintains laziness for Dask-backed objects.

    Parameters
    ----------
    model : xarray.Dataset or xarray.DataArray
        Model data (usually gridded).
    obs : xarray.Dataset, xarray.DataArray, pandas.DataFrame, or dask.dataframe.DataFrame
        Observation data.
    method : str, default 'nearest'
        Spatial interpolation method.
    interp_time : bool, default False
        Whether to interpolate in time.
    suffix : str, default '_model'
        Suffix for model variables if names conflict.
    merge : bool, default True
        Whether to merge the paired data with the original observations.
    **kwargs : dict
        Additional arguments passed to regridding backend.

    Returns
    -------
    xarray.Dataset, pandas.DataFrame, or dask.dataframe.DataFrame
        Matched object of the same type as `obs`.
    """
    if isinstance(obs, xr.Dataset | xr.DataArray):
        return _pair_xarray(model, obs, method=method, interp_time=interp_time, suffix=suffix, merge=merge, **kwargs)
    elif isinstance(obs, pd.DataFrame) or (has_dask_df and isinstance(obs, dd.DataFrame)):
        return _pair_dataframe(model, obs, method=method, interp_time=interp_time, suffix=suffix, merge=merge, **kwargs)
    else:
        raise TypeError(f"Unsupported type for obs: {type(obs)}")


def _pair_xarray(model, obs, *, method="nearest", interp_time=False, suffix="_model", merge=True, **kwargs):
    """Pair xarray model with xarray observations."""
    import datetime

    from ..monet_accessor import _dataset_to_monet

    # Standardize
    model = _dataset_to_monet(model)
    obs = _dataset_to_monet(obs)

    # Use remap via accessor
    paired = obs.monet.remap(model, method=method, **kwargs)

    if interp_time:
        paired = paired.interp(time=obs.time)

    # Handle suffixes
    if isinstance(model, xr.DataArray):
        if model.name in obs.variables:
            paired.name = model.name + suffix
    else:  # Dataset
        for var in model.data_vars:
            if var in obs.variables:
                paired = paired.rename({var: var + suffix})

    # Update history
    curr_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    history = paired.attrs.get("history", "")
    paired.attrs["history"] = history + f"\n{curr_time} > Paired with observations via monet.pair"

    if merge:
        return xr.merge([obs, paired])
    else:
        return paired


def _pair_dataframe(model, obs, *, method="nearest", interp_time=False, suffix="_model", merge=True, **kwargs):
    """Pair xarray model with pandas or dask DataFrame observations."""
    from ..monet_accessor import _dataset_to_monet

    # Ensure model is standardized
    model = _dataset_to_monet(model)

    # Standardize DataFrame columns if needed
    if "lat" in obs.columns:
        obs = obs.rename(columns={"lat": "latitude", "lon": "longitude"})
    elif "Lat" in obs.columns:
        obs = obs.rename(columns={"Lat": "latitude", "Lon": "longitude"})

    # Extract unique locations to minimize remapping work
    # siteid is expected. If not present, we use lat/lon.
    loc_cols = ["latitude", "longitude"]
    if "siteid" in obs.columns:
        loc_cols.append("siteid")

    unique_locs = obs[loc_cols].drop_duplicates()

    # Convert unique locations to xarray (lazily if obs is dask)
    if has_dask_df and isinstance(obs, dd.DataFrame):
        # ESMF/xregrid requires eager coordinate arrays to build weights.
        # We compute only the unique locations (usually small).
        unique_locs_p = unique_locs.compute()
    else:
        unique_locs_p = unique_locs

    # Convert to xarray point Dataset
    from ..accessors.pandas_accessor import MONETAccessorPandas

    # Convert Arrow strings to objects in unique_locs_p to avoid Dask/Xarray issues later
    for col in unique_locs_p.columns:
        if pd.api.types.is_string_dtype(unique_locs_p[col]) and not pd.api.types.is_numeric_dtype(unique_locs_p[col]):
            unique_locs_p[col] = np.asarray(unique_locs_p[col], dtype=object)

    point_ds = MONETAccessorPandas(unique_locs_p)._df_to_da()
    if "siteid" in unique_locs_p.columns:
        # Add siteid as a coordinate so it is preserved during remap and conversion back to DF
        point_ds = point_ds.assign_coords(siteid=(("x"), unique_locs_p.siteid.values))

    # Remap model to points
    paired_da = point_ds.monet.remap(model, method=method, **kwargs)

    # Ensure siteid is preserved in coordinates for the join
    if "siteid" in point_ds.coords:
        if "x" in paired_da.dims:
            paired_da = paired_da.assign_coords(siteid=(("x"), point_ds.siteid.data))

    # Time interpolation if requested
    if interp_time:
        obs_times = obs.time.drop_duplicates()
        if has_dask_df and isinstance(obs, dd.DataFrame):
            obs_times = obs_times.compute()
        paired_da = paired_da.interp(time=obs_times)

    # Convert paired_da to DataFrame, matching laziness of model/paired_da
    if isinstance(paired_da, xr.DataArray):
        paired_da_ds = paired_da.to_dataset()
    else:
        paired_da_ds = paired_da

    # Ensure all strings are object dtype to avoid Dask/Arrow issues during conversion
    for var in paired_da_ds.variables:
        if pd.api.types.is_string_dtype(paired_da_ds[var]) and not pd.api.types.is_numeric_dtype(paired_da_ds[var]):
            paired_da_ds[var] = paired_da_ds[var].astype(object)

    if paired_da.chunks:
        paired_df = paired_da_ds.to_dask_dataframe().reset_index()
    else:
        paired_df = paired_da_ds.to_dataframe().reset_index()

    # Clean up dimensions from conversion
    cols_to_drop = [c for c in ["x", "y", "z", "latitude", "longitude"] if c in paired_df.columns]
    paired_df = paired_df.drop(columns=cols_to_drop)

    # Ensure no Arrow-backed strings remain in paired_df before merging
    if has_dask_df and isinstance(paired_df, dd.DataFrame):
        # We can't easily iterate and convert columns in dask.dataframe eagerly,
        # but the previous conversions should have prevented them from getting into paired_da_ds.
        pass
    else:
        for col in paired_df.columns:
            if pd.api.types.is_string_dtype(paired_df[col]) and not pd.api.types.is_numeric_dtype(paired_df[col]):
                paired_df[col] = np.asarray(paired_df[col], dtype=object)

    # Handle suffixes and variable names
    if isinstance(model, xr.DataArray):
        model_name = model.name or "model_data"
        if model_name in obs.columns:
            paired_df = paired_df.rename(columns={model_name: model_name + suffix})
    else:  # Dataset
        for var in model.data_vars:
            if var in obs.columns:
                paired_df = paired_df.rename(columns={var: var + suffix})

    if merge:
        # Perform join
        join_on = ["time"]
        if "siteid" in obs.columns:
            join_on.append("siteid")

        # If paired_df is dask, the result must be dask
        if has_dask_df and isinstance(paired_df, dd.DataFrame):
            if not isinstance(obs, dd.DataFrame):
                obs = dd.from_pandas(obs, npartitions=paired_df.npartitions)
            return obs.merge(paired_df, on=join_on, how="left")
        else:
            # Both are pandas
            return obs.merge(paired_df, on=join_on, how="left")
    else:
        return paired_df


def combine_da_to_df(da, df, *, merge=True, **kwargs):
    """Combine xarray data with point observations in a dataframe.

    Note: This is a backward compatibility wrapper for `monet.pair`.

    Parameters
    ----------
    da : xarray.DataArray or xarray.Dataset
        Gridded data to be interpolated to target points.
    df : pandas.DataFrame
        Point observations.
    merge : bool, default True
        Whether to merge with original DataFrame.
    **kwargs : dict
        Passed to `pair`.

    Returns
    -------
    pandas.DataFrame
        Combined DataFrame.
    """
    return pair(da, df, merge=merge, **kwargs)


def combine_da_to_da(source, target, *, merge=True, interp_time=False, **kwargs):
    """Combine gridded data with point observation data in xarray format.

    Note: This is a backward compatibility wrapper for `monet.pair`.

    Parameters
    ----------
    source : xarray.DataArray or xarray.Dataset
        Gridded data to interpolate from.
    target : xarray.DataArray or xarray.Dataset
        Point observation data.
    merge : bool, default True
        Whether to merge.
    interp_time : bool, default False
        Whether to interpolate in time.
    **kwargs : dict
        Additional arguments passed to `pair`.

    Returns
    -------
    xarray.Dataset
        Combined Dataset.
    """
    return pair(source, target, merge=merge, interp_time=interp_time, **kwargs)


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

    Note: This is a backward compatibility wrapper for `monet.pair`.

    Parameters
    ----------
    da : xarray.DataArray or xarray.Dataset
        Data to be interpolated to target grid points.
    df : pandas.DataFrame
        DataFrame containing point observations.
    suffix : str, default: None
        Suffix to add to the variable names.
    **kwargs : dict
        Additional keyword arguments for regridding.

    Returns
    -------
    pandas.DataFrame
        DataFrame with combined model and observation data.
    """
    if suffix is None:
        suffix = "_xesmf"
    return pair(da, df, suffix=suffix, **kwargs)


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


def combine_grid_to_point_esmf(grid_data, point_df, method="bilinear", locstream_kwargs=None, regrid_kwargs=None):
    """Combine gridded data with point observations using ESMF LocStream.

    Deprecated as ESMF dependency is removed.
    """
    raise NotImplementedError("This function relies on ESMF which has been removed.")
