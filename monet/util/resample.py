try:
    from pyresample.kd_tree import XArrayResamplerNN
    from pyresample.geometry import SwathDefinition, AreaDefinition

    has_pyresample = True
except ImportError:
    print("PyResample not installed.  Some functionality will be lost")
    has_pyresample = False
try:
    import xesmf

    has_xesmf = True
except ImportError:
    has_xesmf = False


def _ensure_swathdef_compatability(defin):
    """ensures the SwathDefinition is compatible with XArrayResamplerNN.

    Parameters
    ----------
    defin : pyresample SwathDefinition
        a pyresample.geometry.SwathDefinition instance

    Returns
    -------
    type
        Description of returned object.

    """
    import xarray as xr

    if isinstance(defin.lons, xr.DataArray):
        return defin  # do nothing
    else:
        defin.lons = xr.DataArray(defin.lons, dims=["y", "x"]).chunk()
        defin.lats = xr.DataArray(defin.lons, dims=["y", "x"]).chunk()
        return defin


def _check_swath_or_area(defin):
    """Checks for a SwathDefinition or AreaDefinition. If AreaDefinition do
    nothing else ensure compatability with XArrayResamplerNN

    Parameters
    ----------
    defin : pyresample SwathDefinition or AreaDefinition
        Description of parameter `defin`.

    Returns
    -------
    pyresample.geometry
        SwathDefinition or AreaDefinition

    """
    try:
        if isinstance(defin, SwathDefinition):
            newswath = _ensure_swathdef_compatability(defin)
        elif isinstance(defin, AreaDefinition):
            newswath = defin
        else:
            raise RuntimeError
    except RuntimeError:
        print("grid definition must be a pyresample SwathDefinition or " "AreaDefinition")
        return
    return newswath


def _reformat_resampled_data(orig, new, target_grid):
    """reformats the resampled data array filling in coords, name and attrs .

    Parameters
    ----------
    orig : xarray.DataArray
        original input DataArray.
    new : xarray.DataArray
        resampled xarray.DataArray
    target_grid : pyresample.geometry
        target grid is the target SwathDefinition or AreaDefinition

    Returns
    -------
    xarray.DataArray
        reformated xarray.DataArray

    """
    target_lon, target_lat = target_grid.get_lonlats_dask()
    new.name = orig.name
    new["latitude"] = (("y", "x"), target_lat)
    new["longitude"] = (("y", "x"), target_lon)
    new.attrs["area"] = target_grid
    return new


def resample_stratify(da, levels, vertical, axis=1):
    import stratify
    import xarray as xr

    result = stratify.interpolate(levels, vertical.chunk(), da.chunk(), axis=axis)
    dims = da.dims
    out = xr.DataArray(result, dims=dims)
    for i in dims:
        if i != "z":
            out[i] = da[i]
    out.attrs = da.attrs.copy()
    if len(da.coords) > 0:
        for i in da.coords:
            if i != "z":
                out.coords[i] = da.coords[i]
    return out


def resample_xesmf(source_da, target_da, cleanup=False, **kwargs):
    if has_xesmf:
        import xesmf as xe
        import xarray as xr

        regridder = xe.Regridder(source_da, target_da, **kwargs)
        if cleanup:
            regridder.clean_weight_file()
        if isinstance(source_da, xr.Dataset):
            das = {}
            for name, i in source_da.data_vars.items():
                das[name] = regridder(i)
            ds = xr.Dataset(das)
            ds.attrs = source_da.attrs
            return ds
        else:
            return regridder(source_da)
