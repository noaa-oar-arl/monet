"""MONET horizontal and vertical regridding with AXIS."""

import typing as t
import warnings

import numpy as np
import xarray as xr

try:
    import axis
    from axis import Regridder

    has_axis = True
    _axis_import_error = None
except (ImportError, OSError) as exc:
    axis = None
    Regridder = None
    has_axis = False
    _axis_import_error = exc

AXIS_INSTALL_MESSAGE = (
    "AXIS (axis-regrid) is required for horizontal regridding. "
    "Install with: pip install 'git+https://github.com/NOAA-EMC/HELM.git@"
    "a3e36e69e4d2c4426a21a11bde2d1cd2a8a70e0d#subdirectory=libs/axis'. "
    "Requires a C++20 compiler, CMake, MPI and Kokkos/KokkosKernels; see docs/installing.md."
)

_METHOD_MAP = {
    "linear": "bilinear",
    "bilinear": "bilinear",
    "nearest": "nearest",
    "nearest_s2d": "nearest",
    "nearest_d2s": "nearest",
    "bicubic": "bicubic",
    "patch": "patch",
    "conservative": "conservative",
    "conservative2nd": "conservative2nd",
    "conservative1storder": "conservative",
    "conservative2ndorder": "conservative2nd",
}


def __getattr__(name):
    if name == "has_xregrid":
        warnings.warn("has_xregrid is deprecated; use has_axis instead.", DeprecationWarning, stacklevel=2)
        return has_axis
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def _require_axis():
    if not has_axis:
        raise ImportError(AXIS_INSTALL_MESSAGE) from _axis_import_error


def _method(method):
    if not isinstance(method, str) or method.lower() not in _METHOD_MAP:
        raise ValueError(f"Unknown interpolation method {method!r}. Choose from {list(_METHOD_MAP)}")
    return _METHOD_MAP[method.lower()]


def _as_dataset(obj):
    if isinstance(obj, xr.Dataset):
        return obj
    if isinstance(obj, xr.DataArray):
        # Avoid collisions with an existing coordinate, including unnamed inputs.
        name = "_monet_field"
        while name in obj.coords:
            name += "_"
        return obj.to_dataset(name=name)
    raise TypeError("Source and target must be xarray.DataArray or xarray.Dataset")


def _mesh_variables(ds):
    """Names of topology and its referenced grid variables, excluding field data."""
    names = set()
    for name in ds.variables:
        if ds[name].attrs.get("cf_role") == "mesh_topology":
            names.add(name)
            for key, value in ds[name].attrs.items():
                if key.endswith("_coordinates") or key.endswith("_connectivity"):
                    names.update(item for item in str(value).split() if item in ds.variables)
    return names


def _prepare_grid(obj, side):
    """Keep full mesh metadata and normalize rectilinear dimension aliases only."""
    ds = _as_dataset(obj)
    coords = []
    for standard, names in (
        ("latitude", ("lat", "latCell", "lat_face", "lat_node", "latitude")),
        ("longitude", ("lon", "lonCell", "lon_face", "lon_node", "longitude")),
    ):
        coord = next((ds[c] for c in ds.coords if ds[c].attrs.get("standard_name") == standard), None)
        if coord is None:
            coord = next((ds[c] for c in ds.coords if ds[c].attrs.get("axis") == ("Y" if standard == "latitude" else "X")), None)
        if coord is None:
            coord = next((ds[c] for c in names if c in ds), None)
        if coord is None:
            raise KeyError(f"AXIS could not detect {standard} coordinates on the {side} grid")
        if coord.size == 0:
            raise ValueError(f"AXIS {side} grid has empty spatial coordinates")
        coords.append(coord)
    lat, lon = coords
    restore = {}
    restore_coords = {}
    # AXIS uses coordinate names as rectilinear dimensions. Swap dimensions in a
    # shallow view when callers use latitude(y)/longitude(x); restore them on return.
    is_mesh = any(v.attrs.get("cf_role") == "mesh_topology" for v in ds.variables.values())
    if lat.ndim == lon.ndim == 1 and lat.dims != lon.dims and not is_mesh:
        if np.any(np.abs(np.diff(lon.values)) > 180):
            # Infer narrow cells across the dateline rather than cells spanning
            # most of the globe. Restore the caller's longitude convention below.
            restore_coords[str(lon.name)] = lon
            unwrapped = np.rad2deg(np.unwrap(np.deg2rad(lon.values)))
            ds = ds.assign({lon.name: lon.copy(data=unwrapped)})
        swaps = {c.dims[0]: c.name for c in coords if c.dims[0] != c.name}
        if swaps:
            ds = ds.swap_dims(swaps)
            restore = {v: k for k, v in swaps.items()}
    elif lat.dims == lon.dims and not is_mesh:
        # Point/trajectory dimensions such as time or station must not be
        # discarded as non-spatial by AXIS's grid detector.
        keywords = ("lat", "lon", "x", "y", "node", "face", "element", "cell", "n_pts", "ncol", "grid_size", "vert", "tile")
        swaps = {
            dim: f"_monet_axis_node_{i}"
            for i, dim in enumerate(lat.dims)
            if not any(word in dim.lower() for word in keywords) or lat.ndim == 1
        }
        if swaps:
            ds = ds.rename_dims(swaps)
            restore = {v: k for k, v in swaps.items()}
    return ds, restore, restore_coords


class PersistentRegridder:
    """Reusable AXIS weights with MONET type, dimension, and history conventions.

    Construct with :func:`create_regridder`. Only apply to fields on the same
    source grid; weight files do not encode or validate the grid/method identity.
    """

    def __init__(self, engine, source_grid, target_grid, method, source_dims, target_dims, target_coords):
        self._engine = engine
        self._source_dims = source_dims
        self._target_dims = target_dims
        self._target_coords = target_coords
        self.method = method
        self.source_grid_ds = source_grid
        self.target_grid_ds = target_grid

    def __call__(self, obj):
        from .conventions import update_history

        src = _as_dataset(obj)
        for new, old in self._source_dims.items():
            if old in src.dims:
                if new not in src.coords:
                    src = src.assign_coords({new: self.source_grid_ds[new].rename({new: old})})
                src = src.swap_dims({old: new}) if new in src.coords else src.rename_dims({old: new})
        was_da = isinstance(obj, xr.DataArray)
        field_name = next(iter(src.data_vars)) if was_da else None
        out = self._engine(src)
        # AXIS carries non-spatial variables through. Source topology describes
        # the old grid and must be replaced before the output can be remapped again.
        source_mesh = _mesh_variables(self.source_grid_ds)
        target_mesh = _mesh_variables(self.target_grid_ds)
        out = out.drop_vars(list(source_mesh - target_mesh), errors="ignore")
        for name in target_mesh:
            if name in self.target_grid_ds.coords:
                out = out.assign_coords({name: self.target_grid_ds[name]})
            else:
                out[name] = self.target_grid_ds[name]
        for new, old in self._target_dims.items():
            if new in out.dims:
                out = out.swap_dims({new: old}) if old in out.coords else out.rename_dims({new: old})
        if self._target_coords:
            out = out.assign_coords(self._target_coords)
        # AXIS preserves global attrs, but its Dataset path does not preserve each
        # variable's attrs. Restore those without sharing mutable attr dictionaries.
        out.attrs = dict(obj.attrs)
        for name in out.data_vars:
            if name in src.data_vars and name not in source_mesh | target_mesh:
                out[name].attrs = dict(src[name].attrs)
                if source_mesh:
                    out[name].attrs.pop("mesh", None)
                    out[name].attrs.pop("location", None)
                for mesh in target_mesh:
                    if self.target_grid_ds[mesh].attrs.get("cf_role") == "mesh_topology":
                        conn = self.target_grid_ds[mesh].attrs.get("face_node_connectivity")
                        if conn in self.target_grid_ds and self.target_grid_ds[conn].dims[0] in out[name].dims:
                            out[name].attrs.update(mesh=mesh, location="face")
        if was_da:
            out = out[field_name].rename(obj.name)
        update_history(out, f"Resampled via monet.util.resample (backend=AXIS, method={self.method})")
        return out

    def to_file(self, path):
        """Save binary AXIS weights; reuse with the same grids and method."""
        return self._engine.to_file(str(path))

    def close(self):
        """Release AXIS weights and any worker registrations owned by this regridder."""
        self._engine.close()


def create_regridder(
    source_grid: xr.DataArray | xr.Dataset,
    target_grid: xr.DataArray | xr.Dataset,
    method: str = "bilinear",
    *,
    weights_file: str | None = None,
    **engine_options,
) -> PersistentRegridder:
    """Build reusable AXIS weights, optionally loading a previously saved file.

    ``linear`` maps to ``bilinear``; both legacy nearest aliases map to AXIS's
    source-to-target nearest method. ``radius_of_influence`` and ``n_processes``
    are accepted and ignored for compatibility. Other options go to AXIS.
    """
    real_method = _method(method)
    _require_axis()
    src, source_dims, _ = _prepare_grid(source_grid, "source")
    dst, target_dims, target_coords = _prepare_grid(target_grid, "target")
    engine_options.pop("radius_of_influence", None)
    engine_options.pop("n_processes", None)
    if weights_file is not None:
        engine_options["weights_file"] = str(weights_file)
    engine = Regridder(src, dst, method=real_method, **engine_options)
    return PersistentRegridder(engine, src, dst, real_method, source_dims, target_dims, target_coords)


def resample(
    source_data: xr.DataArray | xr.Dataset,
    target_grid: xr.DataArray | xr.Dataset,
    method: str = "nearest",
    **kwargs: t.Any,
) -> xr.DataArray | xr.Dataset:
    """Remap source data onto the target grid with AXIS, preserving input type.

    Methods include nearest, bilinear (linear), conservative, bicubic, patch,
    and conservative2nd. Dask-backed fields stay lazy; grid coordinates and
    interpolation weights are constructed eagerly. For lazy output, the returned
    xarray object retains the regridder until it is closed or garbage-collected.
    See :func:`create_regridder` for options and reusable weights.
    """
    regridder = create_regridder(source_data, target_grid, method=method, **kwargs)
    out = regridder(source_data)
    if isinstance(out, xr.DataArray):
        is_dask = hasattr(out.data, "__dask_graph__")
    else:
        is_dask = any(hasattr(var.data, "__dask_graph__") for var in out.data_vars.values())
    if is_dask:
        # AXIS workers access weights through a worker-local cache. Keep their
        # owner alive with the lazy xarray result; otherwise the temporary
        # regridder is finalized before the graph computes and clears that cache.
        out.set_close(regridder.close)
    return out


def interpolate_vertical(
    data: xr.DataArray | xr.Dataset,
    target_levels,
    level_dim: str = "level",
    tension: float = 0.0,
) -> xr.DataArray | xr.Dataset:
    """Interpolate vertical profiles with AXIS's TSPACK spline solver.

    The complete vertical column is passed to each AXIS task; independent
    dimensions remain available for Dask parallelism.
    """
    _require_axis()
    if level_dim not in data.dims:
        raise ValueError(f"Vertical dimension {level_dim!r} is not present in the input")

    regridder = axis.VerticalRegridder(tension=tension)

    def _interpolate(da: xr.DataArray) -> xr.DataArray:
        if level_dim not in da.dims:
            return da
        if level_dim not in da.coords:
            raise ValueError(f"Input must provide source vertical levels as coordinate {level_dim!r}")

        original_dims = da.dims
        other_dims = tuple(dim for dim in original_dims if dim != level_dim)
        ordered = da.transpose(*other_dims, level_dim)
        source_levels = da[level_dim]
        # AXIS expects the vertical core dimension last, including for hybrid
        # coordinates that vary across horizontal columns.
        if level_dim in source_levels.dims:
            source_levels = source_levels.transpose(*(dim for dim in other_dims if dim in source_levels.dims), level_dim)

        target_dim = f"_monet_axis_target_{level_dim}"
        while target_dim in da.dims or target_dim in da.coords:
            target_dim = f"_{target_dim}"
        if isinstance(target_levels, xr.DataArray):
            target = target_levels
            if level_dim in target.dims:
                target = target.rename({level_dim: target_dim})
            elif target.ndim == 1:
                target = target.rename({target.dims[0]: target_dim})
            else:
                raise ValueError("target_levels must be one-dimensional or use level_dim as its vertical dimension")
        else:
            target_values = np.asarray(target_levels)
            if target_values.ndim != 1:
                raise ValueError("target_levels must be one-dimensional or an xarray DataArray")
            target = xr.DataArray(target_values, dims=(target_dim,))

        result = regridder.interpolate(ordered, source_levels, target)
        if target_dim in result.dims:
            result = result.rename({target_dim: level_dim})
        return result.transpose(*original_dims)

    if isinstance(data, xr.DataArray):
        return _interpolate(data)

    variables = {
        name: _interpolate(variable) if level_dim in variable.dims else variable for name, variable in data.data_vars.items()
    }
    coords = {name: coord for name, coord in data.coords.items() if level_dim not in coord.dims}
    return xr.Dataset(variables, coords=coords, attrs=dict(data.attrs))


def _horizontal_dims(obj: xr.DataArray | xr.Dataset) -> tuple[str, ...]:
    """Return spatial core dimensions in AXIS's latitude-then-longitude order."""
    from .conventions import find_coords

    lat = find_coords(obj, "latitude")
    lon = find_coords(obj, "longitude")
    if lat is None or lon is None:
        raise KeyError("AXIS could not detect latitude and longitude coordinates")
    dims = tuple(dict.fromkeys((*lat.dims, *lon.dims)))
    if not dims:
        raise ValueError("Grid coordinates must have at least one dimension")
    return dims


def resample_vector(
    u: xr.DataArray,
    v: xr.DataArray,
    target_grid: xr.DataArray | xr.Dataset,
    method: str = "bilinear",
    *,
    src_alpha=None,
    dst_alpha=None,
    **kwargs: t.Any,
) -> tuple[xr.DataArray, xr.DataArray]:
    """Remap a paired vector field with AXIS, including grid-frame rotation.

    Dask-backed vector components remain lazy. Horizontal source dimensions are
    core dimensions and are rechunked together as needed by the vector solver.
    """
    _require_axis()
    if not isinstance(u, xr.DataArray) or not isinstance(v, xr.DataArray):
        raise TypeError("Vector components u and v must be xarray.DataArray objects")
    if u.dims != v.dims or u.shape != v.shape:
        raise ValueError("Vector components u and v must have matching dimensions and shapes")
    if method.lower() not in {"bilinear", "nearest"}:
        raise ValueError("AXIS vector regridding supports only 'bilinear' and 'nearest'")

    source_dims = _horizontal_dims(u)
    target_dims = _horizontal_dims(target_grid)
    if not set(source_dims).issubset(u.dims):
        raise ValueError("Source grid coordinate dimensions must be present in both vector components")
    dst_spatial_shape = tuple(target_grid.sizes[dim] for dim in target_dims)
    internal_dst_dims = tuple(f"_axis_vector_dst_{i}" for i in range(len(target_dims)))

    options = dict(kwargs)
    vector_regridder = axis.VectorRegridder(
        u,
        target_grid,
        method=method,
        src_alpha=src_alpha,
        dst_alpha=dst_alpha,
        **options,
    )

    def _transform(u_block, v_block):
        return vector_regridder.transform(u_block, v_block)

    u_out, v_out = xr.apply_ufunc(
        _transform,
        u,
        v,
        input_core_dims=[list(source_dims), list(source_dims)],
        output_core_dims=[list(internal_dst_dims), list(internal_dst_dims)],
        dask="parallelized",
        vectorize=False,
        output_dtypes=[np.float64, np.float64],
        dask_gufunc_kwargs={
            "output_sizes": dict(zip(internal_dst_dims, dst_spatial_shape)),
            "allow_rechunk": True,
        },
    )

    rename = dict(zip(internal_dst_dims, target_dims))
    u_out = u_out.rename(rename)
    v_out = v_out.rename(rename)
    target_coords = {name: coord for name, coord in target_grid.coords.items() if set(coord.dims).issubset(set(target_dims))}
    from .conventions import find_coords

    for key in ("latitude", "longitude"):
        coord = find_coords(target_grid, key)
        if coord is not None and coord.name is not None:
            target_coords[coord.name] = coord
    u_out = u_out.assign_coords(target_coords)
    v_out = v_out.assign_coords(target_coords)
    u_out.name, v_out.name = u.name, v.name
    u_out.attrs, v_out.attrs = dict(u.attrs), dict(v.attrs)
    from .conventions import update_history

    update_history(u_out, f"Vector regridded via AXIS (method={method.lower()})")
    update_history(v_out, f"Vector regridded via AXIS (method={method.lower()})")
    return u_out, v_out


def resample_stratify(
    da: xr.DataArray,
    levels,
    vertical: xr.DataArray | str,
    axis: int = 1,
    tension: float = 0.0,
) -> xr.DataArray:
    """Deprecated: use the xarray accessor's ``interpolate_vertical`` method.

    Parameters
    ----------
    da : xarray.DataArray
        The data to interpolate.
    levels : array-like
        Target vertical level values.
    vertical : xarray.DataArray or str
        Vertical coordinate DataArray or coordinate name.
    axis : int, default 1
        Axis index used only to infer ``level_dim`` when ``vertical`` is a DataArray
        without a name.
    tension : float, default 0.0
        Tension factor for the spline interpolation.

    Returns
    -------
    xarray.DataArray
        Data interpolated to the new vertical levels.
    """
    import warnings

    warnings.warn(
        "resample_stratify() is deprecated. Use .monet.interpolate_vertical() "
        "instead of pytspack.interpolate_vertical() directly.",
        DeprecationWarning,
        stacklevel=2,
    )

    if isinstance(vertical, str):
        level_dim = vertical
    elif isinstance(vertical, xr.DataArray) and vertical.name:
        level_dim = vertical.name
    else:
        level_dim = da.dims[axis]
        if not isinstance(vertical, str) and isinstance(vertical, xr.DataArray):
            da = da.assign_coords({level_dim: vertical})

    return interpolate_vertical(da, levels, level_dim=level_dim, tension=tension)
