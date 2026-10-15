"""MONET horizontal regridding with AXIS; vertical interpolation uses pytspack."""

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
    interpolation weights are constructed eagerly. See :func:`create_regridder`
    for options and reusable weights.
    """
    return create_regridder(source_data, target_grid, method=method, **kwargs)(source_data)


def resample_stratify(
    da: xr.DataArray,
    levels,
    vertical: xr.DataArray | str,
    axis: int = 1,
    tension: float = 0.0,
) -> xr.DataArray:
    """Deprecated: use ``pytspack.interpolate_vertical`` directly.

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

    from pytspack import interpolate_vertical

    warnings.warn(
        "resample_stratify() is deprecated. Use pytspack.interpolate_vertical() directly.",
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

    if isinstance(levels, xr.DataArray):
        levels = levels.values

    # pytspack requires a single chunk along the core (vertical) dimension
    if hasattr(da.data, "chunks") and level_dim in da.dims:
        da = da.chunk({level_dim: -1})

    return interpolate_vertical(da, np.asarray(levels), level_dim=level_dim, tension=tension)
