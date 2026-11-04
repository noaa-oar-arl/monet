"""MONET horizontal and vertical regridding with AXIS."""

import atexit
import logging
import typing as t
import warnings
import weakref

import numpy as np
import xarray as xr

logger = logging.getLogger(__name__)
_LIVE_REGRIDDERS = weakref.WeakSet()

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


@atexit.register
def _close_regridders():
    """Release MONET-owned native matrices before Python modules are torn down."""
    for regridder in list(_LIVE_REGRIDDERS):
        try:
            regridder.close()
        except Exception:  # cleanup must not mask interpreter shutdown
            logger.debug("Could not close MONET's AXIS regridder", exc_info=True)


AXIS_INSTALL_MESSAGE = (
    "AXIS (axis-regrid) is required for horizontal regridding. "
    "Install with: pip install 'git+https://github.com/NOAA-EMC/HELM.git@"
    "dda4cb5cb2b05f082c8637c4ad594fb74d8f8671#subdirectory=libs/axis'. "
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


def _degenerate_point_coord_pair(ds):
    """Return (lat, lon) when 2-D coordinates collapse through a size-1 dimension.

    Single cells or trajectory lists stored as shapes such as (1, 1) or (N, 1)
    build zero-area curvilinear cells, which AXIS's strict mesh validation
    rejects. Treat them as point lists so the node-triangle adapter applies.
    """

    def _find(standard_name, axis, names):
        coord = next((ds[c] for c in ds.coords if ds[c].attrs.get("standard_name") == standard_name), None)
        if coord is None:
            coord = next((ds[c] for c in ds.coords if ds[c].attrs.get("axis") == axis), None)
        if coord is None:
            coord = next((ds[c] for c in ds.coords if c in names), None)
        return coord

    lat = _find("latitude", "Y", ("lat", "latitude"))
    lon = _find("longitude", "X", ("lon", "longitude"))
    if lat is None or lon is None or lat.dims != lon.dims or lat.ndim != 2:
        return None, None
    if 1 not in lat.shape:
        return None, None
    return lat, lon


def _prepare_grid(obj, side, method):
    """Keep full mesh metadata and normalize rectilinear dimension aliases only."""
    ds = _as_dataset(obj)
    target_point_coords = {}
    point_mesh_adapter = False
    has_topology = any(ds[name].attrs.get("cf_role") == "mesh_topology" for name in ds.variables)
    if not has_topology:
        point_lat, point_lon = _degenerate_point_coord_pair(ds)
        if point_lat is not None:
            point_dim = (
                next(dim for dim, size in point_lat.sizes.items() if size > 1)
                if max(point_lat.sizes.values()) > 1
                else point_lat.dims[1]
            )
            flat_lon = point_lon.isel({dim: 0 for dim, size in point_lon.sizes.items() if dim != point_dim}).values
            flat_lat = point_lat.isel({dim: 0 for dim, size in point_lat.sizes.items() if dim != point_dim}).values
            ds = ds.copy()
            ds = ds.drop_vars([point_lon.name, point_lat.name])
            ds = ds.assign_coords(
                {
                    point_lon.name: (point_dim, flat_lon, {"standard_name": "longitude", "units": "degrees_east"}),
                    point_lat.name: (point_dim, flat_lat, {"standard_name": "latitude", "units": "degrees_north"}),
                }
            )
            ds["_monet_point_mesh"] = xr.DataArray(
                np.int32(0),
                attrs={
                    "cf_role": "mesh_topology",
                    "topology_dimension": 0,
                    "node_coordinates": f"{point_lon.name} {point_lat.name}",
                },
            )
            has_topology = True
    if not has_topology:
        # Selecting a field from a node-based UGRID Dataset drops its sibling
        # mesh-topology variable, while the field retains mesh/location attrs.
        # Recreate the node-only topology from those attrs and its coordinates.
        point_field = next(
            (ds[name] for name in ds.data_vars if ds[name].attrs.get("location") == "node" and ds[name].attrs.get("mesh")),
            None,
        )
        if point_field is not None:
            point_coords = {}
            for standard, names in (
                ("longitude", ("longitude", "lon", "lon_node")),
                ("latitude", ("latitude", "lat", "lat_node")),
            ):
                coord = next(
                    (ds[name] for name in ds.coords if ds[name].attrs.get("standard_name") == standard),
                    None,
                )
                if coord is None:
                    coord = next((ds[name] for name in names if name in ds), None)
                if coord is not None:
                    point_coords[standard] = coord
            if (
                len(point_coords) == 2
                and point_coords["longitude"].ndim == 1
                and point_coords["latitude"].dims == point_coords["longitude"].dims
            ):
                mesh_name = str(point_field.attrs["mesh"])
                if mesh_name not in ds:
                    ds[mesh_name] = xr.DataArray(
                        np.int32(0),
                        attrs={
                            "cf_role": "mesh_topology",
                            "topology_dimension": 0,
                            "node_coordinates": (f"{point_coords['longitude'].name} {point_coords['latitude'].name}"),
                        },
                    )
    for name in ds.variables:
        if ds[name].attrs.get("cf_role") != "mesh_topology":
            continue
        attrs = ds[name].attrs
        node_names = attrs.get("node_coordinates", "").split()
        if attrs.get("face_node_connectivity"):
            continue
        if len(node_names) < 2 or any(coord not in ds for coord in node_names[:2]):
            continue
        node_lon, node_lat = ds[node_names[0]], ds[node_names[1]]
        if node_lon.ndim != 1 or node_lat.dims != node_lon.dims:
            continue
        if method in {"conservative", "conservative2nd"}:
            raise ValueError("Node-only UGRID point data cannot be used with conservative remapping methods")

        # AXIS consumes cell meshes. Represent each point as a zero-area
        # triangle so its interpolation weights land exactly at the station.
        # This is only a geometry adapter; the caller's node dimension is kept
        # as the output cell dimension, and the original point metadata remains.
        point_dim = node_lon.dims[0]
        point_count = node_lon.size
        original_point_coords = {coord_name: coord for coord_name, coord in ds.coords.items() if point_dim in coord.dims}
        node_dim = "_monet_point_mesh_node"
        vertex_dim = "_monet_point_mesh_vertex"
        face_lon_name, face_lat_name = "_monet_point_lon", "_monet_point_lat"
        conn_name = "_monet_point_face_nodes"
        ds = ds.copy()
        for coord in (node_lon.name, node_lat.name):
            if coord in ds.coords:
                ds[coord].attrs = {
                    key: value for key, value in ds[coord].attrs.items() if key not in {"standard_name", "axis", "mesh", "location"}
                }
        # Use separate node variables, with each node repeated three times.
        ds["_monet_point_node_lon"] = xr.DataArray(
            np.repeat(node_lon.values, 3),
            dims=node_dim,
            attrs={"standard_name": "longitude", "units": node_lon.attrs.get("units", "degrees_east")},
        )
        ds["_monet_point_node_lat"] = (node_dim, np.repeat(node_lat.values, 3))
        ds[face_lon_name] = xr.DataArray(
            np.asarray(node_lon.values),
            dims=point_dim,
            attrs={"standard_name": "longitude", "units": node_lon.attrs.get("units", "degrees_east")},
        )
        ds[face_lat_name] = xr.DataArray(
            np.asarray(node_lat.values),
            dims=point_dim,
            attrs={"standard_name": "latitude", "units": node_lat.attrs.get("units", "degrees_north")},
        )
        ds = ds.set_coords([face_lon_name, face_lat_name])
        ds[conn_name] = xr.DataArray(
            np.arange(point_count * 3, dtype=np.int64).reshape(point_count, 3),
            dims=(point_dim, vertex_dim),
            attrs={"cf_role": "face_node_connectivity", "start_index": 0},
        )
        ds[name].attrs = {
            "cf_role": "mesh_topology",
            "topology_dimension": 2,
            "node_coordinates": "_monet_point_node_lon _monet_point_node_lat",
            "face_node_connectivity": conn_name,
            "face_coordinates": f"{face_lon_name} {face_lat_name}",
        }
        point_mesh_adapter = True
        if side == "target":
            target_point_coords = original_point_coords
        else:
            target_point_coords = {}

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
    restore_coords.update(target_point_coords)
    if point_mesh_adapter:
        ds.attrs["_monet_point_mesh_adapter"] = True
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


def _longitude_coord(ds):
    for name in ds.coords:
        if ds[name].attrs.get("standard_name") == "longitude":
            return ds[name]
    for name in ("lon", "longitude", "lonCell", "lon_face", "lon_node"):
        if name in ds:
            return ds[name]
    return None


def _shift_point_longitudes(points, grid):
    """Move point longitudes by a whole turn when a target grid uses another convention."""
    point_lon = _longitude_coord(points)
    grid_lon = _longitude_coord(grid)
    if point_lon is None or grid_lon is None:
        return points
    point_values = np.asarray(point_lon.values, dtype=np.float64)
    grid_values = np.asarray(grid_lon.values, dtype=np.float64)
    point_values = point_values[np.isfinite(point_values)]
    grid_values = grid_values[np.isfinite(grid_values)]
    if not point_values.size or not grid_values.size:
        return points
    point_center = (point_values.min() + point_values.max()) / 2.0
    grid_center = (grid_values.min() + grid_values.max()) / 2.0
    offset = 360.0 * round((grid_center - point_center) / 360.0)
    if not offset:
        return points
    shifted_min = point_values.min() + offset
    shifted_max = point_values.max() + offset
    if shifted_max < grid_values.min() or shifted_min > grid_values.max():
        return points

    points = points.copy()
    for name in points.variables:
        variable = points[name]
        if variable.attrs.get("standard_name") == "longitude":
            points[name] = variable.copy(data=np.asarray(variable.values, dtype=np.float64) + offset)
            points[name].attrs = dict(variable.attrs)
    return points


class PersistentRegridder:
    """Reusable AXIS weights with MONET type, dimension, and history conventions.

    Construct with :func:`create_regridder`. Only apply to fields on the same
    source grid; weight files do not encode or validate the grid/method identity.
    Closing releases the engine and makes this regridder unusable. Any owners
    still alive at Python shutdown are closed before native module teardown.
    """

    def __init__(self, engine, source_grid, target_grid, method, source_dims, target_dims, target_coords):
        self._engine = engine
        self._source_dims = source_dims
        self._target_dims = target_dims
        self._target_coords = target_coords
        self.method = method
        self.source_grid_ds = source_grid
        self.target_grid_ds = target_grid
        _LIVE_REGRIDDERS.add(self)

    def __call__(self, obj):
        from .conventions import update_history

        if self._engine is None:
            raise RuntimeError("This MONET regridder is closed. Create a new regridder before applying weights.")
        src = _as_dataset(obj)
        for new, old in self._source_dims.items():
            if old in src.dims:
                if new not in src.coords:
                    src = src.assign_coords({new: self.source_grid_ds[new].rename({new: old})})
                src = src.swap_dims({old: new}) if new in src.coords else src.rename_dims({old: new})
        # Keep dtype promotion and dimension normalization local to this call.
        src = src.copy()
        was_da = isinstance(obj, xr.DataArray)
        field_name = next(iter(src.data_vars)) if was_da else None
        # AXIS's native batch_apply kernels accept float64 arrays only. Promote
        # spatial numeric fields before passing them through the backend; retain
        # the original precision for floating-point outputs and leave integer
        # interpolation results as float64 to avoid truncating weighted values.
        spatial_dims = tuple(self._engine._dims_source)
        source_dtypes = {}
        non_numeric_spatial_vars = []
        for name, variable in src.data_vars.items():
            if not all(dim in variable.dims for dim in spatial_dims):
                continue
            dtype = variable.dtype
            if np.issubdtype(dtype, np.number) or np.issubdtype(dtype, np.bool_):
                source_dtypes[name] = dtype
                if dtype != np.dtype(np.float64):
                    src[name] = variable.astype(np.float64)
            else:
                # Point observations often include station labels on the same
                # node dimension as measured values. They are target metadata,
                # not interpolated fields, and AXIS's sparse kernels accept
                # numeric arrays only.
                non_numeric_spatial_vars.append(name)
        engine_input = src.drop_vars(non_numeric_spatial_vars) if non_numeric_spatial_vars else src
        out = self._engine(engine_input)
        for name, dtype in source_dtypes.items():
            if name in out and np.issubdtype(dtype, np.floating) and dtype != np.dtype(np.float64):
                out[name] = out[name].astype(dtype)
        # AXIS carries non-spatial variables through. Source topology describes
        # the old grid and must be replaced before the output can be remapped again.
        source_mesh = _mesh_variables(self.source_grid_ds)
        point_target = self.target_grid_ds.attrs.get("_monet_point_mesh_adapter", False)
        target_mesh = set() if point_target else _mesh_variables(self.target_grid_ds)
        out = out.drop_vars(list(source_mesh - target_mesh), errors="ignore")
        if point_target:
            out = out.drop_vars(
                [name for name in out.variables if name.startswith("_monet_point_")],
                errors="ignore",
            )
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
        if self._engine is None:
            raise RuntimeError("This MONET regridder is closed. Create a new regridder before saving weights.")
        return self._engine.to_file(str(path))

    def close(self):
        """Release this owner's engine, native weights, and worker registrations."""
        if self._engine is not None:
            try:
                self._engine.close()
            finally:
                # AXIS.close() clears worker caches but retains its Matrix.
                # Drop our engine reference even when the wrapper remains
                # reachable through an xarray close callback at shutdown.
                self._engine = None
                _LIVE_REGRIDDERS.discard(self)


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
    src, source_dims, _ = _prepare_grid(source_grid, "source", real_method)
    dst, target_dims, target_coords = _prepare_grid(target_grid, "target", real_method)
    if src.attrs.get("_monet_point_mesh_adapter"):
        src = _shift_point_longitudes(src, dst)
    if dst.attrs.get("_monet_point_mesh_adapter"):
        dst = _shift_point_longitudes(dst, src)
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
    Close the result after its lazy work finishes to release weights promptly;
    MONET also releases its remaining regridders at Python shutdown.
    See :func:`create_regridder` for options and reusable weights.
    """
    regridder = create_regridder(source_data, target_grid, method=method, **kwargs)
    try:
        out = regridder(source_data)
    except BaseException:
        try:
            regridder.close()
        except Exception:
            logger.debug("Could not close MONET's AXIS regridder after failure", exc_info=True)
        raise
    if isinstance(out, xr.DataArray):
        is_dask = hasattr(out.data, "__dask_graph__")
    else:
        is_dask = any(hasattr(var.data, "__dask_graph__") for var in out.data_vars.values())
    if is_dask:
        # AXIS workers access weights through a worker-local cache. Keep their
        # owner alive with the lazy xarray result; otherwise the temporary
        # regridder is finalized before the graph computes and clears that cache.
        out.set_close(regridder.close)
    else:
        regridder.close()
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
        # AXIS solves whole vertical columns per task: the vertical core
        # dimension must be a single chunk, in both the field and any hybrid
        # level coordinate. Batch dimensions keep their own chunking.
        if hasattr(ordered.data, "__dask_graph__") and len(ordered.chunks[ordered.dims.index(level_dim)]) > 1:
            ordered = ordered.chunk({level_dim: -1})
        if level_dim in source_levels.dims:
            if hasattr(source_levels.data, "__dask_graph__") and len(source_levels.chunks[source_levels.dims.index(level_dim)]) > 1:
                source_levels = source_levels.chunk({level_dim: -1})
            # AXIS expects the vertical core dimension last, including for hybrid
            # coordinates that vary across horizontal columns.
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
        "resample_stratify() is deprecated. Use .monet.interpolate_vertical() instead of pytspack.interpolate_vertical() directly.",
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
