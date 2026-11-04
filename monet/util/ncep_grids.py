"""Access AXIS's named NCEP grid meshes."""

from __future__ import annotations

import numbers
import tempfile
from itertools import islice
from pathlib import Path

import numpy as np
import xarray as xr


def _read_gmsh_rows(lines, count: int, width: int, *, dtype=np.float64) -> np.ndarray:
    """Read fixed-width Gmsh ASCII rows in chunks using NumPy's C parser."""
    chunks = []
    remaining = count
    while remaining:
        batch_size = min(remaining, 100_000)
        chunk = np.fromstring(" ".join(islice(lines, batch_size)), sep=" ", dtype=dtype)
        if chunk.size != batch_size * width:
            raise ValueError("Unexpected Gmsh row format")
        chunks.append(chunk.reshape(batch_size, width))
        remaining -= batch_size
    return np.concatenate(chunks, axis=0) if len(chunks) > 1 else chunks[0]


def _mesh_to_xarray(axis, mesh, grid_id: int, *, shape: tuple[int, int] | None = None) -> xr.Dataset:
    """Export AXIS cell centers as a coordinate-based xarray Dataset."""
    with tempfile.TemporaryDirectory(prefix="monet-ncep-grid-") as temp_dir:
        mesh_path = Path(temp_dir, "mesh.msh")
        axis.write_gmsh(str(mesh_path), mesh)

        node_coords: np.ndarray | None = None
        face_nodes: np.ndarray | None = None
        with mesh_path.open(encoding="ascii") as stream:
            lines = iter(stream)
            for line in lines:
                marker = line.strip()
                if marker == "$Nodes":
                    node_count = int(next(lines))
                    node_rows = _read_gmsh_rows(lines, node_count, 4)
                    node_coords = node_rows[:, 1:3]
                elif marker == "$Elements":
                    element_count = int(next(lines))
                    first = next(lines).split()
                    stride = len(first)
                    first_row = np.asarray(first, dtype=np.int64)[None, :]
                    element_rows = (
                        np.vstack(
                            [
                                first_row,
                                _read_gmsh_rows(lines, element_count - 1, stride, dtype=np.int64),
                            ]
                        )
                        if element_count > 1
                        else first_row
                    )
                    element_types = element_rows[:, 1]
                    tag_counts = element_rows[:, 2]
                    if (
                        np.all(np.isin(element_types, (2, 3)))
                        and np.all(element_types == element_types[0])
                        and np.all(tag_counts == tag_counts[0])
                    ):
                        first_vertex = 3 + int(tag_counts[0])
                        face_nodes = element_rows[:, first_vertex:] - 1
                    else:
                        # Retain support for mixed or non-quad AXIS exports.
                        face_nodes = np.asarray(
                            [row[3 + int(row[2]) :] - 1 for row in element_rows if row[1] in (2, 3)],
                            dtype=object,
                        )

    if node_coords is None:
        raise ValueError(f"AXIS returned an empty mesh for NCEP grid {grid_id}")
    if face_nodes is None or len(face_nodes) != mesh.n_cells:
        raise ValueError(
            f"AXIS mesh export for NCEP grid {grid_id} contained {len(face_nodes)} triangular/quadrilateral faces; "
            f"expected {mesh.n_cells}"
        )

    if face_nodes.dtype == object:
        raise ValueError("AXIS exported mixed face sizes; xarray conversion requires uniform faces")
    face_node_coords = node_coords[face_nodes]
    face_lon_bounds = np.rad2deg(np.unwrap(np.deg2rad(face_node_coords[:, :, 0]), axis=1))
    face_lat_bounds = face_node_coords[:, :, 1]
    lon_cell = (face_lon_bounds.mean(axis=1) + 180.0) % 360.0 - 180.0
    lat_cell = np.clip(face_lat_bounds.mean(axis=1), -90.0, 90.0)

    # Mesh exposes counts but not its structured layout. AXIS can detect the
    # grid dimensions; Gmsh face order is row-major with longitude varying fast.
    if shape is None:
        layout = axis.detect_rectilinear_grid(mesh)
        ni, nj = layout.get("ni"), layout.get("nj")
        if not ni or not nj or ni * nj != len(face_nodes):
            layout = axis.detect_regular_grid(mesh)
            ni, nj = layout.get("ni"), layout.get("nj")
    else:
        ni, nj = shape
    if not ni or not nj or ni * nj != len(face_nodes):
        raise ValueError(
            f"AXIS does not expose a structured shape for NCEP grid {grid_id}; use as_xarray=False to receive its native mesh"
        )

    lon_2d = lon_cell.reshape(nj, ni)
    lat_2d = lat_cell.reshape(nj, ni)
    lon_axis = np.nanmean(lon_2d, axis=0)
    lat_axis = np.nanmean(lat_2d, axis=1)
    rectilinear = np.allclose(lon_2d, lon_axis[None, :], atol=1.0e-7) and np.allclose(lat_2d, lat_axis[:, None], atol=1.0e-7)
    prefix = "F" if shape is not None else "G"
    attrs = {"ncep_grib1_grid_id": grid_id, "grid_name": f"{prefix}{grid_id:03d}"}
    if rectilinear:
        result = xr.Dataset(
            coords={
                "lon": ("lon", lon_axis, {"standard_name": "longitude", "units": "degrees_east"}),
                "lat": ("lat", lat_axis, {"standard_name": "latitude", "units": "degrees_north"}),
            },
            attrs=attrs,
        )
    else:
        result = xr.Dataset(
            coords={
                "lon": (("y", "x"), lon_2d, {"standard_name": "longitude", "units": "degrees_east"}),
                "lat": (("y", "x"), lat_2d, {"standard_name": "latitude", "units": "degrees_north"}),
            },
            attrs=attrs,
        )

    # Keep AXIS's actual cell topology alongside the familiar coordinate grid.
    # The connectivity is also the authoritative polygon bound for each face.
    max_nodes = face_nodes.shape[1]
    fill_value = -1
    connectivity = face_nodes.astype(np.int32, copy=False)
    result = result.assign_coords(
        node=("node", np.arange(len(node_coords), dtype=np.int32)),
        face=("face", np.arange(len(face_nodes), dtype=np.int32)),
        max_face_nodes=("max_face_nodes", np.arange(max_nodes, dtype=np.int32)),
    )
    result["node_lon"] = xr.DataArray(node_coords[:, 0], dims="node", attrs={"standard_name": "longitude", "units": "degrees_east"})
    result["node_lat"] = xr.DataArray(node_coords[:, 1], dims="node", attrs={"standard_name": "latitude", "units": "degrees_north"})
    result["face_lon"] = xr.DataArray(
        lon_cell,
        dims="face",
        attrs={"standard_name": "longitude", "units": "degrees_east", "bounds": "face_lon_bounds"},
    )
    result["face_lat"] = xr.DataArray(
        lat_cell,
        dims="face",
        attrs={"standard_name": "latitude", "units": "degrees_north", "bounds": "face_lat_bounds"},
    )
    result["face_lon_bounds"] = xr.DataArray(face_lon_bounds, dims=("face", "max_face_nodes"), attrs={"units": "degrees_east"})
    result["face_lat_bounds"] = xr.DataArray(face_lat_bounds, dims=("face", "max_face_nodes"), attrs={"units": "degrees_north"})
    result["face_node_connectivity"] = xr.DataArray(
        connectivity,
        dims=("face", "max_face_nodes"),
        attrs={"cf_role": "face_node_connectivity", "start_index": 0, "_FillValue": fill_value},
    )
    result["mesh"] = xr.DataArray(
        np.int32(0),
        attrs={
            "cf_role": "mesh_topology",
            "long_name": "NCEP grid mesh topology",
            "topology_dimension": 2,
            "node_coordinates": "node_lon node_lat",
            "face_node_connectivity": "face_node_connectivity",
            "face_coordinates": "face_lon face_lat",
        },
    )
    return result


def get_ncep_grid(grid_id: int | str, *, as_xarray: bool = True):
    """Return an AXIS named NCEP grid as an xarray Dataset or AXIS mesh.

    AXIS creates the named mesh. By default, MONET exports face cell-center
    coordinates and CF/UGRID face topology as an xarray ``Dataset`` suitable
    for MONET's grid-aware APIs. The UGRID connectivity preserves the exact
    AXIS cell polygons. Set ``as_xarray=False`` to receive AXIS's native
    ``axis.Mesh``.

    Parameters
    ----------
    grid_id : int or str
        Table B grid number. Strings may use the ``G`` prefix (for example,
        ``"G003"``); AXIS receives the corresponding ``grid3`` name.
    as_xarray : bool, default: True
        Return an xarray Dataset instead of the native AXIS mesh.

    Raises
    ------
    ImportError
        If AXIS is not installed.
    ValueError
        If AXIS does not provide a named mesh for this grid identifier, or
        cannot provide a structured cell layout for the xarray form.
    """
    try:
        import axis
    except (ImportError, OSError) as exc:
        raise ImportError("AXIS (axis-regrid) is required to create named NCEP grids") from exc

    if isinstance(grid_id, bool):
        raise TypeError("grid_id must be an integer or a G-prefixed grid number")
    if isinstance(grid_id, numbers.Integral):
        number = int(grid_id)
    elif isinstance(grid_id, str):
        value = grid_id.strip().upper()
        if value.startswith("G"):
            value = value[1:]
        if not value.isdigit():
            raise ValueError(f"Invalid NCEP Table B grid identifier: {grid_id!r}")
        number = int(value)
    else:
        raise TypeError("grid_id must be an integer or a G-prefixed grid number")

    if number < 0:
        raise ValueError(f"Invalid NCEP Table B grid identifier: {grid_id!r}")
    try:
        mesh = axis.make_named_mesh(f"grid{number}")
    except (ValueError, RuntimeError) as exc:
        raise ValueError(f"AXIS does not provide a named mesh for NCEP Table B grid {number}") from exc
    return _mesh_to_xarray(axis, mesh, number) if as_xarray else mesh


def get_guassian_grid(grid_id: int | str, *, as_xarray: bool = True):
    """Return an AXIS named Gaussian grid as an xarray Dataset or AXIS mesh.

    AXIS creates the named mesh. By default, MONET exports face cell-center
    coordinates and CF/UGRID face topology as an xarray ``Dataset`` suitable
    for MONET's grid-aware APIs. The UGRID connectivity preserves the exact
    AXIS cell polygons. Set ``as_xarray=False`` to receive AXIS's native
    ``axis.Mesh``.

    Parameters
    ----------
    grid_id : int or str
        Table B grid number. Strings may use the ``F`` prefix (for example,
        ``"F720"``); AXIS receives the corresponding ``F720`` name.
    as_xarray : bool, default: True
        Return an xarray Dataset instead of the native AXIS mesh.

    Raises
    ------
    ImportError
        If AXIS is not installed.
    ValueError
        If AXIS does not provide a named mesh for this grid identifier, or
        cannot provide a structured cell layout for the xarray form.
    """
    try:
        import axis
    except (ImportError, OSError) as exc:
        raise ImportError("AXIS (axis-regrid) is required to create named Gaussian grids") from exc

    if isinstance(grid_id, bool):
        raise TypeError("grid_id must be an integer or a F-prefixed grid number")
    if isinstance(grid_id, numbers.Integral):
        number = int(grid_id)
    elif isinstance(grid_id, str):
        value = grid_id.strip().upper()
        if value.startswith("F"):
            value = value[1:]
        if not value.isdigit():
            raise ValueError(f"Invalid NCEP Table B grid identifier: {grid_id!r}")
        number = int(value)
    else:
        raise TypeError("grid_id must be an integer or a F-prefixed grid number")

    if number < 0:
        raise ValueError(f"Invalid Gaussian grid identifier: {grid_id!r}")
    try:
        mesh = axis.make_named_mesh(f"F{number}")
    except (ValueError, RuntimeError) as exc:
        raise ValueError(f"AXIS does not provide a named mesh for NCEP Table B grid {number}") from exc
    # F-grid Gaussian meshes have 2N latitude rows and 4N longitude points;
    # the AXIS faces connect adjacent rows, so there is one fewer face row.
    return _mesh_to_xarray(axis, mesh, number, shape=(4 * number, 2 * number - 1)) if as_xarray else mesh


# Preserve the original misspelling for compatibility while exposing the
# correctly spelled public name.
get_gaussian_grid = get_guassian_grid
