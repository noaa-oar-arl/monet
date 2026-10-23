"""Access AXIS's named NCEP grid meshes."""

from __future__ import annotations

import numbers
import tempfile
from pathlib import Path

import numpy as np
import xarray as xr


def _mesh_to_xarray(axis, mesh, grid_id: int) -> xr.Dataset:
    """Export AXIS cell centers as a coordinate-based xarray Dataset."""
    with tempfile.TemporaryDirectory(prefix="monet-ncep-grid-") as temp_dir:
        mesh_path = Path(temp_dir, "mesh.msh")
        axis.write_gmsh(str(mesh_path), mesh)

        node_coords: np.ndarray | None = None
        node_lookup: dict[int, int] = {}
        face_nodes: list[list[int]] = []
        with mesh_path.open(encoding="ascii") as stream:
            lines = iter(stream)
            for line in lines:
                marker = line.strip()
                if marker == "$Nodes":
                    node_count = int(next(lines))
                    node_coords = np.empty((node_count, 2), dtype=np.float64)
                    for index in range(node_count):
                        node_id, x, y, _z = next(lines).split()
                        node_lookup[int(node_id)] = index
                        node_coords[index] = (float(x), float(y))
                elif marker == "$Elements":
                    element_count = int(next(lines))
                    for _ in range(element_count):
                        values = [int(value) for value in next(lines).split()]
                        _element_id, element_type, tag_count = values[:3]
                        if element_type not in (2, 3):  # Gmsh triangle or quadrilateral
                            continue
                        vertex_ids = values[3 + tag_count :]
                        face_nodes.append([node_lookup[node_id] for node_id in vertex_ids])

    if node_coords is None:
        raise ValueError(f"AXIS returned an empty mesh for NCEP grid {grid_id}")
    if len(face_nodes) != mesh.n_cells:
        raise ValueError(
            f"AXIS mesh export for NCEP grid {grid_id} contained {len(face_nodes)} triangular/quadrilateral faces; "
            f"expected {mesh.n_cells}"
        )

    lon_cell = np.empty(len(face_nodes), dtype=np.float64)
    lat_cell = np.empty(len(face_nodes), dtype=np.float64)
    for face_index, indices in enumerate(face_nodes):
        longitudes = np.rad2deg(np.unwrap(np.deg2rad(node_coords[indices, 0])))
        lon_cell[face_index] = (longitudes.mean() + 180.0) % 360.0 - 180.0
        lat_cell[face_index] = np.clip(node_coords[indices, 1].mean(), -90.0, 90.0)

    # Mesh exposes counts but not its structured layout. AXIS can detect the
    # grid dimensions; Gmsh face order is row-major with longitude varying fast.
    layout = axis.detect_rectilinear_grid(mesh)
    ni, nj = layout.get("ni"), layout.get("nj")
    if not ni or not nj or ni * nj != len(face_nodes):
        layout = axis.detect_regular_grid(mesh)
        ni, nj = layout.get("ni"), layout.get("nj")
    if not ni or not nj or ni * nj != len(face_nodes):
        raise ValueError(
            f"AXIS does not expose a structured shape for NCEP grid {grid_id}; "
            "use as_xarray=False to receive its native mesh"
        )

    lon_2d = lon_cell.reshape(nj, ni)
    lat_2d = lat_cell.reshape(nj, ni)
    lon_axis = np.nanmean(lon_2d, axis=0)
    lat_axis = np.nanmean(lat_2d, axis=1)
    rectilinear = np.allclose(lon_2d, lon_axis[None, :], atol=1.0e-7) and np.allclose(
        lat_2d, lat_axis[:, None], atol=1.0e-7
    )
    attrs = {"ncep_grib1_grid_id": grid_id, "grid_name": f"G{grid_id:03d}"}
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
    max_nodes = max(map(len, face_nodes))
    fill_value = -1
    connectivity = np.full((len(face_nodes), max_nodes), fill_value, dtype=np.int32)
    face_lon_bounds = np.full((len(face_nodes), max_nodes), np.nan, dtype=np.float64)
    face_lat_bounds = np.full((len(face_nodes), max_nodes), np.nan, dtype=np.float64)
    for face_index, indices in enumerate(face_nodes):
        connectivity[face_index, : len(indices)] = indices
        face_lon_bounds[face_index, : len(indices)] = np.rad2deg(
            np.unwrap(np.deg2rad(node_coords[indices, 0]))
        )
        face_lat_bounds[face_index, : len(indices)] = node_coords[indices, 1]
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
    result["face_lon_bounds"] = xr.DataArray(
        face_lon_bounds, dims=("face", "max_face_nodes"), attrs={"units": "degrees_east"}
    )
    result["face_lat_bounds"] = xr.DataArray(
        face_lat_bounds, dims=("face", "max_face_nodes"), attrs={"units": "degrees_north"}
    )
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
