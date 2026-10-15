"""Small numerical checks for advanced AXIS grid detection through MONET."""

import numpy as np
import pytest
import xarray as xr

import monet  # noqa: F401
from monet.util.resample import has_axis

pytestmark = pytest.mark.skipif(not has_axis, reason="AXIS not available")


def test_ugrid_bilinear():
    # Cell-centered field over two triangles. Metadata stays on the Dataset.
    src = xr.Dataset(
        {
            "mesh": (
                (),
                0,
                {
                    "cf_role": "mesh_topology",
                    "topology_dimension": 2,
                    "node_coordinates": "lon_node lat_node",
                    "face_node_connectivity": "faces",
                },
            ),
            "faces": (("face", "nv"), [[0, 1, 2], [1, 3, 2]], {"start_index": 0}),
            "field": ("face", [10.0, 10.0], {"mesh": "mesh", "location": "face"}),
        },
        coords={
            "lon_node": ("node", [0.0, 2.0, 0.0, 2.0]),
            "lat_node": ("node", [0.0, 0.0, 2.0, 2.0]),
            "lon_face": ("face", [2 / 3, 4 / 3], {"standard_name": "longitude"}),
            "lat_face": ("face", [2 / 3, 4 / 3], {"standard_name": "latitude"}),
        },
    )
    dst = xr.Dataset(coords={"lat": [0.5, 1.5], "lon": [0.5, 1.5]})
    out = dst.monet.remap(src, method="bilinear")
    assert out.field.dims == ("lat", "lon")
    np.testing.assert_allclose(out.field, 10.0, atol=1e-8)
    assert "mesh" not in out and "mesh" not in out.field.attrs
    back = src.monet.remap(out)
    assert "mesh" in back
    assert back.field.dims == ("face",)
    np.testing.assert_allclose(back.field, 10.0, atol=1e-8)


def test_mpas_bilinear():
    src = xr.Dataset(
        {
            "latVertex": ("nVertices", [0.0, 0.0, 2.0, 2.0]),
            "lonVertex": ("nVertices", [0.0, 2.0, 0.0, 2.0]),
            "verticesOnCell": (("nCells", "maxVertices"), [[1, 2, 3], [2, 4, 3]]),
            "nEdgesOnCell": ("nCells", [3, 3]),
            "latCell": ("nCells", [2 / 3, 4 / 3]),
            "lonCell": ("nCells", [2 / 3, 4 / 3]),
            "field": ("nCells", [10.0, 10.0]),
        }
    )
    dst = xr.Dataset(coords={"lat": [0.75, 1.25], "lon": [0.75, 1.25]})
    out = dst.monet.remap(src, method="bilinear")
    np.testing.assert_allclose(out.field, 10.0, atol=1e-8)


def test_cubed_sphere_tiles():
    lon = np.stack([np.array([[10.0, 20.0], [10.0, 20.0]]) + 40 * t for t in range(6)])
    lat = np.broadcast_to([[10.0, 10.0], [20.0, 20.0]], lon.shape)
    src = xr.Dataset(
        {"field": (("tile", "y", "x"), np.full(lon.shape, 42.0))},
        coords={"lon": (("tile", "y", "x"), lon), "lat": (("tile", "y", "x"), lat)},
    )
    dst = xr.Dataset(coords={"lat": [15.0], "lon": [55.0]})
    out = dst.monet.remap(src)
    np.testing.assert_allclose(out.field, 42.0)


def test_dateline_nearest():
    src = xr.Dataset(
        {"field": (("lat", "lon"), [[1.0, 2.0, 3.0, 4.0], [1.0, 2.0, 3.0, 4.0]])},
        coords={"lat": [-1.0, 1.0], "lon": [178.0, 179.0, 180.0, 181.0]},
    )
    dst = xr.Dataset(coords={"lat": [0.0], "lon": [179.1, -179.1]})
    out = dst.monet.remap(src, periodic=True)
    np.testing.assert_allclose(out.field, [[2.0, 4.0]])
