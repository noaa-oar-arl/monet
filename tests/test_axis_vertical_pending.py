"""Parity probes for AXIS vertical interpolation against stored references.

The strict expected failures record known differences in the pinned AXIS
solver. Keep the numerical tolerance explicit; do not relax it to hide defects.
"""

import json
from pathlib import Path

import numpy as np
import pytest

from monet.util.resample import has_axis

pytestmark = [
    pytest.mark.skipif(not has_axis, reason="AXIS not available"),
    pytest.mark.xfail(strict=True, reason="Pinned AXIS vertical solver fails pytspack parity; see feature research addendum"),
]


def test_axis_vertical_linear_profile():
    import axis

    actual = axis.VerticalRegridder().interpolate(np.array([0.0, 1.0, 2.0]), np.array([0.0, 1.0, 2.0]), np.array([0.25, 0.75]))
    np.testing.assert_allclose(actual, [0.25, 0.75], rtol=1e-6, atol=1e-8)


@pytest.mark.parametrize("descending", [False, True])
@pytest.mark.parametrize("lazy", [False, True])
def test_axis_vertical_pytspack_reference(descending, lazy):
    import axis
    import xarray as xr

    case = json.loads((Path(__file__).parent / "data" / "vertical_reference.json").read_text())["cases"][1]
    data = xr.DataArray(case["field"], dims="pressure", coords={"pressure": case["levels"]})
    if not descending:
        data = data.isel(pressure=slice(None, None, -1))
    if lazy:
        data = data.chunk({"pressure": 2})
    actual = axis.VerticalRegridder(tension=case["tension"]).interpolate(data, data.pressure, np.array(case["targets"]))
    assert (actual.chunks is not None) == lazy
    np.testing.assert_allclose(actual, case["expected"], rtol=3e-2)
