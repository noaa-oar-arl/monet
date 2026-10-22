"""Access AXIS's named NCEP grid meshes."""

from __future__ import annotations

import numbers


def get_ncep_grid(grid_id: int | str):
    """Return an AXIS mesh for a named NCEP GRIB1 Table B grid.

    The mesh is created by AXIS's named-grid registry, so MONET does not carry
    a second copy of grid coordinates. The returned ``axis.Mesh`` can be used
    with AXIS's low-level weight generation and application functions. Decoded
    xarray data should continue to use its own coordinates with MONET's normal
    regridding API.

    Parameters
    ----------
    grid_id : int or str
        Table B grid number. Strings may use the ``G`` prefix (for example,
        ``"G003"``); AXIS receives the corresponding ``grid3`` name.

    Raises
    ------
    ImportError
        If AXIS is not installed.
    ValueError
        If AXIS does not provide a named mesh for this grid identifier.
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
        return axis.make_named_mesh(f"grid{number}")
    except (ValueError, RuntimeError) as exc:
        raise ValueError(f"AXIS does not provide a named mesh for NCEP Table B grid {number}") from exc

