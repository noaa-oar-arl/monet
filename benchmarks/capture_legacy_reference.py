"""Regenerate stored references in an isolated xregrid/ESMPy environment."""

import argparse
import importlib.metadata as md
import json
from pathlib import Path

import numpy as np
import xarray as xr
from xregrid import Regridder

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--source-url", help="Repository URL when importing from a source checkout")
parser.add_argument("--source-commit", help="Exact revision when importing from a source checkout")
args = parser.parse_args()

root = Path(__file__).resolve().parents[1] / "tests" / "data" / "regrid_reference"
root.mkdir(parents=True, exist_ok=True)
for method in ("nearest", "bilinear", "conservative"):
    if method == "conservative":
        lat = np.arange(-82.5, 90, 15.0)
        lon = np.arange(-172.5, 180, 15.0)
        tlat = np.arange(-75, 90, 30.0)
        tlon = np.arange(-165, 180, 30.0)
        values = 2.0 + np.cos(np.deg2rad(lat))[:, None] * np.cos(np.deg2rad(lon))[None, :]
    else:
        lat = np.linspace(30, 40, 5)
        lon = np.linspace(-100, -90, 5)
        tlat = lat[1:-1]
        tlon = lon[1:-1]
        values = 5.0 + lat[:, None] * 0.2 + lon[None, :] * 0.1
    src = xr.Dataset({"field": (("lat", "lon"), values)}, coords={"lat": lat, "lon": lon})
    dst = xr.Dataset(coords={"lat": tlat, "lon": tlon})
    for grid in (src, dst):
        grid.lat.attrs.update(standard_name="latitude", units="degrees_north")
        grid.lon.attrs.update(standard_name="longitude", units="degrees_east")
    engine = Regridder(src, dst, method="nearest_s2d" if method == "nearest" else method)
    out = engine(src)
    src.to_netcdf(root / f"{method}_source.nc")
    dst.to_netcdf(root / f"{method}_target.nc")
    out.to_netcdf(root / f"{method}_expected.nc")
    print(method, out.field.values.tolist(), flush=True)
(root / "provenance.json").write_text(
    json.dumps(
        {
            "generator": "xregrid.Regridder(source, target, method=...)",
            "versions": {k: md.version(k) for k in ("xregrid", "esmpy", "xarray", "numpy")},
            "source_url": args.source_url,
            "source_commit": args.source_commit,
        },
        indent=2,
    )
    + "\n"
)
