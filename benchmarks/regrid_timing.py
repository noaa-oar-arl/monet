"""Compare end-to-end conservative remapping in separate AXIS and legacy envs.

Run --engine legacy --output old.json in the old environment, then
--engine axis --baseline old.json --output axis.json in the MONET environment.
No old engine is imported for the AXIS run. Results include build/apply/compute;
imports and input generation are outside the timer. Use identical thread counts.
"""

import argparse
import importlib.metadata
import json
import platform
import resource
import statistics
import sys
import time
from pathlib import Path

import numpy as np
import xarray as xr


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", choices=["axis", "legacy"], default="axis")
    parser.add_argument("--method", choices=["conservative"], default="conservative")
    parser.add_argument("--resolution", type=float, default=2.0, help="Source spacing in degrees; target spacing is twice this")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--lazy", action="store_true")
    parser.add_argument(
        "--monet-path",
        type=Path,
        help="Checkout to import for an end-to-end MONET measurement (use the old checkout for --engine legacy)",
    )
    args = parser.parse_args()
    if args.resolution <= 0 or args.resolution > 30 or args.repeats < 1:
        parser.error("resolution must be in (0,30] and repeats positive")
    step = args.resolution
    lat = np.arange(-90 + step / 2, 90, step)
    lon = np.arange(-180 + step / 2, 180, step)
    source = xr.Dataset(
        {"field": (("lat", "lon"), 2.0 + np.cos(np.deg2rad(lat))[:, None] * np.cos(np.deg2rad(lon))[None, :])},
        coords={"lat": lat, "lon": lon},
    )
    target = xr.Dataset(coords={"lat": np.arange(-90 + step, 90, 2 * step), "lon": np.arange(-180 + step, 180, 2 * step)})
    for grid in (source, target):
        grid.lat.attrs.update(standard_name="latitude", units="degrees_north")
        grid.lon.attrs.update(standard_name="longitude", units="degrees_east")
    if args.lazy:
        source = source.chunk({"lat": 30, "lon": 60})
    if args.monet_path:
        sys.path.insert(0, str(args.monet_path.resolve()))
        from monet.util.resample import resample

        def run():
            return resample(source, target, method=args.method)

        dependency = "xregrid" if args.engine == "legacy" else "axis-regrid"
    elif args.engine == "legacy":
        from xregrid import Regridder

        def run():
            return Regridder(source, target, method=args.method)(source)

        dependency = "xregrid"
    else:
        from axis import Regridder

        def run():
            return Regridder(source, target, method=args.method)(source)

        dependency = "axis-regrid"
    samples = []
    for _ in range(args.repeats):
        start = time.perf_counter()
        out = run()
        if args.lazy and not hasattr(out.field.data, "dask"):
            raise AssertionError("Output was computed eagerly")
        out.compute()
        samples.append(time.perf_counter() - start)
    result = {
        "engine": args.engine,
        "method": args.method,
        "resolution": step,
        "lazy": args.lazy,
        "seconds": samples,
        "median_seconds": statistics.median(samples),
        "peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * (1 if sys.platform == "darwin" else 1024),
        "platform": platform.platform(),
        "python": platform.python_version(),
        "dependency_version": importlib.metadata.version(dependency),
    }
    result["monet_path"] = str(args.monet_path.resolve()) if args.monet_path else None
    if args.baseline:
        baseline = json.loads(args.baseline.read_text())
        for key in ("resolution", "method", "lazy"):
            if baseline[key] != result[key]:
                raise ValueError(f"Baseline differs in {key}")
        result["speedup"] = baseline["median_seconds"] / result["median_seconds"]
        result["speedup_target_met"] = result["speedup"] >= 3
        result["peak_memory_target_met"] = result["peak_rss_bytes"] <= baseline["peak_rss_bytes"]
    rendered = json.dumps(result, indent=2)
    print(rendered)
    if args.output:
        args.output.write_text(rendered + "\n")


if __name__ == "__main__":
    main()
