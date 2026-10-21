# MONET — Areas of Improvement

This report identifies opportunities to improve algorithmic efficiency, runtime performance, and user experience across the codebase. Items are grouped by category and ordered roughly by impact.

---

## 1. Algorithm & Correctness

### 1.1 Vertical coordinate ordering assumption
**File:** [monet/util/vertical.py](monet/util/vertical.py)
`calc_fv3_height` silently assumes a top-to-bottom vertical ordering. If the input is bottom-up the hydrostatic height calculation will be physically wrong with no warning. Add an assertion or automatic detection of ordering before `np.cumsum` is called.

### 1.2 `np.cumsum` breaks Dask chunking
**File:** [monet/util/vertical.py](monet/util/vertical.py)
`_hydrostatic_logic` calls `np.cumsum` along the vertical axis, which forces a full materialisation of any Dask chunk that spans the vertical dimension. Replacing it with `dask.array.cumsum` (or wrapping it in `xr.apply_ufunc` with `dask="parallelized"`) would preserve the lazy graph.

### 1.3 Unit validation missing in meteorological functions
**File:** [monet/met_funcs.py](monet/met_funcs.py)
`calc_rho` and similar functions document pressure in mb but accept any numeric input without checking. A user who passes Pascals gets a silently wrong result. Consider lightweight range-checks or integration with `pint`/`cf_xarray` units.

### 1.4 Coordinate name duplication between `BaseAccessor` and `conventions`
**Files:** [monet/accessors/base.py](monet/accessors/base.py), [monet/util/conventions.py](monet/util/conventions.py)
Logic for canonicalising `latitude/longitude → lat/lon` is duplicated. A single authoritative helper would reduce the risk of the two diverging.

---

## 2. Performance

### 2.1 Repeated coordinate detection on every property access
**File:** [monet/accessors/base.py](monet/accessors/base.py)
`BaseAccessor.lat` and `BaseAccessor.lon` re-run the coordinate lookup on every attribute access. Cache the result on first successful detection (e.g., in a `__post_init__`-style method or via `functools.cached_property`) to avoid redundant work in loops over time steps.

### 2.2 `_thin_data` may trigger full Dask compute
**File:** [monet/plots/plots.py](monet/plots/plots.py)
If the input to `_thin_data` is a large lazy Dask array, any slice or fancy-index that requires shape knowledge may materialise the whole array. Ensure thinning is done on already-computed 2-D slices, or explicitly call `.isel()` on spatial dimensions only.

### 2.3 `facet_time_map` creates one GeoAxes per time step
**File:** [monet/plots/cartopy_utils.py](monet/plots/cartopy_utils.py)
Creating a `Cartopy` `GeoAxes` is expensive (~50–200 ms each). For long time series, the cumulative overhead dominates rendering time. Pre-create all axes and re-use the projection, or provide a `max_panels` guard that errors early.

### 2.4 Inner `_logic` functions redefined on every call
**File:** [monet/met_funcs.py](monet/met_funcs.py)
Several public functions define a nested `_logic` closure that is reconstructed on every invocation. Hoisting these to module-level functions (or converting them to `functools.partial` wrappers) removes unnecessary closure overhead and makes them independently testable.

### 2.5 `to_ascii2nc_df` mutates the caller's DataFrame
**File:** [monet/accessors/pandas_accessor.py](monet/accessors/pandas_accessor.py)
Temporary columns are added directly to the original DataFrame, creating an $O(N)$ side-effect. Work on a copy or use a local dictionary and construct only the output at the end.

---

## 3. User Experience & API

### 3.1 `nearest_ij` not implemented
**File:** [monet/accessors/dataarray_accessor.py](monet/accessors/dataarray_accessor.py)
`nearest_ij` raises `NotImplementedError` for the `xregrid` backend. This is one of the most common operations users need (point-in-grid lookups). Implementing it — even with a fallback using `xr.Dataset.sel(method="nearest")` — would unblock many workflows.

### 3.2 `resample` import error surfaced only at runtime
**File:** [monet/util/resample.py](monet/util/resample.py)
`xregrid` is not a hard dependency, so its `ImportError` appears only when `resample` is first called. Add a clear, actionable message at that point (e.g., _"Install xregrid: `pip install xregrid`"_) and document the optional dependency in `pyproject.toml`'s `[project.optional-dependencies]`.

### 3.3 `draw_map` defaults to `10m` resolution with no adaptive fallback
**File:** [monet/plots/mapgen.py](monet/plots/mapgen.py)
The `10m` Natural Earth dataset is accurate but slow for global or continental-scale views. Consider automatically selecting `50m` or `110m` when the map extent exceeds a threshold, with a parameter to override.

### 3.4 `_validate` error message is not actionable
**File:** [monet/accessors/pandas_accessor.py](monet/accessors/pandas_accessor.py)
`AttributeError` is raised when expected columns are missing, but the message doesn't list which column names were searched for. Improving it to something like _"Could not find a latitude column; tried: lat, latitude, Latitude, y"_ would help users debug column-name mismatches quickly.

### 3.5 `from monet_stats import *` pollutes namespace
**File:** [monet/util/stats.py](monet/util/stats.py)
Star-imports make it impossible to know which symbols are available without reading the external package source. Replace with explicit imports (or at minimum populate `__all__`) so IDEs and documentation tools can introspect the module correctly.

### 3.6 Inconsistent parameter naming in `met_funcs`
**File:** [monet/met_funcs.py](monet/met_funcs.py)
Temperature parameters alternate between `T_A_K`, `T_K`, and `temp` across functions. Standardising on a single convention (e.g., always `T_K` for Kelvin) reduces cognitive overhead.

### 3.7 Large argument surface on `plot_quick_imshow`
**File:** [monet/plots/cartopy_utils.py](monet/plots/cartopy_utils.py)
The function accepts many keyword arguments making it hard to discover. Group related options into small dataclass/TypedDict keyword groups (e.g., `map_opts`, `colorbar_opts`) or provide sensible opinionated wrappers for the most common use-cases.

---

## 4. Code Quality & Maintainability

### 4.1 `remap.py` is empty
**File:** [monet/util/remap.py](monet/util/remap.py)
The file exists but contains no code. Either implement the intended functionality or remove it to avoid confusing contributors.

### 4.2 `HAS_CF` / `HAS_REGIONS_DEPS` flags used inconsistently
**Files:** various `util/` modules
Some modules guard optional imports with a top-level flag; others catch `ImportError` inline. Standardise on the top-level flag pattern and check it before entering a function body so the error is raised immediately with a helpful install hint.

### 4.3 Backward-compatibility shims add maintenance burden
**File:** [monet/monet_accessor.py](monet/monet_accessor.py)
`monet_accessor.py` exists only to re-export from the new location. Schedule its removal with a deprecation timeline and `DeprecationWarning` that includes a migration note.

### 4.4 `get_non_spatial_dims` uses string heuristics
**File:** [monet/util/conventions.py](monet/util/conventions.py)
Detecting spatial vs non-spatial dimensions by matching name strings is fragile. Prefer `cf_xarray`'s axis/coordinate classification (`ds.cf["X"]`, `ds.cf["Y"]`) when available, falling back to the current heuristics only for non-CF-compliant data.

---

## 5. Testing Gaps

| Area | Gap |
|---|---|
| `vertical.py` ordering | No test for bottom-up input to `calc_fv3_height` |
| `met_funcs.py` unit safety | No test that verifies a wrong-unit input produces an error or warning |
| `pandas_accessor.py` mutation | No test verifying the original DataFrame is unchanged after `to_ascii2nc_df` |
| `nearest_ij` | No test for the unimplemented code path |
| `resample` optional dep | No test for the `ImportError` message quality |

---

## 6. Quick Wins (Low Effort / High Impact)

1. Cache `lat`/`lon` coordinate detection in `BaseAccessor` — ~5 lines, immediate speedup for any loop over an accessor.
2. Improve `_validate` error message — 1-line change, immediate UX improvement.
3. Add `pip install xregrid` hint to the `resample` `ImportError` — 1-line change.
4. Hoist `_logic` closures in `met_funcs.py` to module level — improves testability with no functional change.
5. Guard `remap.py` — either stub with `NotImplementedError` and a TODO, or delete.
