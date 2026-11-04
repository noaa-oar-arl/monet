# NCEP Grid and Region Data

## Named grids

MONET delegates named NCEP GRIB1 Table B mesh construction to AXIS's named-grid registry. No duplicate coordinate catalog is stored here. AXIS must provide the requested mesh; coordinate-bearing decoded xarray data continues to use its own coordinate metadata.

Source: https://www.nco.ncep.noaa.gov/pmb/docs/on388/tableb.html

## NCEP verification regions

Files under `regions/` are the NCEP `.poly` source files from MET's `develop/data/poly` directory, copied from the local source tree provided for this feature. The bundled set follows that directory's `Makefile.am`; MET's current names include `GAC` (Gulf of America Coast) and omit the older `GMC`, `NPO`, `SAK`, and `SPO` files. Keep source filenames and upstream license/copyright notices intact. The upstream source directory is:

https://github.com/dtcenter/MET/tree/develop/data/poly

The MET Appendix B guide documents the NCEP region names and the EAST/WEST/CONUS component sets:

https://metplus.readthedocs.io/projects/met/en/main_v11.0/Users_Guide/appendixB.html#polylines-for-ncep-regions

The upstream MET project is distributed under Apache-2.0. The upstream license and data copyright notice are included beside the files. MONET reads the `.poly` files directly; no raster mask cache or `.npz` region artifact is required.

Exclusive base assignment uses this stable priority order: APL, ATC, CAM, CAR,
ECA, GAC, GLF, GRB, HWI, LMV, MDW, MEX, NAK, NAO, NEC, NMT, NPL, NSA, NWC,
PRI, SAO, SEC, SMT, SPL, SWC, SWD, WCA. If polygons overlap, only the first
matching acronym is assigned. Polygon boundaries count as inside.

The separately requestable compounds are EAST (APL, GAC, LMV, MDW, NEC, SEC),
WEST (GRB, NMT, NPL, NWC, SMT, SPL, SWC, SWD), and CONUS (the union of EAST
and WEST).
