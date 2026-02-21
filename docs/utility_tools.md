# Utility Tools

MONET provides a variety of utility functions in `monet.util.tools` for common meteorological and air quality data processing tasks.

## Time Series Filters

### Kolmogorov-Zurbenko (KZ) Filter
The KZ filter is a low-pass filter created by iteratively applying a moving average. It is particularly useful for separating different time scales in atmospheric data (e.g., removing seasonal cycles or short-term noise).

```python
from monet.util.tools import kolmogorov_zurbenko_filter

# Apply KZ filter to a DataFrame
# window=29, iterations=3 is common for removing short-term fluctuations
filtered_df = kolmogorov_zurbenko_filter(df, col='O3', window=29, iterations=3)
```

## Meteorological Conversions

### Wind Components (U and V)
Convert wind speed and direction to zonal (U) and meridional (V) components.

```python
from monet.util.tools import wsdir2uv

u, v = wsdir2uv(wind_speed, wind_direction)
```

### Relative Humidity
Calculate relative humidity from temperature, pressure, and vapor pressure.

```python
from monet.util.tools import get_relhum

rh = get_relhum(temperature_k, pressure_hpa, vapor_pressure_hpa)
```

## Averaging and Rolling Maxima

MONET includes specialized averaging functions for air quality standards:

*   `calc_8hr_rolling_max(df, col, window)`: Calculates the 8-hour rolling maximum (e.g., for Ozone standards).
*   `calc_24hr_ave(df, col)`: Calculates daily (24-hour) averages.
*   `calc_3hr_ave(df, col)`: Calculates 3-hour averages.
*   `calc_annual_ave(df, col)`: Calculates annual averages.

All these functions expect a pandas DataFrame with `siteid` and `time_local` columns and return the merged results.

## Region Identification

### Giorgi and EPA Regions
You can easily add region identifiers (indices and acronyms) to your datasets or dataframes.

```python
from monet.util.tools import get_giorgi_region_df, get_epa_region_df

# Add Giorgi region info (e.g., CNA, ENA, etc.)
ds_with_regions = get_giorgi_region_df(ds)

# Add EPA region info (Regions 1-10)
df_with_epa = get_epa_region_df(df)
```

These functions are convention-aware and will automatically find latitude and longitude coordinates in your data.

## Meteorological Functions

The `monet.met_funcs` module contains a collection of routines for estimating various atmospheric and surface variables, particularly those related to Monin-Obukhov Similarity Theory.

### Basic Computations
*   `calc_pressure(z)`: Estimates barometric pressure (mb) at a given height `z` above sea level.
*   `calc_rho(p, ea, T_K)`: Calculates air density (kg m-3).
*   `calc_vapor_pressure(T_K)`: Calculates saturation water vapor pressure (mb).
*   `calc_mixing_ratio(ea, p)`: Calculates the water vapor mixing ratio.

### Solar Geometry
*   `calc_sun_angles(lat, lon, stdlon, doy, ftime)`: Calculates Sun Zenith and Azimuth angles.

### Surface Fluxes and Stability
*   `calc_L(...)`: Calculates the Monin-Obukhov stability length.
*   `calc_u_star(...)`: Calculates friction velocity.
*   `calc_Psi_H(zoL)` and `calc_Psi_M(zoL)`: Adiabatic correction factors for heat and momentum transport.

## Vertical Coordinate Utilities

The `monet.util.vertical` module provides tools for working with various vertical coordinate systems, particularly for models like FV3.

### FV3 Pressure and Height
*   `calc_fv3_pressure(ak, bk, ps)`: Calculates 3D pressure from hybrid coefficients.
*   `calc_fv3_height(temp, phalf, hsfc)`: Calculates geopotential height at layer interfaces using the hypsometric equation.

These functions support both NumPy and Dask-backed arrays and maintain data provenance in the `history` attribute.
