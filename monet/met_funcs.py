"""
This package contains the main routines for estimating variables related to the
Monin-Obukhov (MO) Similarity Theory, such as  MO length, adiabatic correctors
for heat and momentum transport. It requires the following package.

References
----------
.. [Brutsaert2005] Brutsaert, W. (2005). Hydrology: an introduction (Vol. 61, No. 8).
   Cambridge: Cambridge University Press.

.. [Norman2000] Norman, J. M., W. P. Kustas, J. H. Prueger, and G. R. Diak (2000),
   Surface flux estimation using radiometric temperature: A dual-temperature-difference
   method to minimize measurement errors, Water Resour. Res., 36(8), 2263-2274,
   https://doi.org/10.1029/2000WR900033.
"""

import datetime
from collections.abc import Callable
from typing import Any

import numpy as np
import xarray as xr
from numpy.typing import ArrayLike

# ==============================================================================
# List of constants used in Meteorological computations
# ==============================================================================
# Stephan Boltzmann constant (W m-2 K-4)
sb = 5.670373e-8
# heat capacity of dry air at constant pressure (J kg-1 K-1)
c_pd = 1003.5
# heat capacity of water vapour at constant pressure (J kg-1 K-1)
c_pv = 1865
# ratio of the molecular weight of water vapor to dry air
epsilon = 0.622
# Psicrometric Constant kPa K-1
psicr = 0.0658
# gas constant for dry air, J/(kg*degK)
R_d = 287.04
# acceleration of gravity (m s-2)
g = 9.8
# ==============================================================================
# List of constants used in MO similarity
# ==============================================================================
# von Karman's constant
k = 0.4
# acceleration of gravity (m s-2)
gravity = 9.8


def _apply_aero(func: Callable, *args: Any, name: str = "", **kwargs: Any) -> Any:
    """Helper to apply a function following Aero Protocol."""
    is_xr = any(isinstance(arg, (xr.DataArray, xr.Dataset)) for arg in args)

    if is_xr:
        # Broadcast xarray objects
        # result = xr.apply_ufunc(func, *args, kwargs=kwargs, dask="parallelized", output_dtypes=[float])
        # Note: sometimes we need to broadcast before apply_ufunc if they have different dims
        # but apply_ufunc with dask='parallelized' handles most cases if they share dims or are broadcastable.
        result = xr.apply_ufunc(
            func,
            *args,
            kwargs=kwargs,
            dask="parallelized",
            output_dtypes=[float],
        )

        # Update history
        if hasattr(result, "attrs"):
            curr_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            msg = f"{curr_time} > Computed {name} via monet.met_funcs"
            history = result.attrs.get("history", "")
            result.attrs["history"] = (history + f"\n{msg}").strip()

        return result

    return func(*args, **kwargs)


def calc_c_p(p: ArrayLike, ea: ArrayLike) -> Any:
    """Calculates the heat capacity of air at constant pressure.

    Parameters
    ----------
    p : float or xarray.DataArray
        total air pressure (dry air + water vapour) (mb).
    ea : float or xarray.DataArray
        water vapor pressure at reference height above canopy (mb).

    Returns
    -------
    c_p : float or xarray.DataArray
        heat capacity of (moist) air at constant pressure (J kg-1 K-1).

    References
    ----------
    based on equation (6.1) from Maarten Ambaum (2010):
    Thermal Physics of the Atmosphere (pp 109)."""

    def _logic(p, ea):
        # first calculate specific humidity, rearanged eq (5.22) from Maarten
        # Ambaum (2010), (pp 100)
        q = epsilon * ea / (p + (epsilon - 1.0) * ea)
        # then the heat capacity of (moist) air
        return (1.0 - q) * c_pd + q * c_pv

    return _apply_aero(_logic, p, ea, name="heat capacity (c_p)")


def calc_lambda(T_A_K: ArrayLike) -> Any:
    """Calculates the latent heat of vaporization.

    Parameters
    ----------
    T_A_K : float or xarray.DataArray
        Air temperature (Kelvin).

    Returns
    -------
    Lambda : float or xarray.DataArray
        Latent heat of vaporisation (J kg-1).

    References
    ----------
    based on Eq. 3-1 Allen FAO98"""

    def _logic(T_A_K):
        return 1e6 * (2.501 - (2.361e-3 * (T_A_K - 273.15)))

    return _apply_aero(_logic, T_A_K, name="latent heat of vaporization")


def calc_pressure(z: ArrayLike) -> Any:
    """Calculates the barometric pressure above sea level.

    Parameters
    ----------
    z: float or xarray.DataArray
        height above sea level (m).

    Returns
    -------
    p: float or xarray.DataArray
        air pressure (mb)."""

    def _logic(z):
        return 1013.25 * (1.0 - 2.225577e-5 * z) ** 5.25588

    return _apply_aero(_logic, z, name="barometric pressure")


def calc_psicr(c_p: ArrayLike, p: ArrayLike, Lambda: ArrayLike) -> Any:
    """Calculates the psicrometric constant.

    Parameters
    ----------
    c_p : float or xarray.DataArray
        heat capacity of (moist) air at constant pressure (J kg-1 K-1).
    p : float or xarray.DataArray
        atmopheric pressure (mb).
    Lambda : float or xarray.DataArray
        latent heat of vaporzation (J kg-1).

    Returns
    -------
    psicr : float or xarray.DataArray
        Psicrometric constant (mb C-1)."""

    def _logic(c_p, p, Lambda):
        return c_p * p / (epsilon * Lambda)

    return _apply_aero(_logic, c_p, p, Lambda, name="psicrometric constant")


def calc_rho(p: ArrayLike, ea: ArrayLike, T_A_K: ArrayLike) -> Any:
    """Calculates the density of air.

    Parameters
    ----------
    p : float or xarray.DataArray
        total air pressure (dry air + water vapour) (mb).
    ea : float or xarray.DataArray
        water vapor pressure at reference height above canopy (mb).
    T_A_K : float or xarray.DataArray
        air temperature at reference height (Kelvin).

    Returns
    -------
    rho : float or xarray.DataArray
        density of air (kg m-3).

    References
    ----------
    based on equation (2.6) from Brutsaert (2005): Hydrology - An Introduction (pp 25)."""

    def _logic(p, ea, T_A_K):
        # p is multiplied by 100 to convert from mb to Pascals
        return ((p * 100.0) / (R_d * T_A_K)) * (1.0 - (1.0 - epsilon) * ea / p)

    return _apply_aero(_logic, p, ea, T_A_K, name="air density")


def calc_stephan_boltzmann(T_K: ArrayLike) -> Any:
    """Calculates the total energy radiated by a blackbody.

    Parameters
    ----------
    T_K : float or xarray.DataArray
        body temperature (Kelvin)

    Returns
    -------
    M : float or xarray.DataArray
        Emitted radiance (W m-2)"""

    def _logic(T_K):
        return sb * T_K**4

    return _apply_aero(_logic, T_K, name="emitted radiance")


def calc_theta_s(
    xlat: ArrayLike,
    xlong: ArrayLike,
    stdlng: ArrayLike,
    doy: ArrayLike,
    year: ArrayLike,
    ftime: ArrayLike,
) -> Any:
    """Calculates the Sun Zenith Angle (SZA).

    Parameters
    ----------
    xlat : float or xarray.DataArray
        latitude of the site (degrees).
    xlong : float or xarray.DataArray
        longitude of the site (degrees).
    stdlng : float or xarray.DataArray
        central longitude of the time zone of the site (degrees).
    doy : float or xarray.DataArray
        day of year of measurement (1-366).
    year : float or xarray.DataArray
        year of measurement .
    ftime : float or xarray.DataArray
        time of measurement (decimal hours).

    Returns
    -------
    theta_s : float or xarray.DataArray
        Sun Zenith Angle (degrees).

    References
    ----------
    Adopted from Martha Anderson's fortran code for ALEXI which in turn was based on Cupid.
    """

    def _logic(xlat, xlong, stdlng, doy, year, ftime):
        pid180 = np.pi / 180
        pid2 = np.pi / 2.0

        # Latitude computations
        xlat_rad = np.radians(xlat)
        sinlat = np.sin(xlat_rad)
        coslat = np.cos(xlat_rad)

        # Declination computations
        kday = (year - 1977.0) * 365.0 + doy + 28123.0
        xm = np.radians(-1.0 + 0.9856 * kday)
        delnu = 2.0 * 0.01674 * np.sin(xm) + 1.25 * 0.01674 * 0.01674 * np.sin(2.0 * xm)
        slong = np.radians(-79.8280 + 0.9856479 * kday) + delnu
        decmax = np.sin(np.radians(23.44))
        decl = np.arcsin(decmax * np.sin(slong))
        sindec = np.sin(decl)
        cosdec = np.cos(decl)
        eqtm = 9.4564 * np.sin(2.0 * slong) / cosdec - 4.0 * delnu / pid180
        eqtm = eqtm / 60.0

        # Get sun zenith angle
        timsun = ftime  # MODIS time is already solar time
        hrang = (timsun - 12.0) * pid2 / 6.0
        theta_s = np.arccos(sinlat * sindec + coslat * cosdec * np.cos(hrang))

        # if the sun is below the horizon just set it slightly above horizon
        theta_s = np.minimum(theta_s, pid2 - 0.0000001)
        return np.degrees(theta_s)

    return _apply_aero(_logic, xlat, xlong, stdlng, doy, year, ftime, name="sun zenith angle")


def calc_sun_angles(lat: ArrayLike, lon: ArrayLike, stdlon: ArrayLike, doy: ArrayLike, ftime: ArrayLike) -> Any:
    """Calculates the Sun Zenith and Azimuth Angles (SZA & SAA).

    Parameters
    ----------
    lat : float or xarray.DataArray
        latitude of the site (degrees).
    lon : float or xarray.DataArray
        longitude of the site (degrees).
    stdlon : float or xarray.DataArray
        central longitude of the time zone of the site (degrees).
    doy : float or xarray.DataArray
        day of year of measurement (1-366).
    ftime : float or xarray.DataArray
        time of measurement (decimal hours).

    Returns
    -------
    sza, saa : float or xarray.DataArray
        Sun Zenith Angle (degrees) and Sun Azimuth Angle (degrees).
    """

    def _logic(lat, lon, stdlon, doy, ftime):
        # Calculate declination
        declination = 0.409 * np.sin((2.0 * np.pi * doy / 365.0) - 1.39)
        EOT = (
            0.258 * np.cos(declination)
            - 7.416 * np.sin(declination)
            - 3.648 * np.cos(2.0 * declination)
            - 9.228 * np.sin(2.0 * declination)
        )
        LC = (stdlon - lon) / 15.0
        time_corr = (-EOT / 60.0) + LC
        solar_time = ftime - time_corr

        # Get the hour angle
        w = (solar_time - 12.0) * 15.0

        # Get solar elevation angle
        sin_thetha = np.cos(np.radians(w)) * np.cos(declination) * np.cos(np.radians(lat)) + np.sin(declination) * np.sin(
            np.radians(lat)
        )
        sun_elev = np.arcsin(sin_thetha)

        # Get solar zenith angle
        sza = np.pi / 2.0 - sun_elev
        sza_deg = np.degrees(sza)

        # Get solar azimuth angle
        cos_phi = (
            np.sin(declination) * np.cos(np.radians(lat)) - np.cos(np.radians(w)) * np.cos(declination) * np.sin(np.radians(lat))
        ) / np.cos(sun_elev)

        saa_deg = np.zeros(sza_deg.shape)
        saa_deg[w <= 0.0] = np.degrees(np.arccos(cos_phi[w <= 0.0]))
        saa_deg[w > 0.0] = 360.0 - np.degrees(np.arccos(cos_phi[w > 0.0]))

        return sza_deg, saa_deg

    is_xr = any(isinstance(arg, (xr.DataArray, xr.Dataset)) for arg in (lat, lon, stdlon, doy, ftime))

    if is_xr:
        # For multiple outputs from apply_ufunc
        result = xr.apply_ufunc(
            _logic,
            lat,
            lon,
            stdlon,
            doy,
            ftime,
            dask="parallelized",
            output_dtypes=[float, float],
            input_core_dims=[[]] * 5,
            output_core_dims=[[], []],
        )

        # Update history
        for res in result:
            if hasattr(res, "attrs"):
                curr_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                msg = "Computed sun angles via monet.met_funcs"
                history = res.attrs.get("history", "")
                res.attrs["history"] = (history + f"\n{curr_time} > {msg}").strip()

        return result

    return _logic(lat, lon, stdlon, doy, ftime)


def calc_vapor_pressure(T_K: ArrayLike) -> Any:
    """Calculate the saturation water vapour pressure.

    Parameters
    ----------
    T_K : float or xarray.DataArray
        temperature (K).

    Returns
    -------
    ea : float or xarray.DataArray
        saturation water vapour pressure (mb).
    """

    def _logic(T_K):
        T_C = T_K - 273.15
        return 6.112 * np.exp((17.67 * T_C) / (T_C + 243.5))

    return _apply_aero(_logic, T_K, name="saturation vapor pressure")


def calc_delta_vapor_pressure(T_K: ArrayLike) -> Any:
    """Calculate the slope of saturation water vapour pressure.

    Parameters
    ----------
    T_K : float or xarray.DataArray
        temperature (K).

    Returns
    -------
    s : float or xarray.DataArray
        slope of the saturation water vapour pressure (kPa K-1)
    """

    def _logic(T_K):
        T_C = T_K - 273.15
        return 4098.0 * (0.6108 * np.exp(17.27 * T_C / (T_C + 237.3))) / ((T_C + 237.3) ** 2)

    return _apply_aero(_logic, T_K, name="slope of saturation vapor pressure")


def calc_mixing_ratio(ea: ArrayLike, p: ArrayLike) -> Any:
    """Calculate ratio of mass of water vapour to the mass of dry air (-)

    Parameters
    ----------
    ea : float or xarray.DataArray
        water vapor pressure at reference height (mb).
    p : float or xarray.DataArray
        total air pressure (dry air + water vapour) at reference height (mb).

    Returns
    -------
    r : float or xarray.DataArray
        mixing ratio (-)

    References
    ----------
    https://glossary.ametsoc.org/wiki/Mixing_ratio
    """

    def _logic(ea, p):
        return epsilon * ea / (p - ea)

    return _apply_aero(_logic, ea, p, name="mixing ratio")


def calc_lapse_rate_moist(T_A_K: ArrayLike, ea: ArrayLike, p: ArrayLike) -> Any:
    """Calculate moist-adiabatic lapse rate (K/m)

    Parameters
    ----------
    T_A_K : float or xarray.DataArray
        air temperature at reference height (K).
    ea : float or xarray.DataArray
        water vapor pressure at reference height (mb).
    p : float or xarray.DataArray
        total air pressure (dry air + water vapour) at reference height (mb).

    Returns
    -------
    Gamma_w : float or xarray.DataArray
        moist-adiabatic lapse rate (K/m)

    References
    ----------
    https://glossary.ametsoc.org/wiki/Adiabatic_lapse_rate
    """

    def _logic(T_A_K, ea, p):
        r = epsilon * ea / (p - ea)
        # Heat capacity of air
        q = epsilon * ea / (p + (epsilon - 1.0) * ea)
        c_p = (1.0 - q) * c_pd + q * c_pv
        # Latent heat
        lambda_v = 1e6 * (2.501 - (2.361e-3 * (T_A_K - 273.15)))
        return g * (R_d * T_A_K**2 + lambda_v * r * T_A_K) / (c_p * R_d * T_A_K**2 + lambda_v**2 * r * epsilon)

    return _apply_aero(_logic, T_A_K, ea, p, name="moist-adiabatic lapse rate")


def flux_2_evaporation(flux: ArrayLike, T_K: ArrayLike = 20 + 273.15, time_domain: float = 1) -> Any:
    """Converts heat flux units (W m-2) to evaporation rates (mm time-1) to a given temporal window

    Parameters
    ----------
    flux : float or xarray.DataArray
        heat flux value to be converted,
        usually refers to latent heat flux LE to be converted to ET
    T_K : float or xarray.DataArray
        environmental temperature in Kelvin. Default=20 Celsius
    time_domain : float
        Temporal window in hours. Default 1 hour (mm h-1)

    Returns
    -------
    ET : float or xarray.DataArray
        evaporation rate at the time_domain. Default mm h-1
    """

    def _logic(flux, T_K, time_domain):
        # Calculate latent heat of vaporization
        lambda_ = 1e6 * (2.501 - (2.361e-3 * (T_K - 273.15)))  # J kg-1
        ET = flux / lambda_  # kg s-1

        # Convert instantaneous rate to the time_domain rate
        return ET * time_domain * 3600.0

    return _apply_aero(_logic, flux, T_K, time_domain, name="evaporation rate")


def calc_L(
    ustar: ArrayLike,
    T_A_K: ArrayLike,
    rho: ArrayLike,
    c_p: ArrayLike,
    H: ArrayLike,
    LE: ArrayLike,
) -> Any:
    """Calculates the Monin-Obukhov length.

    Parameters
    ----------
    ustar : float or xarray.DataArray
        friction velocity (m s-1).
    T_A_K : float or xarray.DataArray
        air temperature (Kelvin).
    rho : float or xarray.DataArray
        air density (kg m-3).
    c_p : float or xarray.DataArray
        Heat capacity of air at constant pressure (J kg-1 K-1).
    H : float or xarray.DataArray
        sensible heat flux (W m-2).
    LE : float or xarray.DataArray
        latent heat flux (W m-2).

    Returns
    -------
    L : float or xarray.DataArray
        Obukhov stability length (m).

    References
    ----------
    [Brutsaert2005]_
    """

    def _logic(ustar, T_A_K, rho, c_p, H, LE):
        # first convert latent heat into rate of surface evaporation (kg m-2 s-1)
        Lambda = 1e6 * (2.501 - (2.361e-3 * (T_A_K - 273.15)))  # in J kg-1
        E = LE / Lambda
        # Virtual sensible heat flux
        Hv = H + (0.61 * T_A_K * c_p * E)

        L = np.full(ustar.shape, np.inf)
        i = Hv != 0
        L_const = k * gravity / T_A_K
        L[i] = -(ustar[i] ** 3) / (L_const[i] * (Hv[i] / (rho[i] * c_p[i])))
        return L

    return _apply_aero(_logic, ustar, T_A_K, rho, c_p, H, LE, name="Obukhov stability length")


def calc_Psi_H(zoL: ArrayLike) -> Any:
    """Calculates the adiabatic correction factor for heat transport.

    Parameters
    ----------
    zoL : float or xarray.DataArray
        stability coefficient (unitless).

    Returns
    -------
    Psi_H : float or xarray.DataArray
        adiabatic corrector factor for heat transport (unitless).

    References
    ----------
    [Brutsaert2005]_
    """

    def _logic(zoL):
        Psi_H = np.zeros(zoL.shape)

        # for stable and neutral (zoL = 0 -> Psi_H = 0) conditions
        i = zoL >= 0.0
        a = 6.1
        b = 2.5
        Psi_H[i] = -a * np.log(zoL[i] + (1.0 + zoL[i] ** b) ** (1.0 / b))

        # for unstable conditions
        i = zoL < 0.0
        y = -zoL[i]
        c = 0.33
        d = 0.057
        n = 0.78
        Psi_H[i] = ((1.0 - d) / n) * np.log((c + y**n) / c)
        return Psi_H

    return _apply_aero(_logic, zoL, name="adiabatic correction factor (heat)")


def calc_Psi_M(zoL: ArrayLike) -> Any:
    """Adiabatic correction factor for momentum transport.

    Parameters
    ----------
    zoL : float or xarray.DataArray
        stability coefficient (unitless).

    Returns
    -------
    Psi_M : float or xarray.DataArray
        adiabatic corrector factor for momentum transport (unitless).

    References
    ----------
    [Brutsaert2005]_
    """

    def _logic(zoL):
        Psi_M = np.zeros(zoL.shape)
        # for stable and neutral (zoL = 0 -> Psi_M = 0) conditions
        i = zoL >= 0.0
        a = 6.1
        b = 2.5
        Psi_M[i] = -a * np.log(zoL[i] + (1.0 + zoL[i] ** b) ** (1.0 / b))
        # for unstable conditions
        i = zoL < 0
        y = -zoL[i]
        a = 0.33
        b = 0.41
        x = (y / a) ** 0.333333
        Psi_0 = -np.log(a) + 3**0.5 * b * a**0.333333 * np.pi / 6.0
        y_min = np.minimum(y, b**-3)
        Psi_M[i] = (
            np.log(a + y_min)
            - 3.0 * b * y_min**0.333333
            + (b * a**0.333333) / 2.0 * np.log((1.0 + x) ** 2 / (1.0 - x + x**2))
            + 3.0**0.5 * b * a**0.333333 * np.arctan((2.0 * x - 1.0) / 3**0.5)
            + Psi_0
        )
        return Psi_M

    return _apply_aero(_logic, zoL, name="adiabatic correction factor (momentum)")


def calc_richardson(
    u: ArrayLike,
    z_u: ArrayLike,
    d_0: ArrayLike,
    T_R0: ArrayLike,
    T_R1: ArrayLike,
    T_A0: ArrayLike,
    T_A1: ArrayLike,
) -> Any:
    """Richardson number.

    Estimates the Bulk Richardson number for turbulence using
    time difference temperatures.

    Parameters
    ----------
    u : float or xarray.DataArray
        Wind speed (m s-1).
    z_u : float or xarray.DataArray
        Wind speed measurement height (m).
    d_0 : float or xarray.DataArray
        Zero-plane displacement height (m).
    T_R0 : float or xarray.DataArray
        radiometric surface temperature at time 0 (K).
    T_R1 : float or xarray.DataArray
        radiometric surface temperature at time 1 (K).
    T_A0 : float or xarray.DataArray
        air temperature at time 0 (K).
    T_A1 : float or xarray.DataArray
        air temperature at time 1 (K).

    Returns
    -------
    Ri : float or xarray.DataArray
        Richardson number.

    References
    ----------
    [Norman2000]_
    """

    def _logic(u, z_u, d_0, T_R0, T_R1, T_A0, T_A1):
        # See eq (2) from Louis 1979
        return -(gravity * (z_u - d_0) / T_A1) * (((T_R1 - T_R0) - (T_A1 - T_A0)) / u**2)  # equation (12) [Norman2000]

    return _apply_aero(_logic, u, z_u, d_0, T_R0, T_R1, T_A0, T_A1, name="Richardson number")


def calc_u_star(u: ArrayLike, z_u: ArrayLike, L: ArrayLike, d_0: ArrayLike, z_0M: ArrayLike) -> Any:
    """Friction velocity.

    Parameters
    ----------
    u : float or xarray.DataArray
        wind speed above the surface (m s-1).
    z_u : float or xarray.DataArray
        wind speed measurement height (m).
    L : float or xarray.DataArray
        Monin Obukhov stability length (m).
    d_0 : float or xarray.DataArray
        zero-plane displacement height (m).
    z_0M : float or xarray.DataArray
        aerodynamic roughness length for momentum transport (m).

    Returns
    -------
    u_star : float or xarray.DataArray
        friction velocity (m s-1).

    References
    ----------
    [Brutsaert2005]_
    """

    def _logic(u, z_u, L, d_0, z_0M):
        # calculate correction factors in other conditions
        L_adj = np.where(L == 0.0, 1e-36, L)

        # Re-implementing calc_Psi_M logic here to keep it within _logic for apply_ufunc if needed,
        # but since calc_Psi_M now handles numpy arrays via _apply_aero, we can just call it.
        # Wait, inside _logic (called by apply_ufunc), inputs are numpy arrays.
        # So we can call the original logic of calc_Psi_M.
        def _psi_m_logic(zoL):
            Psi_M = np.zeros(zoL.shape)
            i = zoL >= 0.0
            a, b = 6.1, 2.5
            Psi_M[i] = -a * np.log(zoL[i] + (1.0 + zoL[i] ** b) ** (1.0 / b))
            i = zoL < 0
            y = -zoL[i]
            a, b = 0.33, 0.41
            x = (y / a) ** 0.333333
            Psi_0 = -np.log(a) + 3**0.5 * b * a**0.333333 * np.pi / 6.0
            y_min = np.minimum(y, b**-3)
            Psi_M[i] = (
                np.log(a + y_min)
                - 3.0 * b * y_min**0.333333
                + (b * a**0.333333) / 2.0 * np.log((1.0 + x) ** 2 / (1.0 - x + x**2))
                + 3.0**0.5 * b * a**0.333333 * np.arctan((2.0 * x - 1.0) / 3**0.5)
                + Psi_0
            )
            return Psi_M

        Psi_M = _psi_m_logic((z_u - d_0) / L_adj)
        Psi_M0 = _psi_m_logic(z_0M / L_adj)
        return u * k / (np.log((z_u - d_0) / z_0M) - Psi_M + Psi_M0)

    return _apply_aero(_logic, u, z_u, L, d_0, z_0M, name="friction velocity (u*)")
