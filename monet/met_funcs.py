"""
DESCRIPTION
===========
This package contains the main routines for estimating variables related to the
 Monin-Obukhov (MO) Similarity Theory, such as  MO length, adiabatic correctors
for heat and momentum transport. It requires the following package.

"""

import numpy as np

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


def calc_c_p(p, ea):
    ''' Calculates the heat capacity of air at constant pressure.

    Parameters
    ----------
    p : float
        total air pressure (dry air + water vapour) (mb).
    ea : float
        water vapor pressure at reference height above canopy (mb).

    Returns
    -------
    c_p : heat capacity of (moist) air at constant pressure (J kg-1 K-1).

    References
    ----------
    based on equation (6.1) from Maarten Ambaum (2010):
    Thermal Physics of the Atmosphere (pp 109).'''

    # first calculate specific humidity, rearanged eq (5.22) from Maarten
    # Ambaum (2010), (pp 100)
    q = epsilon * ea / (p + (epsilon - 1.0) * ea)
    # then the heat capacity of (moist) air
    c_p = (1.0 - q) * c_pd + q * c_pv
    return np.asarray(c_p)


def calc_lambda(T_A_K):
    '''Calculates the latent heat of vaporization.

    Parameters
    ----------
    T_A_K : float
        Air temperature (Kelvin).

    Returns
    -------
    Lambda : float
        Latent heat of vaporisation (J kg-1).

    References
    ----------
    based on Eq. 3-1 Allen FAO98 '''

    Lambda = 1e6 * (2.501 - (2.361e-3 * (T_A_K - 273.15)))
    return np.asarray(Lambda)


def calc_pressure(z):
    ''' Calculates the barometric pressure above sea level.

    Parameters
    ----------
    z: float
        height above sea level (m).

    Returns
    -------
    p: float
        air pressure (mb).'''

    p = 1013.25 * (1.0 - 2.225577e-5 * z)**5.25588
    return np.asarray(p)


def calc_psicr(c_p, p, Lambda):
    ''' Calculates the psicrometric constant.

    Parameters
    ----------
    c_p : float
        heat capacity of (moist) air at constant pressure (J kg-1 K-1).
    p : float
        atmopheric pressure (mb).
    Lambda : float
        latent heat of vaporzation (J kg-1).

    Returns
    -------
    psicr : float
        Psicrometric constant (mb C-1).'''

    psicr = c_p * p / (epsilon * Lambda)
    return np.asarray(psicr)


def calc_rho(p, ea, T_A_K):
    '''Calculates the density of air.

    Parameters
    ----------
    p : float
        total air pressure (dry air + water vapour) (mb).
    ea : float
        water vapor pressure at reference height above canopy (mb).
    T_A_K : float
        air temperature at reference height (Kelvin).

    Returns
    -------
    rho : float
        density of air (kg m-3).

    References
    ----------
    based on equation (2.6) from Brutsaert (2005): Hydrology - An Introduction (pp 25).'''

    # p is multiplied by 100 to convert from mb to Pascals
    rho = ((p * 100.0) / (R_d * T_A_K)) * (1.0 - (1.0 - epsilon) * ea / p)
    return np.asarray(rho)


def calc_stephan_boltzmann(T_K):
    '''Calculates the total energy radiated by a blackbody.

    Parameters
    ----------
    T_K : float
        body temperature (Kelvin)

    Returns
    -------
    M : float
        Emitted radiance (W m-2)'''

    M = sb * T_K**4
    return np.asarray(M)


def calc_theta_s(xlat, xlong, stdlng, doy, year, ftime):
    """Calculates the Sun Zenith Angle (SZA).

    Parameters
    ----------
    xlat : float
        latitude of the site (degrees).
    xlong : float
        longitude of the site (degrees).
    stdlng : float
        central longitude of the time zone of the site (degrees).
    doy : float
        day of year of measurement (1-366).
    year : float
        year of measurement .
    ftime : float
        time of measurement (decimal hours).

    Returns
    -------
    theta_s : float
        Sun Zenith Angle (degrees).

    References
    ----------
    Adopted from Martha Anderson's fortran code for ALEXI which in turn was based on Cupid.
    """

    pid180 = np.pi / 180
    pid2 = np.pi / 2.0

    # Latitude computations
    xlat = np.radians(xlat)
    sinlat = np.sin(xlat)
    coslat = np.cos(xlat)

    # Declination computations
    kday = (year - 1977.0) * 365.0 + doy + 28123.0
    xm = np.radians(-1.0 + 0.9856 * kday)
    delnu = (2.0 * 0.01674 * np.sin(xm) +
             1.25 * 0.01674 * 0.01674 * np.sin(2.0 * xm))
    slong = np.radians((-79.8280 + 0.9856479 * kday)) + delnu
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
    theta_s = np.degrees(theta_s)

    return np.asarray(theta_s)


def calc_sun_angles(lat, lon, stdlon, doy, ftime):
    """Calculates the Sun Zenith and Azimuth Angles (SZA & SAA).

    Parameters
    ----------
    lat : float
        latitude of the site (degrees).
    long : float
        longitude of the site (degrees).
    stdlng : float
        central longitude of the time zone of the site (degrees).
    doy : float
        day of year of measurement (1-366).
    ftime : float
        time of measurement (decimal hours).

    Returns
    -------
    sza : float
        Sun Zenith Angle (degrees).
    saa : float
        Sun Azimuth Angle (degrees).
    """

    lat, lon, stdlon, doy, ftime = map(np.asarray,
                                       (lat, lon, stdlon, doy, ftime))

    # Calculate declination
    declination = 0.409 * np.sin((2.0 * np.pi * doy / 365.0) - 1.39)
    EOT = (0.258 * np.cos(declination) - 7.416 * np.sin(declination) -
           3.648 * np.cos(2.0 * declination) -
           9.228 * np.sin(2.0 * declination))
    LC = (stdlon - lon) / 15.
    time_corr = (-EOT / 60.) + LC
    solar_time = ftime - time_corr

    # Get the hour angle
    w = np.asarray((solar_time - 12.0) * 15.)

    # Get solar elevation angle
    sin_thetha = (
        np.cos(np.radians(w)) * np.cos(declination) * np.cos(np.radians(lat)) +
        np.sin(declination) * np.sin(np.radians(lat)))
    sun_elev = np.arcsin(sin_thetha)

    # Get solar zenith angle
    sza = np.pi / 2.0 - sun_elev
    sza = np.asarray(np.degrees(sza))

    # Get solar azimuth angle
    cos_phi = np.asarray(
        (np.sin(declination) * np.cos(np.radians(lat)) -
         np.cos(np.radians(w)) * np.cos(declination) * np.sin(np.radians(lat)))
        / np.cos(sun_elev))
    saa = np.zeros(sza.shape)
    saa[w <= 0.0] = np.degrees(np.arccos(cos_phi[w <= 0.0]))
    saa[w > 0.0] = 360. - np.degrees(np.arccos(cos_phi[w > 0.0]))

    return np.asarray(sza), np.asarray(saa)


def calc_vapor_pressure(T_K):
    """Calculate the saturation water vapour pressure.

    Parameters
    ----------
    T_K : float
        temperature (K).

    Returns
    -------
    ea : float
        saturation water vapour pressure (mb).
    """

    T_C = T_K - 273.15
    ea = 6.112 * np.exp((17.67 * T_C) / (T_C + 243.5))
    return np.asarray(ea)


def calc_delta_vapor_pressure(T_K):
    """Calculate the slope of saturation water vapour pressure.

    Parameters
    ----------
    T_K : float
        temperature (K).

    Returns
    -------
    s : float
        slope of the saturation water vapour pressure (kPa K-1)
    """

    T_C = T_K - 273.15
    s = 4098.0 * (0.6108 * np.exp(17.27 * T_C /
                                  (T_C + 237.3))) / ((T_C + 237.3)**2)
    return np.asarray(s)


def calc_mixing_ratio(ea, p):
    '''Calculate ratio of mass of water vapour to the mass of dry air (-)

    Parameters
    ----------
    ea : float or numpy array
        water vapor pressure at reference height (mb).
    p : float or numpy array
        total air pressure (dry air + water vapour) at reference height (mb).

    Returns
    -------
    r : float or numpy array
        mixing ratio (-)

    References
    ----------
    http://glossary.ametsoc.org/wiki/Mixing_ratio'''

    r = epsilon * ea / (p - ea)
    return r


def calc_lapse_rate_moist(T_A_K, ea, p):
    '''Calculate moist-adiabatic lapse rate (K/m)

    Parameters
    ----------
    T_A_K : float or numpy array
        air temperature at reference height (K).
    ea : float or numpy array
        water vapor pressure at reference height (mb).
    p : float or numpy array
        total air pressure (dry air + water vapour) at reference height (mb).

    Returns
    -------
    Gamma_w : float or numpy array
        moist-adiabatic lapse rate (K/m)

    References
    ----------
    http://glossary.ametsoc.org/wiki/Saturation-adiabatic_lapse_rate'''

    r = calc_mixing_ratio(ea, p)
    c_p = calc_c_p(p, ea)
    lambda_v = calc_lambda(T_A_K)
    Gamma_w = ((g * (R_d * T_A_K**2 + lambda_v * r * T_A_K) /
                (c_p * R_d * T_A_K**2 + lambda_v**2 * r * epsilon)))
    return Gamma_w


def flux_2_evaporation(flux, T_K=20 + 273.15, time_domain=1):
    '''Converts heat flux units (W m-2) to evaporation rates (mm time-1) to a given temporal window

    Parameters
    ----------
    flux : float or numpy array
        heat flux value to be converted,
        usually refers to latent heat flux LE to be converted to ET
    T_K : float or numpy array
        environmental temperature in Kelvin. Default=20 Celsius
    time_domain : float
        Temporal window in hours. Default 1 hour (mm h-1)

    Returns
    -------
    ET : float or numpy array
        evaporation rate at the time_domain. Default mm h-1
    '''
    # Calculate latent heat of vaporization
    lambda_ = calc_lambda(T_K)  # J kg-1
    ET = flux / lambda_  # kg s-1

    # Convert instantaneous rate to the time_domain rate
    ET = ET * time_domain * 3600.

    return ET


def calc_L(ustar, T_A_K, rho, c_p, H, LE):
    '''Calculates the Monin-Obukhov length.

    Parameters
    ----------
    ustar : float
        friction velocity (m s-1).
    T_A_K : float
        air temperature (Kelvin).
    rho : float
        air density (kg m-3).
    c_p : float
        Heat capacity of air at constant pressure (J kg-1 K-1).
    H : float
        sensible heat flux (W m-2).
    LE : float
        latent heat flux (W m-2).

    Returns
    -------
    L : float
        Obukhov stability length (m).

    References
    ----------
    .. [Brutsaert2005] Brutsaert, W. (2005). Hydrology: an introduction (Vol. 61, No. 8).
        Cambridge: Cambridge University Press.'''

    # Convert input scalars to numpy arrays
    ustar, T_A_K, rho, c_p, H, LE = map(np.asarray,
                                        (ustar, T_A_K, rho, c_p, H, LE))
    # first convert latent heat into rate of surface evaporation (kg m-2 s-1)
    Lambda = met.calc_lambda(T_A_K)  # in J kg-1
    E = LE / Lambda
    del LE, Lambda
    # Virtual sensible heat flux
    Hv = H + (0.61 * T_A_K * c_p * E)
    del H, E

    L = np.asarray(np.ones(ustar.shape) * float('inf'))
    i = Hv != 0
    L_const = np.asarray(k * gravity / T_A_K)
    L[i] = -ustar[i]**3 / (L_const[i] * (Hv[i] / (rho[i] * c_p[i])))
    return np.asarray(L)


def calc_Psi_H(zoL):
    ''' Calculates the adiabatic correction factor for heat transport.

    Parameters
    ----------
    zoL : float
        stability coefficient (unitless).

    Returns
    -------
    Psi_H : float
        adiabatic corrector factor fof heat transport (unitless).

    References
    ----------
    .. [Brutsaert2005] Brutsaert, W. (2005). Hydrology: an introduction (Vol. 61, No. 8).
        Cambridge: Cambridge University Press.
    '''

    # Convert input scalars to numpy array
    zoL = np.asarray(zoL)
    Psi_H = np.zeros(zoL.shape)

    # for stable and netural (zoL = 0 -> Psi_H = 0) conditions
    i = zoL >= 0.0
    a = 6.1
    b = 2.5
    Psi_H[i] = -a * np.log(zoL[i] + (1.0 + zoL[i]**b)**(1. / b))

    # for unstable conditions
    i = zoL < 0.0
    y = -zoL[i]
    del zoL
    c = 0.33
    d = 0.057
    n = 0.78
    Psi_H[i] = ((1.0 - d) / n) * np.log((c + y**n) / c)
    return np.asarray(Psi_H)


def calc_Psi_M(zoL):
    ''' Adiabatic correction factor for momentum transport.

    Parameters
    ----------
    zoL : float
        stability coefficient (unitless).

    Returns
    -------
    Psi_M : float
        adiabatic corrector factor fof momentum transport (unitless).

    References
    ----------
    .. [Brutsaert2005] Brutsaert, W. (2005). Hydrology: an introduction (Vol. 61, No. 8).
        Cambridge: Cambridge University Press.
    '''

    # Convert input scalars to numpy array
    zoL = np.asarray(zoL)

    Psi_M = np.zeros(zoL.shape)
    # for stable and netural (zoL = 0 -> Psi_M = 0) conditions
    i = zoL >= 0.0
    a = 6.1
    b = 2.5
    Psi_M[i] = -a * np.log(zoL[i] + (1.0 + zoL[i]**b)**(1.0 / b))
    # for unstable conditions
    i = zoL < 0
    y = -zoL[i]
    del zoL
    a = 0.33
    b = 0.41
    x = np.asarray((y / a)**0.333333)
    Psi_0 = -np.log(a) + 3**0.5 * b * a**0.333333 * np.pi / 6.0
    y = np.minimum(y, b**-3)
    Psi_M[i] = (np.log(a + y) - 3.0 * b * y**0.333333 +
                (b * a**0.333333) / 2.0 * np.log(
                    (1.0 + x)**2 / (1.0 - x + x**2)) +
                3.0**0.5 * b * a**0.333333 * np.arctan(
                    (2.0 * x - 1.0) / 3**0.5) + Psi_0)
    return np.asarray(Psi_M)


def calc_richardson(u, z_u, d_0, T_R0, T_R1, T_A0, T_A1):
    '''Richardson number.

    Estimates the Bulk Richardson number for turbulence using
    time difference temperatures.

    Parameters
    ----------
    u : float
        Wind speed (m s-1).
    z_u : float
        Wind speed measurement height (m).
    d_0 : float
        Zero-plane displacement height (m).
    T_R0 : float
        radiometric surface temperature at time 0 (K).
    T_R1 : float
        radiometric surface temperature at time 1 (K).
    T_A0 : float
        air temperature at time 0 (K).
    T_A1 : float
        air temperature at time 1 (K).

    Returns
    -------
    Ri : float
        Richardson number.

    References
    ----------
    .. [Norman2000] Norman, J. M., W. P. Kustas, J. H. Prueger, and G. R. Diak (2000),
        Surface flux estimation using radiometric temperature: A dual-temperature-difference
        method to minimize measurement errors, Water Resour. Res., 36(8), 2263-2274,
        http://dx.doi.org/10.1029/2000WR900033.
    '''

    # See eq (2) from Louis 1979
    Ri = (-(gravity * (z_u - d_0) / T_A1) * (((T_R1 - T_R0) -
                                              (T_A1 - T_A0)) / u**2)
          )  # equation (12) [Norman2000]
    return np.asarray(Ri)


def calc_u_star(u, z_u, L, d_0, z_0M):
    '''Friction velocity.

    Parameters
    ----------
    u : float
        wind speed above the surface (m s-1).
    z_u : float
        wind speed measurement height (m).
    L : float
        Monin Obukhov stability length (m).
    d_0 : float
        zero-plane displacement height (m).
    z_0M : float
        aerodynamic roughness length for momentum transport (m).

    References
    ----------
    .. [Brutsaert2005] Brutsaert, W. (2005). Hydrology: an introduction (Vol. 61, No. 8).
        Cambridge: Cambridge University Press.
    '''

    # Covert input scalars to numpy arrays
    u, z_u, L, d_0, z_0M = map(np.asarray, (u, z_u, L, d_0, z_0M))

    # calculate correction factors in other conditions
    L[L == 0.0] = 1e-36
    Psi_M = calc_Psi_M((z_u - d_0) / L)
    Psi_M0 = calc_Psi_M(z_0M / L)
    del L
    u_star = u * k / (np.log((z_u - d_0) / z_0M) - Psi_M + Psi_M0)
    return np.asarray(u_star)
