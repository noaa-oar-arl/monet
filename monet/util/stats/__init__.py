"""
Statistics submodule for MONET utility functions.
"""

# Expose all functions from all stats submodules
# Dynamically build __all__ from all submodules

# Explicit imports for all public API symbols (for lint compliance)
from .contingency_metrics import CSI, ETS, FAR, FBI, HSS, POD, TSS, scores
from .correlation_metrics import (
    AC,
    E1,
    IOA,
    KGE,
    R2,
    RMSE,
    WDAC,
    WDIOA,
    WDRMSE,
    IOA_m,
    RMSEs,
    RMSEu,
    WDIOA_m,
    WDRMSE_m,
    d1,
    kendalltau,
    spearmanr,
    taylor_skill,
)
from .error_metrics import (
    MB,
    MNB,
    MNE,
    MO,
    MP,
    NO,
    NOP,
    NP,
    RM,
    STDO,
    STDP,
    WDMB,
    MdnB,
    MdnNB,
    MdnNE,
    MdnO,
    MdnP,
    NMdnGE,
    RMdn,
    WDMB_m,
    WDMdnB,
)
from .relative_metrics import (
    FB,
    FE,
    ME,
    MNPB,
    MNPE,
    NMB,
    NMB_ABS,
    NME,
    USUTPB,
    USUTPE,
    WDME,
    MdnE,
    MdnNPB,
    MdnNPE,
    NMdnB,
    NMdnE,
    NME_m,
    NME_m_ABS,
    WDMdnE,
    WDME_m,
    WDNMB_m,
)
from .spatial_ensemble_metrics import CRPS, EDS, FSS, SAL, spread_error
from .utils_stats import circlebias, circlebias_m, matchedcompressed, matchmasks

__all__ = [
    # contingency_metrics
    "HSS",
    "ETS",
    "CSI",
    "scores",
    "POD",
    "FAR",
    "FBI",
    "TSS",
    # correlation_metrics
    "R2",
    "RMSE",
    "WDRMSE_m",
    "WDRMSE",
    "RMSEs",
    "RMSEu",
    "d1",
    "E1",
    "IOA_m",
    "IOA",
    "WDIOA_m",
    "WDIOA",
    "AC",
    "WDAC",
    "taylor_skill",
    "KGE",
    "spearmanr",
    "kendalltau",
    # error_metrics
    "STDO",
    "STDP",
    "MNB",
    "MNE",
    "MdnNB",
    "MdnNE",
    "NMdnGE",
    "NO",
    "NOP",
    "NP",
    "MO",
    "MP",
    "MdnO",
    "MdnP",
    "RM",
    "RMdn",
    "MB",
    "MdnB",
    "WDMB_m",
    "WDMB",
    "WDMdnB",
    # relative_metrics
    "NMB",
    "WDNMB_m",
    "NMB_ABS",
    "NMdnB",
    "FB",
    "ME",
    "MdnE",
    "WDME_m",
    "WDME",
    "WDMdnE",
    "NME_m",
    "NME_m_ABS",
    "NME",
    "NMdnE",
    "FE",
    "USUTPB",
    "USUTPE",
    "MNPB",
    "MdnNPB",
    "MNPE",
    "MdnNPE",
    # spatial_ensemble_metrics
    "FSS",
    "EDS",
    "CRPS",
    "spread_error",
    "SAL",
    # utils_stats
    "matchedcompressed",
    "matchmasks",
    "circlebias_m",
    "circlebias",
]


def stats(df, minval, maxval):
    """Short summary.

    Parameters
    ----------
    df : type
        Description of parameter `df`.
    minval : type
        Description of parameter `minval`.
    maxval : type
        Description of parameter `maxval`.

    Returns
    -------
    type
        Description of returned object.

    """
    from numpy import sqrt

    dd = {}
    dd["N"] = df.Obs.dropna().count()
    dd["Obs"] = df.Obs.mean()
    dd["Mod"] = df.CMAQ.mean()
    dd["MB"] = MB(df.Obs.values, df.CMAQ.values)  # mean bias
    dd["R"] = sqrt(R2(df.Obs.values, df.CMAQ.values))  # pearsonr ** 2
    dd["IOA"] = IOA(df.Obs.values, df.CMAQ.values)  # Index of Agreement
    dd["RMSE"] = RMSE(df.Obs.values, df.CMAQ.values)
    dd["NMB"] = NMB(df.Obs.values, df.CMAQ.values)
    try:
        a, b, c, d = scores(df.Obs.values, df.CMAQ.values, 70, 1000)
        dd["POD"] = a / (a + b)
        dd["FAR"] = c / (a + c)
    except Exception:
        dd["POD"] = 1.0
        dd["FAR"] = 0.0
    return dd
