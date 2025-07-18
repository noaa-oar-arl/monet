"""
Statistics submodule for MONET utility functions.
"""

# Import commonly used metrics for convenience
from .error_metrics import *
from .relative_metrics import *
from .correlation_metrics import *
from .spatial_ensemble_metrics import *
from .contingency_metrics import *
from .utils_stats import *

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