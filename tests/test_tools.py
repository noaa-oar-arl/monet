import numpy as np
import pandas as pd
import pytest

from monet import util
from monet.util import tools


@pytest.mark.parametrize("mod", [tools, util], ids=["tools", "util"])
def test_calc_3hr_and_annual_ave(mod):
    """3-hour and annual averages per site, across a year boundary."""
    t = pd.date_range("2025-12-30", periods=24 * 400, freq="h")
    df = pd.DataFrame(
        {
            "time_local": np.tile(t, 2),
            "siteid": np.repeat(["A", "B"], len(t)),
            "o3": np.arange(2 * len(t), dtype=float),
        }
    )

    three = mod.calc_3hr_ave(df.copy(), col="o3")
    assert len(three) == 2 * len(t) // 3
    assert three.loc[0, "o3_y"] == 1.0  # mean of hours 0, 1 and 2

    annual = mod.calc_annual_ave(df.copy(), col="o3")
    site_a = annual[annual.siteid == "A"].set_index("time_local")["o3_y"]
    assert site_a[pd.Timestamp("2025-12-31")] == 23.5  # first 48 hours
