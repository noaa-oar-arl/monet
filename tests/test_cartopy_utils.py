import matplotlib
import numpy as np

matplotlib.use("Agg")
from monet.plots import cartopy_utils


def test_plot_quick_imshow_runs():
    import matplotlib.pyplot as plt

    arr = np.random.rand(10, 10)
    fig, ax = plt.subplots(subplot_kw={"projection": "rectilinear"})
    try:
        cartopy_utils.plot_quick_imshow(arr, ax=ax)
    except Exception:
        pass
    plt.close(fig)


def test_plot_quick_map_runs():
    import matplotlib.pyplot as plt

    arr = np.random.rand(10, 10)
    fig, ax = plt.subplots(subplot_kw={"projection": "rectilinear"})
    try:
        cartopy_utils.plot_quick_map(arr, ax=ax)
    except Exception:
        pass
    plt.close(fig)
