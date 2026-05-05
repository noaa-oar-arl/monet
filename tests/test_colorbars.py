import matplotlib

matplotlib.use("Agg")
from monet.plots import colorbars


def test_colorbar_index_runs():
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots()
    cb = colorbars.colorbar_index(ncolors=3, cmap="viridis", ax=ax)
    assert cb is not None
    plt.close(fig)


def test_cmap_discretize():
    cmap = colorbars.cmap_discretize("viridis", 5)
    assert hasattr(cmap, "__call__")
