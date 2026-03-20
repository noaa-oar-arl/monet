import dask.array as da
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pytest
import xarray as xr
from PIL import Image

from monet.plots import _dynamic_fig_size, savefig, sp_scatter_bias


@pytest.fixture
def sample_fig():
    fig, ax = plt.subplots(figsize=(5, 5))
    ax.plot([0, 1], [0, 1])
    ax.set_title("Test Figure")
    yield fig
    plt.close(fig)


def test_savefig_basic(sample_fig, tmp_path):
    fname = tmp_path / "test_plot.png"
    savefig(str(fname), decorate=False, fig=sample_fig)
    assert fname.exists()

    # Check that it's a valid image
    with Image.open(fname) as img:
        assert img.format == "PNG"


def test_savefig_with_logo(sample_fig, tmp_path):
    fname = tmp_path / "test_logo.png"
    # This should use the default MONET logo
    savefig(str(fname), decorate=True, loc=1, fig=sample_fig)
    assert fname.exists()

    with Image.open(fname) as img:
        # Default dpi=100 for 5x5 fig is 500x500
        assert img.size == (500, 500)


def test_savefig_logo_positions(sample_fig, tmp_path):
    for loc in [1, 2, 3, 4]:
        fname = tmp_path / f"test_logo_{loc}.png"
        savefig(str(fname), decorate=True, loc=loc, logo_height=50, fig=sample_fig)
        assert fname.exists()


def test_savefig_invalid_ext(sample_fig, tmp_path):
    fname = tmp_path / "test.pdf"
    with pytest.raises(ValueError, match="only PNG and JPEG supported"):
        savefig(str(fname), fig=sample_fig)


def test_savefig_no_ext(sample_fig, tmp_path):
    fname = tmp_path / "test"
    with pytest.raises(ValueError, match="must include a file extension"):
        savefig(str(fname), fig=sample_fig)


def test_savefig_invalid_loc(sample_fig, tmp_path):
    fname = tmp_path / "test_loc.png"
    with pytest.raises(ValueError, match="invalid `loc`"):
        savefig(str(fname), loc=5, fig=sample_fig)


def test_savefig_custom_logo(sample_fig, tmp_path):
    # Create a dummy logo
    logo_path = tmp_path / "custom_logo.png"
    logo_img = Image.new("RGBA", (100, 100), color="red")
    logo_img.save(logo_path)

    fname = tmp_path / "test_custom_logo.png"
    savefig(str(fname), decorate=True, logo=str(logo_path), logo_height=20, fig=sample_fig)
    assert fname.exists()

    with Image.open(fname) as img:
        assert img.size == (500, 500)


def test_dynamic_fig_size_aero():
    # Eager
    da_eager = xr.DataArray(np.zeros((10, 20)), dims=["lat", "lon"], coords={"lat": range(10), "lon": range(20)})
    size_eager = _dynamic_fig_size(da_eager)
    assert isinstance(size_eager, tuple)
    assert size_eager[0] == 10
    assert size_eager[1] == 5.0

    # Lazy
    da_lazy = xr.DataArray(da.zeros((10, 20), chunks=5), dims=["lat", "lon"], coords={"lat": range(10), "lon": range(20)})
    size_lazy = _dynamic_fig_size(da_lazy)
    assert size_lazy == size_eager


def test_savefig_full_pipeline_aero(tmp_path):
    # Demonstrate plotting a Dask-backed array and then saving
    da_lazy = xr.DataArray(da.random.random((10, 10), chunks=5), dims=["x", "y"], name="test")
    fig, ax = plt.subplots()
    da_lazy.plot(ax=ax)
    fname = tmp_path / "full_pipeline_lazy.png"
    savefig(str(fname), fig=fig, decorate=True, logo_height=30)
    assert fname.exists()
    plt.close(fig)


def test_sp_scatter_bias_basic():
    df = pd.DataFrame({"latitude": [40, 41], "longitude": [-70, -71], "obs": [1, 2], "mod": [1.1, 1.9]})
    ax = sp_scatter_bias(df, col1="obs", col2="mod")
    assert ax is not None
    plt.close(ax.figure)


def test_sp_scatter_bias_invalid():
    df = pd.DataFrame({"latitude": [40], "longitude": [-70]})
    with pytest.raises(ValueError, match="User must specify col1 and col2"):
        sp_scatter_bias(df)
