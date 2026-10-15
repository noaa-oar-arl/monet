import numpy as np
import pytest
import xarray as xr

from monet.accessors.base import has_axis
from monet.util.resample import resample


def make_test_data(nx=10, ny=10, dask=False):
    ds = xr.Dataset(
        {"var1": (("y", "x"), np.random.rand(ny, nx))},
        coords={
            "latitude": (("y", "x"), np.meshgrid(np.linspace(0, 10, nx), np.linspace(0, 10, ny))[1]),
            "longitude": (("y", "x"), np.meshgrid(np.linspace(0, 10, nx), np.linspace(0, 10, ny))[0]),
        },
    )
    if dask:
        ds = ds.chunk({"y": 5, "x": 5})
    return ds


def make_target_grid(nx=5, ny=5):
    ds = xr.Dataset(
        coords={
            "latitude": (("y", "x"), np.meshgrid(np.linspace(0, 10, nx), np.linspace(0, 10, ny))[1]),
            "longitude": (("y", "x"), np.meshgrid(np.linspace(0, 10, nx), np.linspace(0, 10, ny))[0]),
        },
    )
    return ds


@pytest.mark.skipif(not has_axis, reason="AXIS not available")
def test_resample_eager():
    source = make_test_data(dask=False)
    target = make_target_grid()

    # Test nearest
    out_nearest = resample(source, target, method="nearest")
    assert out_nearest["var1"].shape == (5, 5)
    assert isinstance(out_nearest["var1"].values, np.ndarray)

    # Test bilinear
    out_linear = resample(source, target, method="bilinear")
    assert out_linear["var1"].shape == (5, 5)
    assert isinstance(out_linear["var1"].values, np.ndarray)


@pytest.mark.skipif(not has_axis, reason="AXIS not available")
def test_resample_lazy():
    source = make_test_data(dask=True)
    target = make_target_grid()

    # Test nearest
    out_nearest = resample(source, target, method="nearest")
    assert out_nearest["var1"].shape == (5, 5)
    # Check if it is a dask-backed array
    assert hasattr(out_nearest["var1"].data, "chunks")

    # Test bilinear
    out_linear = resample(source, target, method="bilinear")
    assert out_linear["var1"].shape == (5, 5)
    assert hasattr(out_linear["var1"].data, "chunks")


@pytest.mark.skipif(not has_axis, reason="AXIS not available")
def test_resample_dataarray():
    source = make_test_data(dask=False)["var1"]
    target = make_target_grid()

    out = resample(source, target, method="nearest")
    assert out.shape == (5, 5)
    assert isinstance(out, xr.DataArray)


@pytest.mark.parametrize(
    "alias,expected",
    [
        ("linear", "bilinear"),
        ("nearest", "nearest"),
        ("nearest_s2d", "nearest"),
        ("nearest_d2s", "nearest"),
        ("bilinear", "bilinear"),
        ("conservative", "conservative"),
        ("bicubic", "bicubic"),
        ("patch", "patch"),
        ("conservative2nd", "conservative2nd"),
    ],
)
def test_axis_method_contract(monkeypatch, alias, expected):
    from unittest.mock import Mock

    import monet.util.resample as backend

    source = make_test_data()
    original = source.copy(deep=True)
    engine = Mock(side_effect=lambda obj: obj.copy(deep=False))
    constructor = Mock(return_value=engine)
    monkeypatch.setattr(backend, "has_axis", True)
    monkeypatch.setattr(backend, "Regridder", constructor, raising=False)
    out = backend.resample(source, source, method=alias, radius_of_influence=1, n_processes=2, periodic=True)
    assert constructor.call_args.kwargs == {"method": expected, "periodic": True}
    assert "backend=AXIS" in out.attrs["history"]
    xr.testing.assert_identical(source, original)


def test_axis_unknown_method(monkeypatch):
    import monet.util.resample as backend

    monkeypatch.setattr(backend, "has_axis", True)
    with pytest.raises(ValueError, match="bilinear.*nearest"):
        backend.resample(make_test_data(), make_target_grid(), method="bogus")


def test_axis_missing_engine(monkeypatch):
    import monet.accessors.base as accessors
    import monet.accessors.pandas_accessor as pandas_accessor
    import monet.util.resample as backend

    monkeypatch.setattr(backend, "has_axis", False)
    monkeypatch.setattr(accessors, "has_axis", False)
    monkeypatch.setattr(pandas_accessor, "has_axis", False)
    for call in (
        lambda: backend.resample(make_test_data(), make_target_grid()),
        lambda: make_target_grid().monet.remap(make_test_data()),
        lambda: backend.create_regridder(make_test_data(), make_target_grid()),
    ):
        with pytest.raises(ImportError, match="AXIS.*axis-regrid.*pip install"):
            call()


def test_axis_deprecated_flags():
    import monet.accessors.base as accessors
    import monet.monet_accessor as compat
    import monet.util.resample as backend

    for module in (backend, compat, accessors):
        with pytest.warns(DeprecationWarning, match="has_axis"):
            assert module.has_xregrid is backend.has_axis


@pytest.mark.skipif(not has_axis, reason="AXIS not available")
def test_axis_unnamed_array_and_coordinate_errors():
    source = make_test_data()["var1"].rename(None)
    out = resample(source, make_target_grid())
    assert out.name is None
    assert out.dims == ("y", "x")
    for side in ("source", "target"):
        broken = xr.Dataset(coords={"x": [0.0, 1.0]})
        with pytest.raises(KeyError, match=side):
            resample(broken if side == "source" else source, broken if side == "target" else make_target_grid())
    with pytest.raises(ValueError, match="empty"):
        resample(source, make_target_grid().isel(x=slice(0, 0)))


@pytest.mark.skipif(not has_axis, reason="AXIS not available")
def test_axis_persistent_weights(tmp_path):
    from unittest.mock import patch

    from monet.util.resample import create_regridder

    source = make_test_data()
    target = make_target_grid()
    rg = create_regridder(source, target, method="nearest")
    out = rg(source)
    np.testing.assert_allclose(out.var1, resample(source, target).var1)
    np.testing.assert_allclose(rg(source.var1 * 2), out.var1 * 2)
    path = tmp_path / "weights.bin"
    rg.to_file(path)
    # Reloading must bypass fit/weight generation, not merely produce the same values.
    import axis

    with patch.object(axis.Regridder, "fit", side_effect=AssertionError("weights regenerated")):
        restored = create_regridder(source, target, method="nearest", weights_file=path)
        np.testing.assert_allclose(restored(source).var1, out.var1)


@pytest.mark.skipif(not has_axis, reason="AXIS not available")
def test_axis_lazy_values_and_attrs():
    source = make_test_data()
    source.var1.attrs["units"] = "K"
    source.attrs["history"] = "original"
    expected = resample(source, make_target_grid(), method="bilinear")
    lazy = resample(source.chunk({"y": 5, "x": 5}), make_target_grid(), method="bilinear")
    assert hasattr(lazy.var1.data, "dask")
    xr.testing.assert_allclose(lazy.compute(), expected)
    assert lazy.var1.attrs["units"] == "K"
    assert source.attrs["history"] == "original"


def test_axis_native_import_failure(monkeypatch):
    """A broken native library must not make MONET's module unimportable."""
    import importlib.util
    import sys
    from pathlib import Path
    from types import ModuleType

    broken = ModuleType("axis")

    def fail(name):
        raise OSError("incompatible native runtime")

    broken.__getattr__ = fail
    monkeypatch.setitem(sys.modules, "axis", broken)
    spec = importlib.util.spec_from_file_location(
        "_monet_axis_import_probe", Path(__file__).parents[1] / "monet" / "util" / "resample.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module.has_axis is False
    with pytest.raises(ImportError, match="AXIS") as exc:
        module.resample(make_test_data(), make_target_grid())
    assert isinstance(exc.value.__cause__, OSError)


@pytest.mark.skipif(not has_axis, reason="AXIS not available")
def test_axis_distributed_workers():
    distributed = pytest.importorskip("distributed")
    source = make_test_data()
    target = make_target_grid()
    expected = resample(source, target, method="bilinear")
    with distributed.LocalCluster(
        n_workers=2, threads_per_worker=1, processes=True, dashboard_address=None, memory_limit=0
    ) as cluster:
        with distributed.Client(cluster):
            actual = resample(source.chunk({"y": 5, "x": 5}), target, method="bilinear")
            assert hasattr(actual.var1.data, "dask")
            xr.testing.assert_allclose(actual.compute(), expected)
