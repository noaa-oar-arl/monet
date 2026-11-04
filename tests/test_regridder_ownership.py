"""MONET retains lazy regridders during use and releases engines before teardown."""

import gc
import subprocess
import sys
import weakref

import numpy as np
import pytest
import xarray as xr

import monet.util.resample as backend


class TrackedEngine:
    def __init__(self, events):
        self.events = events
        self._dims_source = ("lat", "lon")

    def __call__(self, data):
        return data.copy()

    def close(self):
        self.events.append("closed")


@pytest.fixture
def registry(monkeypatch):
    live = weakref.WeakSet()
    monkeypatch.setattr(backend, "_LIVE_REGRIDDERS", live)
    return live


def make_owner(events):
    grid = xr.Dataset(coords={"lat": [0.0], "lon": [0.0]})
    return backend.PersistentRegridder(TrackedEngine(events), grid, grid, "nearest", {}, {}, {})


def test_close_releases_engine_while_owner_stays_reachable(registry):
    events = []
    owner = make_owner(events)
    engine = weakref.ref(owner._engine)
    owner.close()
    owner.close()
    assert engine() is None
    assert events == ["closed"]
    assert owner not in registry
    with pytest.raises(RuntimeError, match="closed"):
        owner(xr.Dataset())
    with pytest.raises(RuntimeError, match="closed"):
        owner.to_file("unused.weights")


def test_shutdown_releases_engines_for_retained_owners(registry):
    events = []
    owners = [make_owner(events), make_owner(events)]
    engines = [weakref.ref(owner._engine) for owner in owners]
    backend._close_regridders()
    assert all(engine() is None for engine in engines)
    assert events == ["closed", "closed"]
    assert not registry


def test_registry_does_not_keep_owners_alive(registry):
    owner = make_owner([])
    reference = weakref.ref(owner)
    del owner
    gc.collect()
    assert reference() is None
    assert not registry


@pytest.mark.parametrize("lazy", [False, True], ids=["eager", "lazy"])
def test_resample_keeps_engine_only_while_lazy_output_is_open(monkeypatch, registry, lazy):
    events = []
    engines = []

    def create(*args, **kwargs):
        owner = make_owner(events)
        engines.append(weakref.ref(owner._engine))
        return owner

    monkeypatch.setattr(backend, "create_regridder", create)
    source = xr.Dataset({"field": (("lat", "lon"), np.ones((1, 1)))})
    if lazy:
        source = source.chunk({"lat": -1, "lon": -1})
    result = backend.resample(source, source)
    if lazy:
        assert engines[0]() is not None
        assert not events
        result.load()
        assert engines[0]() is not None
        result.close()
    assert engines[0]() is None
    assert events == ["closed"]
    np.testing.assert_array_equal(result.field, [[1.0]])


def test_resample_closes_engine_when_apply_fails(monkeypatch, registry):
    events = []
    owner = make_owner(events)
    engines = weakref.ref(owner._engine)

    def fail(*args, **kwargs):
        raise ValueError("apply failed")

    monkeypatch.setattr(TrackedEngine, "__call__", fail)
    monkeypatch.setattr(backend, "create_regridder", lambda *args, **kwargs: owner)
    with pytest.raises(ValueError, match="apply failed"):
        backend.resample(xr.Dataset(), xr.Dataset())
    assert engines() is None
    assert events == ["closed"]


def test_shutdown_callback_releases_engine_before_module_teardown():
    code = """
import atexit
import weakref

def report():
    assert reference() is None
    print('engine released before module teardown')

atexit.register(report)
from monet.util.resample import PersistentRegridder

class Engine:
    def close(self):
        pass

engine = Engine()
reference = weakref.ref(engine)
owner = PersistentRegridder(engine, None, None, 'nearest', {}, {}, {})
del engine
assert reference() is not None
"""
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=30, check=False)
    assert result.returncode == 0, result.stderr
    assert "engine released before module teardown" in result.stdout
