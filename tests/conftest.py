import sys
from unittest.mock import MagicMock


def mock_if_missing(module_names):
    for name in module_names:
        try:
            __import__(name)
        except (ImportError, AttributeError, ModuleNotFoundError):
            if name not in sys.modules:
                m = MagicMock()
                # Ensure mocked packages have __path__ and __version__ where needed
                if "." in name or name in ["cartopy", "monetio"]:
                    m.__path__ = []
                if name == "cartopy":
                    m.__version__ = "0.22.0"
                sys.modules[name] = m


mock_if_missing(
    [
        "cartopy",
        "cartopy.crs",
        "cartopy.feature",
        "cartopy.mpl.gridliner",
        "cartopy.io.shapereader",
        "cartopy.mpl",
        "cartopy.mpl.feature_artist",
        "cartopy.mpl.geoaxes",
        "cartopy.mpl.ticker",
        "pydecorate",
        "xregrid",
        "monet_stats",
        "pytspack",
        "mpi4py",
    ]
)

# For monet_regrid, keep it as it was or make it conditional
if "monet_regrid" not in sys.modules:
    try:
        # Use find_spec instead of import to avoid unused import warning
        import importlib.util

        if importlib.util.find_spec("monet_regrid") is None:
            sys.modules["monet_regrid"] = MagicMock()
    except Exception:
        sys.modules["monet_regrid"] = MagicMock()
