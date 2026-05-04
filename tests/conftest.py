import importlib.machinery
import sys
from unittest.mock import MagicMock


def mock_if_missing(module_names):
    for name in module_names:
        try:
            __import__(name)
        except (ImportError, AttributeError, ModuleNotFoundError):
            if name not in sys.modules:
                m = MagicMock()
                # Use a real ModuleSpec to avoid xarray/importlib issues
                m.__spec__ = importlib.machinery.ModuleSpec(name, None)
                # Ensure mocked packages have __path__ and __version__ where needed
                if "." in name or name in ["cartopy", "monetio"]:
                    m.__path__ = []
                if name == "cartopy":
                    m.__version__ = "0.22.0"
                sys.modules[name] = m


# Only mock truly optional dependencies.
# Core dependencies like dask, xregrid, monet_stats, pytspack should NOT be mocked
# so that Vectorized Protocol compliance and proper test skipping can be verified.
mock_if_missing(
    [
        "monetio",
        "geopandas",
        "rasterio",
        "shapely",
    ]
)
