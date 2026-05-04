"""
Statistics utilities module - compatibility layer for monet_stats.

This module imports monet_stats and re-exports all functions for backward compatibility.
The actual statistical functions are now provided by the monet-stats package.
"""

try:
    import monet_stats as _monet_stats

    # Build an explicit public API instead of using star-import so that
    # IDEs and documentation tools can introspect available symbols.
    __all__ = [name for name in dir(_monet_stats) if not name.startswith("_")]

    # Populate this module's namespace from monet_stats
    import sys as _sys

    _this = _sys.modules[__name__]
    for _name in __all__:
        setattr(_this, _name, getattr(_monet_stats, _name))

    # Convenience alias kept for backward compatibility
    if hasattr(_monet_stats, "stats"):
        stats = _monet_stats.stats

except ImportError:
    import warnings

    warnings.warn(
        "monet_stats package is not installed. " "Install it with: pip install monet-stats",
        ImportWarning,
        stacklevel=2,
    )

    __all__ = ["stats"]

    def stats(*args, **kwargs):
        raise ImportError("monet_stats package is required for statistical functions. " "Install with: pip install monet-stats")
