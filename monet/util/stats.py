"""
Statistics utilities module - compatibility layer for monet_stats.

This module imports monet_stats and re-exports all functions for backward compatibility.
The actual statistical functions are now provided by the monet-stats package.
"""

try:
    # Import all functions from monet_stats for backward compatibility
    import monet_stats

    # Re-export all public functions
    from monet_stats import *  # noqa: F403

    # Keep the original stats function if it exists
    if hasattr(monet_stats, "stats"):
        stats = monet_stats.stats

except ImportError:
    import warnings

    warnings.warn(
        "monet_stats package is not installed. Please install it with 'pip install monet-stats' to use statistical functions.",
        ImportWarning,
    )

    # Define a placeholder function
    def stats(*args, **kwargs):
        raise ImportError("monet_stats package is required for statistical functions. Install with 'pip install monet-stats'")
