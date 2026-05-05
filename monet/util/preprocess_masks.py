"""Script to preprocess masks for MONET."""

import argparse

from monet.util.mask import get_mask


def preprocess_all(resolution: float = 0.05) -> None:
    """Preprocess all standard regions at the given resolution.

    Parameters
    ----------
    resolution : float, default: 0.05
        Resolution of the masks in degrees.
    """
    regions = ["giorgi", "ipcc_ar6", "epa_eco", "timezones", "epa_admin", "land"]
    for region in regions:
        print(f"Preprocessing {region}...")
        try:
            get_mask(region, resolution=resolution)
        except Exception as e:
            print(f"Failed to preprocess {region}: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Preprocess MONET masks")
    parser.add_argument("--resolution", type=float, default=0.05, help="Resolution in degrees")
    args = parser.parse_args()
    preprocess_all(resolution=args.resolution)
