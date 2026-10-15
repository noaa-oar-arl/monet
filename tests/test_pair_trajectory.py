import unittest
from unittest.mock import patch

import numpy as np
import pandas as pd
import xarray as xr


class TestPairTrajectory(unittest.TestCase):
    def setUp(self):
        # Create dummy observations (moving platform)
        times_obs = pd.date_range("2024-01-01", periods=10, freq="1h")
        nodes = np.arange(5)
        self.obs = xr.Dataset(
            {"data": (("time", "node"), np.random.rand(10, 5))},
            coords={
                "time": times_obs,
                "node": nodes,
                "latitude": (("time", "node"), np.random.uniform(30, 40, (10, 5))),
                "longitude": (("time", "node"), np.random.uniform(-100, -90, (10, 5))),
            },
        )

        # Create dummy model (gridded)
        times_model = pd.date_range("2024-01-01", periods=5, freq="2h")  # Different time steps
        lat = np.linspace(25, 45, 10)
        lon = np.linspace(-105, -85, 10)
        self.model = xr.Dataset(
            {"data": (("time", "y", "x"), np.random.rand(5, 10, 10))},
            coords={
                "time": times_model,
                "latitude": (("y", "x"), np.broadcast_to(lat[:, None], (10, 10))),
                "longitude": (("y", "x"), np.broadcast_to(lon[None, :], (10, 10))),
            },
        )

    @patch("monet.accessors.base.BaseAccessor.remap")
    def test_pair_trajectory_interp_false(self, mock_remap):
        # Mock remap return value
        # It should return data with original name 'data', same as model.
        mock_ret = xr.Dataset(
            {"data": (("valid_time", "time", "node"), np.ones((5, 10, 5)))},
            coords={"valid_time": self.model.time.values, "time": self.obs.time.values, "node": self.obs.node.values},
        )
        mock_remap.return_value = mock_ret

        # Call pair with interp_time=False (default)
        self.model.monet.pair(self.obs, method="bilinear", interp_time=False)

        # Verify remap was called
        self.assertTrue(mock_remap.called)

        # Verify arguments
        args, kwargs = mock_remap.call_args
        # When patching a class method with MagicMock, 'self' is usually NOT passed
        # unless autospec=True is used. Here args[0] is the first explicit argument (model).

        model_passed = args[0]

        # Spatial remapping must preserve model time under its temporary name,
        # rather than expanding model data to observation times before remapping.
        self.assertEqual(model_passed.sizes["valid_time"], 5)
        np.testing.assert_array_equal(model_passed.valid_time, self.model.time)

    @patch("monet.accessors.base.BaseAccessor.remap")
    def test_pair_trajectory_interp_true(self, mock_remap):
        mock_ret = xr.Dataset(
            {"data": (("valid_time", "time", "node"), np.ones((5, 10, 5)))},
            coords={"valid_time": self.model.time.values, "time": self.obs.time.values, "node": self.obs.node.values},
        )
        mock_remap.return_value = mock_ret

        self.model.monet.pair(self.obs, method="bilinear", interp_time=True)

        self.assertTrue(mock_remap.called)
        args, kwargs = mock_remap.call_args
        model_passed = args[0]

        self.assertEqual(model_passed.sizes["valid_time"], 5)
        np.testing.assert_array_equal(model_passed.valid_time, self.model.time)


if __name__ == "__main__":
    unittest.main()


def test_axis_real_trajectory():
    import pytest

    from monet.util.resample import has_axis

    if not has_axis:
        pytest.skip("AXIS not available")
    times = pd.date_range("2024-01-01", periods=3, freq="h")
    model = xr.Dataset(
        {"temp": (("time", "lat", "lon"), np.broadcast_to(np.arange(3.0)[:, None, None], (3, 3, 3)))},
        coords={"time": times, "lat": [30.0, 35.0, 40.0], "lon": [-100.0, -95.0, -90.0]},
    )
    obs = xr.Dataset(
        {"obs": (("time", "station"), np.zeros((3, 2)))},
        coords={
            "time": times,
            "station": [0, 1],
            "latitude": (("time", "station"), [[32.0, 36.0], [33.0, 37.0], [34.0, 38.0]]),
            "longitude": (("time", "station"), [[-98.0, -93.0], [-97.0, -92.0], [-96.0, -91.0]]),
        },
    )
    original = model.copy(deep=True)
    out = model.monet.pair(obs, merge=False)
    assert out.temp.dims == ("time", "station")
    np.testing.assert_allclose(out.temp, np.broadcast_to(np.arange(3.0)[:, None], (3, 2)))
    xr.testing.assert_identical(model, original)
