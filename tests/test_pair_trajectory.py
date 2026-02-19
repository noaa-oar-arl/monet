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
        mock_ret = self.obs.copy()
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

        # Check if model_passed time matches obs time (10 steps)
        # Fixed code should pass model aligned to obs (10 steps) using reindex.
        self.assertEqual(model_passed.dims["time"], 10)
        xr.testing.assert_equal(model_passed.time, self.obs.time)

    @patch("monet.accessors.base.BaseAccessor.remap")
    def test_pair_trajectory_interp_true(self, mock_remap):
        mock_ret = self.obs.copy()
        mock_remap.return_value = mock_ret

        self.model.monet.pair(self.obs, method="bilinear", interp_time=True)

        self.assertTrue(mock_remap.called)
        args, kwargs = mock_remap.call_args
        model_passed = args[0]

        self.assertEqual(model_passed.dims["time"], 10)
        xr.testing.assert_equal(model_passed.time, self.obs.time)


if __name__ == "__main__":
    unittest.main()
