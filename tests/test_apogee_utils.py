import os
os.environ["ASTRA_DATABASE_PATH"] = ":memory:"

import numpy as np
import pytest


class TestTransformErrToIvar:

    def test_basic_conversion(self):
        from astra.models.apogee import _transform_err_to_ivar

        err = np.array([[0.5, 1.0, 2.0]])
        ivar = _transform_err_to_ivar(err)
        np.testing.assert_allclose(ivar, [4.0, 1.0, 0.25])

    def test_zero_error_gives_zero_ivar(self):
        from astra.models.apogee import _transform_err_to_ivar

        err = np.array([[0.0, 1.0]])
        ivar = _transform_err_to_ivar(err)
        assert ivar[0] == 0.0
        np.testing.assert_allclose(ivar[1], 1.0)

    def test_nan_error_gives_zero_ivar(self):
        from astra.models.apogee import _transform_err_to_ivar

        err = np.array([[np.nan, 1.0]])
        ivar = _transform_err_to_ivar(err)
        assert ivar[0] == 0.0
        np.testing.assert_allclose(ivar[1], 1.0)

    def test_inf_error_gives_zero_ivar(self):
        from astra.models.apogee import _transform_err_to_ivar

        err = np.array([[np.inf, 2.0]])
        ivar = _transform_err_to_ivar(err)
        assert ivar[0] == 0.0
        np.testing.assert_allclose(ivar[1], 0.25)

    def test_1d_input(self):
        """1D input should be handled by atleast_2d."""
        from astra.models.apogee import _transform_err_to_ivar

        err = np.array([0.5, 1.0])
        ivar = _transform_err_to_ivar(err)
        np.testing.assert_allclose(ivar, [4.0, 1.0])
