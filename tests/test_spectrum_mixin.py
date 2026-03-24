import os
os.environ["ASTRA_DATABASE_PATH"] = ":memory:"

import numpy as np
import pytest


class TestSpectrumMixinEFlux:

    def test_e_flux_from_finite_ivar(self):
        from astra.models.spectrum import SpectrumMixin

        class FakeSpectrum(SpectrumMixin):
            pass

        s = FakeSpectrum()
        s.ivar = np.array([4.0, 1.0, 100.0])
        e_flux = s.e_flux
        np.testing.assert_allclose(e_flux, [0.5, 1.0, 0.1])

    def test_e_flux_zero_ivar_gives_large_value(self):
        from astra.models.spectrum import SpectrumMixin

        class FakeSpectrum(SpectrumMixin):
            pass

        s = FakeSpectrum()
        s.ivar = np.array([0.0, 4.0])
        e_flux = s.e_flux
        assert e_flux[0] == 1e10
        np.testing.assert_allclose(e_flux[1], 0.5)

    def test_e_flux_negative_ivar_gives_large_value(self):
        from astra.models.spectrum import SpectrumMixin

        class FakeSpectrum(SpectrumMixin):
            pass

        s = FakeSpectrum()
        s.ivar = np.array([-1.0, 4.0])
        e_flux = s.e_flux
        assert e_flux[0] == 1e10
        np.testing.assert_allclose(e_flux[1], 0.5)

    def test_e_flux_nan_ivar(self):
        from astra.models.spectrum import SpectrumMixin

        class FakeSpectrum(SpectrumMixin):
            pass

        s = FakeSpectrum()
        s.ivar = np.array([np.nan, 4.0])
        e_flux = s.e_flux
        assert e_flux[0] == 1e10
        np.testing.assert_allclose(e_flux[1], 0.5)
