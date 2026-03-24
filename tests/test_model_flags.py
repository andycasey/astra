import os
os.environ["ASTRA_DATABASE_PATH"] = ":memory:"

import pytest
from astra.models.base import database


def _setup_tables(*models):
    """Create tables for the given models, including Source and Spectrum."""
    from astra.models.source import Source
    from astra.models.spectrum import Spectrum
    all_models = [Source, Spectrum] + list(models)
    database.create_tables(all_models)
    return Source, Spectrum


def _make_spectrum(Source, Spectrum):
    """Create a Source and Spectrum and return the spectrum_pk and source_pk."""
    source_pk = Source.create().pk
    spectrum_pk = Spectrum.create().pk
    return source_pk, spectrum_pk


class TestApogeeNetFlags:

    def setup_method(self):
        from astra.models.apogeenet import ApogeeNet
        self.ApogeeNet = ApogeeNet
        Source, Spectrum = _setup_tables(ApogeeNet)
        self.source_pk, self.spectrum_pk = _make_spectrum(Source, Spectrum)

    def _make_dummy_spectrum(self):
        class FakeSpectrum:
            pass
        s = FakeSpectrum()
        s.spectrum_pk = self.spectrum_pk
        s.source_pk = self.source_pk
        return s

    def test_no_params_sets_unreliable_fe_h(self):
        """When no parameters are given, flag_unreliable_fe_h should not be set
        (fe_h is None, teff is None, logg is None -> all conditions False)."""
        s = self._make_dummy_spectrum()
        r = self.ApogeeNet.from_spectrum(s)
        # With all None, none of the conditions for flag_unreliable_fe_h are True
        assert not r.flag_unreliable_fe_h
        assert not r.flag_unreliable_teff
        assert not r.flag_unreliable_logg

    def test_valid_params_no_flags(self):
        s = self._make_dummy_spectrum()
        r = self.ApogeeNet.from_spectrum(s, teff=5000, logg=3.0, fe_h=-1.0)
        assert not r.flag_unreliable_teff
        assert not r.flag_unreliable_logg
        assert not r.flag_unreliable_fe_h

    def test_teff_too_low(self):
        s = self._make_dummy_spectrum()
        r = self.ApogeeNet.from_spectrum(s, teff=1500, logg=3.0, fe_h=-1.0)
        assert r.flag_unreliable_teff
        # teff < 3200 also triggers unreliable_fe_h
        assert r.flag_unreliable_fe_h

    def test_teff_too_high(self):
        s = self._make_dummy_spectrum()
        r = self.ApogeeNet.from_spectrum(s, teff=200000, logg=3.0, fe_h=-1.0)
        assert r.flag_unreliable_teff

    def test_logg_too_low(self):
        s = self._make_dummy_spectrum()
        r = self.ApogeeNet.from_spectrum(s, teff=5000, logg=-2.0, fe_h=-1.0)
        assert r.flag_unreliable_logg

    def test_logg_too_high(self):
        s = self._make_dummy_spectrum()
        r = self.ApogeeNet.from_spectrum(s, teff=5000, logg=11.0, fe_h=-1.0)
        assert r.flag_unreliable_logg
        # logg > 5 also triggers unreliable_fe_h
        assert r.flag_unreliable_fe_h

    def test_fe_h_too_low(self):
        s = self._make_dummy_spectrum()
        r = self.ApogeeNet.from_spectrum(s, teff=5000, logg=3.0, fe_h=-5.0)
        assert r.flag_unreliable_fe_h

    def test_fe_h_too_high(self):
        s = self._make_dummy_spectrum()
        r = self.ApogeeNet.from_spectrum(s, teff=5000, logg=3.0, fe_h=3.0)
        assert r.flag_unreliable_fe_h

    def test_raw_errors_stored(self):
        s = self._make_dummy_spectrum()
        r = self.ApogeeNet.from_spectrum(s, teff=5000, logg=3.0, fe_h=-1.0,
                                         e_teff=50, e_logg=0.1, e_fe_h=0.05)
        assert r.raw_e_teff == 50
        assert r.raw_e_logg == 0.1
        assert r.raw_e_fe_h == 0.05

    def test_flag_bad_combines_flags(self):
        s = self._make_dummy_spectrum()
        # All flags unreliable
        r = self.ApogeeNet.from_spectrum(s, teff=1500, logg=-2.0, fe_h=-5.0)
        assert r.flag_bad

    def test_flag_bad_false_when_clean(self):
        s = self._make_dummy_spectrum()
        r = self.ApogeeNet.from_spectrum(s, teff=5000, logg=3.0, fe_h=-1.0)
        assert not r.flag_bad

    def test_flag_warn_equals_unreliable_fe_h(self):
        s = self._make_dummy_spectrum()
        r = self.ApogeeNet.from_spectrum(s, teff=5000, logg=3.0, fe_h=-5.0)
        assert r.flag_warn
        r2 = self.ApogeeNet.from_spectrum(s, teff=5000, logg=3.0, fe_h=-1.0)
        assert not r2.flag_warn


class TestMDwarfTypeFlags:

    def test_flag_bad_is_true_when_any_flag_set(self):
        from astra.models.mdwarftype import MDwarfType
        Source, Spectrum = _setup_tables(MDwarfType)
        source_pk, spectrum_pk = _make_spectrum(Source, Spectrum)

        r = MDwarfType(spectrum_pk=spectrum_pk, source_pk=source_pk)
        assert not r.flag_bad

        r.flag_suspicious = True
        assert r.flag_bad

    def test_flag_bad_with_exception_flag(self):
        from astra.models.mdwarftype import MDwarfType
        Source, Spectrum = _setup_tables(MDwarfType)
        source_pk, spectrum_pk = _make_specimen = _make_specimen = _make_spectrum(Source, Spectrum)

        r = MDwarfType(spectrum_pk=spectrum_pk, source_pk=source_pk)
        r.flag_exception = True
        assert r.flag_bad


class TestThePayneFlags:

    def test_flag_warn_true_when_any_flag(self):
        from astra.models.the_payne import ThePayne
        Source, Spectrum = _setup_tables(ThePayne)
        source_pk, spectrum_pk = _make_spectrum(Source, Spectrum)

        r = ThePayne(spectrum_pk=spectrum_pk, source_pk=source_pk)
        assert not r.flag_warn

        r.flag_warn_teff = True
        assert r.flag_warn

    def test_flag_bad_is_fitting_failure(self):
        from astra.models.the_payne import ThePayne
        Source, Spectrum = _setup_tables(ThePayne)
        source_pk, spectrum_pk = _make_spectrum(Source, Spectrum)

        r = ThePayne(spectrum_pk=spectrum_pk, source_pk=source_pk)
        assert not r.flag_bad

        r.flag_fitting_failure = True
        assert r.flag_bad


class TestSlamFlags:

    def test_flag_warn(self):
        from astra.models.slam import Slam
        Source, Spectrum = _setup_tables(Slam)
        source_pk, spectrum_pk = _make_spectrum(Source, Spectrum)

        r = Slam(spectrum_pk=spectrum_pk, source_pk=source_pk)
        assert not r.flag_warn

        r.flag_bad_optimizer_status = True
        assert r.flag_warn

        r2 = Slam(spectrum_pk=spectrum_pk, source_pk=source_pk)
        r2.flag_outside_photometry_range = True
        assert r2.flag_warn

    def test_flag_bad(self):
        from astra.models.slam import Slam
        Source, Spectrum = _setup_tables(Slam)
        source_pk, spectrum_pk = _make_spectrum(Source, Spectrum)

        r = Slam(spectrum_pk=spectrum_pk, source_pk=source_pk)
        assert not r.flag_bad

        r.flag_teff_outside_bounds = True
        assert r.flag_bad

        r2 = Slam(spectrum_pk=spectrum_pk, source_pk=source_pk)
        r2.flag_fe_h_outside_bounds = True
        assert r2.flag_bad


class TestAstroNNFlags:

    def test_flag_warn_true_when_any_flag(self):
        from astra.models.astronn import AstroNN
        Source, Spectrum = _setup_tables(AstroNN)
        source_pk, spectrum_pk = _make_spectrum(Source, Spectrum)

        r = AstroNN(spectrum_pk=spectrum_pk, source_pk=source_pk)
        assert not r.flag_warn

        r.flag_uncertain_teff = True
        assert r.flag_warn

    def test_flag_bad_requires_all_three(self):
        from astra.models.astronn import AstroNN
        Source, Spectrum = _setup_tables(AstroNN)
        source_pk, spectrum_pk = _make_spectrum(Source, Spectrum)

        r = AstroNN(spectrum_pk=spectrum_pk, source_pk=source_pk)
        r.flag_uncertain_logg = True
        r.flag_uncertain_teff = True
        # Missing flag_uncertain_fe_h, so flag_bad should be False
        assert not r.flag_bad

        r.flag_uncertain_fe_h = True
        assert r.flag_bad


class TestAstroNNDistFlags:

    def test_flag_bad(self):
        from astra.models.astronn_dist import AstroNNdist
        Source, Spectrum = _setup_tables(AstroNNdist)
        source_pk, spectrum_pk = _make_spectrum(Source, Spectrum)

        r = AstroNNdist(spectrum_pk=spectrum_pk, source_pk=source_pk)
        assert not r.flag_bad

        r.flag_fakemag_unreliable = True
        assert r.flag_bad

    def test_flag_warn_is_missing_extinction(self):
        from astra.models.astronn_dist import AstroNNdist
        Source, Spectrum = _setup_tables(AstroNNdist)
        source_pk, spectrum_pk = _make_spectrum(Source, Spectrum)

        r = AstroNNdist(spectrum_pk=spectrum_pk, source_pk=source_pk)
        assert not r.flag_warn

        r.flag_missing_extinction = True
        assert r.flag_warn


class TestBossNetFlagProperties:

    def test_flag_warn_includes_suspicious_and_unreliable_fe_h(self):
        from astra.models.bossnet import BossNet
        Source, Spectrum = _setup_tables(BossNet)
        source_pk, spectrum_pk = _make_spectrum(Source, Spectrum)

        r = BossNet(spectrum_pk=spectrum_pk, source_pk=source_pk)
        assert not r.flag_warn

        r.flag_suspicious_fe_h = True
        assert r.flag_warn

        r2 = BossNet(spectrum_pk=spectrum_pk, source_pk=source_pk)
        r2.flag_unreliable_fe_h = True
        assert r2.flag_warn

    def test_flag_bad_includes_all_unreliable_and_runtime(self):
        from astra.models.bossnet import BossNet
        Source, Spectrum = _setup_tables(BossNet)
        source_pk, spectrum_pk = _make_spectrum(Source, Spectrum)

        for flag_name in ("flag_unreliable_teff", "flag_unreliable_logg",
                          "flag_unreliable_fe_h", "flag_runtime_exception"):
            r = BossNet(spectrum_pk=spectrum_pk, source_pk=source_pk)
            setattr(r, flag_name, True)
            assert r.flag_bad, f"flag_bad should be True when {flag_name} is set"
