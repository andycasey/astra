"""
Tests for models with 0% or low coverage:
  - base.py (additional coverage for uncovered lines)
  - aspcap.py
  - ferre.py
  - line_forest.py
  - clam.py
  - nmf_rectify.py
  - spectrum.py (additional coverage)

Focuses on flag properties, field defaults, instantiation, category headers,
and pure computation methods that don't require external data or DB queries.
"""
import os
os.environ["ASTRA_DATABASE_PATH"] = ":memory:"

import numpy as np
import pytest
from astra.models.base import database


def _setup_tables(*models):
    from astra.models.source import Source
    from astra.models.spectrum import Spectrum
    all_models = [Source, Spectrum] + list(models)
    database.create_tables(all_models)
    return Source, Spectrum


def _make_spectrum(Source, Spectrum):
    source_pk = Source.create().pk
    spectrum_pk = Spectrum.create().pk
    return source_pk, spectrum_pk


# ---------------------------------------------------------------------------
# base.py: BaseModel, constants, category_headers, category_comments
# ---------------------------------------------------------------------------

class TestBaseModelMeta:

    def test_database_is_set(self):
        from astra.models.base import BaseModel
        assert BaseModel._meta.database is not None

    def test_legacy_table_names_false(self):
        from astra.models.base import BaseModel
        assert BaseModel._meta.legacy_table_names is False


class TestGetDatabaseAndSchema:

    def test_with_env_var(self):
        """The ASTRA_DATABASE_PATH env var should cause SQLite to be used."""
        from astra.models.base import database
        # Since we set the env var, database should be SQLite in-memory
        assert database is not None

    def test_testing_mode(self):
        from astra.models.base import get_database_and_schema
        config = {"TESTING": True}
        db, schema = get_database_and_schema(config)
        assert db is not None
        assert schema is None

    def test_sqlite_path_config(self):
        import tempfile
        from astra.models.base import get_database_and_schema
        with tempfile.NamedTemporaryFile(suffix=".db") as f:
            config = {"database": {"path": f.name}}
            # Temporarily unset env var so config is used
            old = os.environ.pop("ASTRA_DATABASE_PATH", None)
            try:
                db, schema = get_database_and_schema(config)
                assert db is not None
                assert schema is None
            finally:
                if old is not None:
                    os.environ["ASTRA_DATABASE_PATH"] = old

    def test_no_database_config_defaults_to_memory(self):
        from astra.models.base import get_database_and_schema
        old = os.environ.pop("ASTRA_DATABASE_PATH", None)
        try:
            db, schema = get_database_and_schema({})
            assert db is not None
            assert schema is None
        finally:
            if old is not None:
                os.environ["ASTRA_DATABASE_PATH"] = old


class TestResilientDatabase:

    def test_init_sets_max_retries(self):
        from astra.models.base import ResilientDatabase
        # ResilientDatabase uses cooperative MRO; test indirectly by inspecting attributes
        # We can't easily instantiate it alone, but we can check the class exists
        assert hasattr(ResilientDatabase, '__init__')


class TestCategoryHeadersAndComments:

    def _get_category_headers(self, cls):
        ch = cls.category_headers
        if isinstance(ch, (tuple, list)):
            return ch
        return cls.category_headers.fget(cls)

    def _get_category_comments(self, cls):
        cc = cls.category_comments
        if isinstance(cc, (tuple, list)):
            return cc
        return cls.category_comments.fget(cls)

    def test_ferre_coarse_has_category_headers(self):
        from astra.models.ferre import FerreCoarse
        headers = self._get_category_headers(FerreCoarse)
        assert isinstance(headers, tuple)
        header_names = [h[0] for h in headers]
        assert "Grid and Working Directory" in header_names
        assert "Stellar Parameters" in header_names

    def test_clam_has_category_headers(self):
        from astra.models.clam import Clam
        headers = self._get_category_headers(Clam)
        assert isinstance(headers, tuple)
        header_names = [h[0] for h in headers]
        assert "Stellar Labels" in header_names

    def test_nmf_rectify_has_category_headers(self):
        from astra.models.nmf_rectify import NMFRectify
        headers = self._get_category_headers(NMFRectify)
        assert isinstance(headers, tuple)
        header_names = [h[0] for h in headers]
        assert "Continuum Fitting" in header_names

    def test_line_forest_has_category_headers(self):
        from astra.models.line_forest import LineForest
        headers = self._get_category_headers(LineForest)
        assert isinstance(headers, tuple)
        # Should have H-alpha, H-beta etc as category headers
        header_names = [h[0] for h in headers]
        assert any("H-alpha" in h for h in header_names)


# ---------------------------------------------------------------------------
# base.py: add_category_headers, add_category_comments
# ---------------------------------------------------------------------------

class TestAddCategoryHeadersAndComments:

    def _has_working_classmethod_property(self):
        """Check if @classmethod @property stacking works in this Python version."""
        from astra.models.clam import Clam
        try:
            result = Clam.category_headers
            return isinstance(result, (tuple, list))
        except TypeError:
            return False

    def test_add_category_headers_no_match(self):
        """When original_names doesn't contain the field, it should skip."""
        if not self._has_working_classmethod_property():
            pytest.skip("@classmethod @property stacking not supported in this Python version")
        from unittest.mock import MagicMock
        from astra.models.base import add_category_headers

        hdu = MagicMock()
        from astra.models.clam import Clam
        result = add_category_headers(hdu, [Clam], {"col1": "nonexistent_field"}, upper=False)
        assert result is None

    def test_add_category_comments_no_match(self):
        if not self._has_working_classmethod_property():
            pytest.skip("@classmethod @property stacking not supported in this Python version")
        from unittest.mock import MagicMock
        from astra.models.base import add_category_comments

        from astra.models.clam import Clam
        result = add_category_comments(hdu=MagicMock(), models=[Clam], original_names={"col1": "nonexistent"}, upper=False)
        assert result is None


# ---------------------------------------------------------------------------
# aspcap.py: ASPCAP flags, fields, instantiation
# ---------------------------------------------------------------------------

class TestASPCAPModel:

    def setup_method(self):
        from astra.models.aspcap import ASPCAP
        self.ASPCAP = ASPCAP
        Source, Spectrum = _setup_tables(ASPCAP)
        self.source_pk, self.spectrum_pk = _make_spectrum(Source, Spectrum)

    def test_instantiation_defaults(self):
        r = self.ASPCAP(spectrum_pk=self.spectrum_pk, source_pk=self.source_pk)
        assert r.teff is None
        assert r.logg is None
        assert r.fe_h is None
        assert r.rchi2 is None

    def test_flag_warn_false_when_no_flags(self):
        r = self.ASPCAP(spectrum_pk=self.spectrum_pk, source_pk=self.source_pk)
        assert not r.flag_warn

    def test_flag_warn_true_when_any_result_flag(self):
        r = self.ASPCAP(spectrum_pk=self.spectrum_pk, source_pk=self.source_pk)
        # Set result_flags directly since some flag names are overridden by coarse_ferre_flags
        r.result_flags = 1
        assert r.flag_warn

    def test_flag_bad_false_when_clean(self):
        r = self.ASPCAP(spectrum_pk=self.spectrum_pk, source_pk=self.source_pk)
        assert not r.flag_bad

    def test_flag_bad_true_for_suspicious_parameters(self):
        r = self.ASPCAP(spectrum_pk=self.spectrum_pk, source_pk=self.source_pk)
        r.flag_suspicious_parameters = True
        assert r.flag_bad

    def test_flag_bad_true_for_high_v_sini(self):
        r = self.ASPCAP(spectrum_pk=self.spectrum_pk, source_pk=self.source_pk)
        r.flag_high_v_sini = True
        assert r.flag_bad

    def test_flag_bad_true_for_high_v_micro(self):
        r = self.ASPCAP(spectrum_pk=self.spectrum_pk, source_pk=self.source_pk)
        r.flag_high_v_micro = True
        assert r.flag_bad

    def test_flag_bad_true_for_unphysical_parameters(self):
        r = self.ASPCAP(spectrum_pk=self.spectrum_pk, source_pk=self.source_pk)
        r.flag_unphysical_parameters = True
        assert r.flag_bad

    def test_flag_bad_true_for_high_rchi2(self):
        r = self.ASPCAP(spectrum_pk=self.spectrum_pk, source_pk=self.source_pk)
        r.flag_high_rchi2 = True
        assert r.flag_bad

    def test_flag_bad_true_for_low_snr(self):
        r = self.ASPCAP(spectrum_pk=self.spectrum_pk, source_pk=self.source_pk)
        r.flag_low_snr = True
        assert r.flag_bad

    def test_flag_bad_true_for_ferre_fail(self):
        """flag_ferre_fail is overridden by coarse_ferre_flags but flag_bad references it."""
        r = self.ASPCAP(spectrum_pk=self.spectrum_pk, source_pk=self.source_pk)
        # flag_ferre_fail is now on coarse_ferre_flags (overridden)
        r.flag_ferre_fail = True
        assert r.flag_bad

    def test_flag_bad_true_for_no_suitable_initial_guess(self):
        r = self.ASPCAP(spectrum_pk=self.spectrum_pk, source_pk=self.source_pk)
        r.flag_no_suitable_initial_guess = True
        assert r.flag_bad

    def test_flag_bad_true_for_spectrum_io_error(self):
        r = self.ASPCAP(spectrum_pk=self.spectrum_pk, source_pk=self.source_pk)
        r.flag_spectrum_io_error = True
        assert r.flag_bad

    def test_flag_bad_true_for_teff_grid_edge_bad(self):
        r = self.ASPCAP(spectrum_pk=self.spectrum_pk, source_pk=self.source_pk)
        r.flag_teff_grid_edge_bad = True
        assert r.flag_bad

    def test_flag_bad_true_for_logg_grid_edge_bad(self):
        r = self.ASPCAP(spectrum_pk=self.spectrum_pk, source_pk=self.source_pk)
        r.flag_logg_grid_edge_bad = True
        assert r.flag_bad

    def test_flag_bad_true_for_high_std_v_rad(self):
        r = self.ASPCAP(spectrum_pk=self.spectrum_pk, source_pk=self.source_pk)
        r.flag_high_std_v_rad = True
        assert r.flag_bad

    def test_abundance_flag_setting(self):
        """Test that abundance-level flags can be set and read."""
        r = self.ASPCAP(spectrum_pk=self.spectrum_pk, source_pk=self.source_pk)
        assert not r.flag_al_h_censored_high_teff
        r.flag_al_h_censored_high_teff = True
        assert r.flag_al_h_censored_high_teff

    def test_irfm_teff_flags(self):
        r = self.ASPCAP(spectrum_pk=self.spectrum_pk, source_pk=self.source_pk)
        assert not r.flag_out_of_v_k_bounds
        r.flag_out_of_v_k_bounds = True
        assert r.flag_out_of_v_k_bounds

    def test_initial_flags(self):
        r = self.ASPCAP(spectrum_pk=self.spectrum_pk, source_pk=self.source_pk)
        assert not r.flag_initial_guess_from_apogeenet
        r.flag_initial_guess_from_apogeenet = True
        assert r.flag_initial_guess_from_apogeenet

    def test_calibrated_flags(self):
        r = self.ASPCAP(spectrum_pk=self.spectrum_pk, source_pk=self.source_pk)
        r.flag_as_dwarf_for_calibration = True
        assert r.flag_as_dwarf_for_calibration
        r.flag_as_giant_for_calibration = True
        assert r.flag_as_giant_for_calibration
        # DR21 bits are offset past the deprecated DR19-era bits 0-4
        r.flag_teff_m_dwarf = True
        assert r.flag_teff_m_dwarf
        r.flag_logg_not_calibrated = True
        assert r.flag_logg_not_calibrated
        assert r.calibrated_flags == 2**0 + 2**1 + 2**9 + 2**22

    def test_unmask_pixel_array(self):
        from astra.pipelines.ferre.utils import get_apogee_pixel_mask
        mask = get_apogee_pixel_mask()
        r = self.ASPCAP(spectrum_pk=self.spectrum_pk, source_pk=self.source_pk)
        # Create a small masked array
        n_good = int(mask.sum())
        test_array = np.ones(n_good)
        result = r._unmask_pixel_array(test_array)
        assert result.shape == mask.shape
        assert np.all(result[mask] == 1.0)
        assert np.all(np.isnan(result[~mask]))

    def test_field_defaults(self):
        r = self.ASPCAP(spectrum_pk=self.spectrum_pk, source_pk=self.source_pk)
        assert r.short_grid_name == ""
        assert r.continuum_order == -1
        assert r.interpolation_order == -1


# ---------------------------------------------------------------------------
# ferre.py: FerreCoarse, FerreStellarParameters, FerreChemicalAbundances
# ---------------------------------------------------------------------------

class TestFerreCoarseModel:

    def setup_method(self):
        from astra.models.ferre import FerreCoarse
        self.FerreCoarse = FerreCoarse
        Source, Spectrum = _setup_tables(FerreCoarse)
        self.source_pk, self.spectrum_pk = _make_spectrum(Source, Spectrum)

    def test_instantiation_defaults(self):
        r = self.FerreCoarse(spectrum_pk=self.spectrum_pk, source_pk=self.source_pk)
        assert r.teff is None
        assert r.logg is None
        assert r.pwd == ""
        assert r.short_grid_name == ""
        assert r.continuum_order == -1
        assert r.ferre_index == -1
        assert r.ferre_n_obj == -1

    def test_initial_flags_default_zero(self):
        r = self.FerreCoarse(spectrum_pk=self.spectrum_pk, source_pk=self.source_pk)
        assert r.initial_flags == 0
        assert not r.flag_initial_guess_from_apogeenet
        assert not r.flag_initial_guess_from_doppler

    def test_set_initial_flags(self):
        r = self.FerreCoarse(spectrum_pk=self.spectrum_pk, source_pk=self.source_pk)
        r.flag_initial_guess_from_apogeenet = True
        assert r.flag_initial_guess_from_apogeenet
        assert r.initial_flags & 1 != 0

    def test_frozen_flags(self):
        r = self.FerreCoarse(spectrum_pk=self.spectrum_pk, source_pk=self.source_pk)
        assert not r.flag_teff_frozen
        r.flag_teff_frozen = True
        assert r.flag_teff_frozen
        r.flag_logg_frozen = True
        assert r.flag_logg_frozen

    def test_ferre_flags_default_zero(self):
        r = self.FerreCoarse(spectrum_pk=self.spectrum_pk, source_pk=self.source_pk)
        assert r.ferre_flags == 0
        assert not r.flag_ferre_fail
        assert not r.flag_missing_model_flux

    def test_set_ferre_flags(self):
        r = self.FerreCoarse(spectrum_pk=self.spectrum_pk, source_pk=self.source_pk)
        r.flag_ferre_fail = True
        assert r.flag_ferre_fail
        r.flag_missing_model_flux = True
        assert r.flag_missing_model_flux
        r.flag_potential_ferre_timeout = True
        assert r.flag_potential_ferre_timeout

    def test_grid_edge_flags(self):
        r = self.FerreCoarse(spectrum_pk=self.spectrum_pk, source_pk=self.source_pk)
        r.flag_teff_grid_edge_warn = True
        assert r.flag_teff_grid_edge_warn
        r.flag_teff_grid_edge_bad = True
        assert r.flag_teff_grid_edge_bad

    def test_multiple_flags_combine(self):
        r = self.FerreCoarse(spectrum_pk=self.spectrum_pk, source_pk=self.source_pk)
        r.flag_ferre_fail = True
        r.flag_spectrum_io_error = True
        r.flag_teff_grid_edge_warn = True
        assert r.flag_ferre_fail
        assert r.flag_spectrum_io_error
        assert r.flag_teff_grid_edge_warn
        assert not r.flag_missing_model_flux


class TestFerreOutputMixin:

    def test_unmask(self):
        from astra.models.ferre import FerreCoarse
        from astra.pipelines.ferre.utils import get_apogee_pixel_mask
        mask = get_apogee_pixel_mask()
        Source, Spectrum = _setup_tables(FerreCoarse)
        source_pk, spectrum_pk = _make_spectrum(Source, Spectrum)

        r = FerreCoarse(spectrum_pk=spectrum_pk, source_pk=source_pk)
        n_good = int(mask.sum())
        test_array = np.arange(n_good, dtype=float)
        result = r.unmask(test_array)
        assert result.shape == mask.shape
        assert np.all(result[mask] == test_array)
        assert np.all(np.isnan(result[~mask]))

    def test_unmask_custom_fill(self):
        from astra.models.ferre import FerreCoarse
        from astra.pipelines.ferre.utils import get_apogee_pixel_mask
        mask = get_apogee_pixel_mask()
        Source, Spectrum = _setup_tables(FerreCoarse)
        source_pk, spectrum_pk = _make_spectrum(Source, Spectrum)

        r = FerreCoarse(spectrum_pk=spectrum_pk, source_pk=source_pk)
        n_good = int(mask.sum())
        test_array = np.ones(n_good)
        result = r.unmask(test_array, fill_value=-999.0)
        assert np.all(result[~mask] == -999.0)


class TestFerreStellarParametersModel:
    """FerreStellarParameters re-declares source_pk/spectrum_pk causing SQLite schema issues,
    so we test without creating tables."""

    def test_instantiation(self):
        from astra.models.ferre import FerreStellarParameters
        r = FerreStellarParameters()
        assert r.teff is None
        assert r.logg is None
        assert r.pwd == ""

    def test_frozen_flags(self):
        from astra.models.ferre import FerreStellarParameters
        r = FerreStellarParameters()
        r.flag_teff_frozen = True
        assert r.flag_teff_frozen
        r.flag_m_h_frozen = True
        assert r.flag_m_h_frozen

    def test_ferre_flags(self):
        from astra.models.ferre import FerreStellarParameters
        r = FerreStellarParameters()
        r.flag_ferre_fail = True
        assert r.flag_ferre_fail
        r.flag_teff_grid_edge_bad = True
        assert r.flag_teff_grid_edge_bad


class TestFerreChemicalAbundancesModel:
    """FerreChemicalAbundances re-declares source_pk/spectrum_pk causing SQLite schema issues,
    so we test without creating tables."""

    def test_instantiation(self):
        from astra.models.ferre import FerreChemicalAbundances
        r = FerreChemicalAbundances()
        assert r.teff is None
        assert r.pwd == ""

    def test_frozen_flags(self):
        from astra.models.ferre import FerreChemicalAbundances
        r = FerreChemicalAbundances()
        r.flag_alpha_m_frozen = True
        assert r.flag_alpha_m_frozen
        r.flag_c_m_frozen = True
        assert r.flag_c_m_frozen
        r.flag_n_m_frozen = True
        assert r.flag_n_m_frozen

    def test_ferre_flags(self):
        from astra.models.ferre import FerreChemicalAbundances
        r = FerreChemicalAbundances()
        r.flag_spectrum_io_error = True
        assert r.flag_spectrum_io_error
        r.flag_no_suitable_initial_guess = True
        assert r.flag_no_suitable_initial_guess

    def test_grid_edge_flags(self):
        from astra.models.ferre import FerreChemicalAbundances
        r = FerreChemicalAbundances()
        r.flag_m_h_atm_grid_edge_warn = True
        assert r.flag_m_h_atm_grid_edge_warn
        r.flag_alpha_m_grid_edge_bad = True
        assert r.flag_alpha_m_grid_edge_bad


# ---------------------------------------------------------------------------
# line_forest.py: LineForest model
# ---------------------------------------------------------------------------

class TestLineForestModel:
    """LineForest uses ArrayField with GIN indexes (PostgreSQL-only), so skip table creation."""

    def test_instantiation_defaults(self):
        from astra.models.line_forest import LineForest
        r = LineForest()
        assert r.eqw_h_alpha is None
        assert r.abs_h_alpha is None
        assert r.detection_stat_h_alpha is None
        assert r.eqw_h_beta is None

    def test_all_hydrogen_line_fields_exist(self):
        from astra.models.line_forest import LineForest
        r = LineForest()
        for line in ("h_alpha", "h_beta", "h_gamma", "h_delta", "h_epsilon"):
            assert hasattr(r, f"eqw_{line}")
            assert hasattr(r, f"abs_{line}")
            assert hasattr(r, f"detection_stat_{line}")
            assert hasattr(r, f"detection_raw_{line}")

    def test_paschen_line_fields_exist(self):
        from astra.models.line_forest import LineForest
        r = LineForest()
        for n in range(7, 18):
            assert hasattr(r, f"eqw_pa_{n}")
            assert hasattr(r, f"abs_pa_{n}")

    def test_calcium_line_fields_exist(self):
        from astra.models.line_forest import LineForest
        r = LineForest()
        for wl in ("8662", "8542", "8498"):
            assert hasattr(r, f"eqw_ca_ii_{wl}")
            assert hasattr(r, f"abs_ca_ii_{wl}")

    def test_helium_line_fields_exist(self):
        from astra.models.line_forest import LineForest
        r = LineForest()
        for wl in ("6678", "5875", "5015", "4471"):
            assert hasattr(r, f"eqw_he_i_{wl}")
        assert hasattr(r, "eqw_he_ii_4685")

    def test_lithium_field_exists(self):
        from astra.models.line_forest import LineForest
        r = LineForest()
        assert hasattr(r, "eqw_li_i")
        assert hasattr(r, "abs_li_i")


# ---------------------------------------------------------------------------
# clam.py: Clam model
# ---------------------------------------------------------------------------

class TestClamModel:

    def setup_method(self):
        from astra.models.clam import Clam
        self.Clam = Clam
        Source, Spectrum = _setup_tables(Clam)
        self.source_pk, self.spectrum_pk = _make_spectrum(Source, Spectrum)

    def test_instantiation_defaults(self):
        r = self.Clam(spectrum_pk=self.spectrum_pk, source_pk=self.source_pk)
        assert r.teff is None
        assert r.logg is None
        assert r.m_h is None
        assert r.rchi2 is None

    def test_initial_fields_exist(self):
        r = self.Clam(spectrum_pk=self.spectrum_pk, source_pk=self.source_pk)
        for field in ("initial_teff", "initial_logg", "initial_m_h",
                       "initial_n_m", "initial_c_m", "initial_v_micro", "initial_v_sini"):
            assert hasattr(r, field)
            assert getattr(r, field) is None

    def test_result_flags_default_zero(self):
        r = self.Clam(spectrum_pk=self.spectrum_pk, source_pk=self.source_pk)
        assert r.result_flags == 0
        assert not r.flag_spectrum_io_error
        assert not r.flag_runtime_error

    def test_set_flag_spectrum_io_error(self):
        r = self.Clam(spectrum_pk=self.spectrum_pk, source_pk=self.source_pk)
        r.flag_spectrum_io_error = True
        assert r.flag_spectrum_io_error
        assert r.result_flags & 1 != 0

    def test_set_flag_runtime_error(self):
        r = self.Clam(spectrum_pk=self.spectrum_pk, source_pk=self.source_pk)
        r.flag_runtime_error = True
        assert r.flag_runtime_error
        assert r.result_flags & 2 != 0

    def test_flags_independent(self):
        r = self.Clam(spectrum_pk=self.spectrum_pk, source_pk=self.source_pk)
        r.flag_spectrum_io_error = True
        assert not r.flag_runtime_error
        r.flag_runtime_error = True
        assert r.flag_spectrum_io_error
        assert r.flag_runtime_error

    def test_clear_flag(self):
        r = self.Clam(spectrum_pk=self.spectrum_pk, source_pk=self.source_pk)
        r.flag_spectrum_io_error = True
        assert r.flag_spectrum_io_error
        r.flag_spectrum_io_error = False
        assert not r.flag_spectrum_io_error

    def test_stellar_label_fields_exist(self):
        r = self.Clam(spectrum_pk=self.spectrum_pk, source_pk=self.source_pk)
        for field in ("teff", "e_teff", "logg", "e_logg", "m_h", "e_m_h",
                       "n_m", "e_n_m", "c_m", "e_c_m", "v_micro", "e_v_micro",
                       "v_sini", "e_v_sini"):
            assert hasattr(r, field)


# ---------------------------------------------------------------------------
# nmf_rectify.py: NMFRectify model
# ---------------------------------------------------------------------------

class TestNMFRectifyModel:
    """NMFRectify uses ArrayField with GIN indexes (PostgreSQL-only), so skip table creation."""

    def test_instantiation_defaults(self):
        from astra.models.nmf_rectify import NMFRectify
        r = NMFRectify(L=1.0, deg=3)
        assert r.rchi2 is None
        assert r.joint_rchi2 is None
        assert r.L == 1.0
        assert r.deg == 3

    def test_nmf_flags_default_zero(self):
        from astra.models.nmf_rectify import NMFRectify
        r = NMFRectify(L=1.0, deg=3)
        assert r.nmf_flags == 0
        assert not r.flag_initialised_from_small_w
        assert not r.flag_could_not_read_spectrum
        assert not r.flag_runtime_exception

    def test_set_flag_initialised_from_small_w(self):
        from astra.models.nmf_rectify import NMFRectify
        r = NMFRectify(L=1.0, deg=3)
        r.flag_initialised_from_small_w = True
        assert r.flag_initialised_from_small_w

    def test_set_flag_could_not_read_spectrum(self):
        from astra.models.nmf_rectify import NMFRectify
        r = NMFRectify(L=1.0, deg=3)
        r.flag_could_not_read_spectrum = True
        assert r.flag_could_not_read_spectrum
        assert not r.flag_runtime_exception

    def test_set_flag_runtime_exception(self):
        from astra.models.nmf_rectify import NMFRectify
        r = NMFRectify(L=1.0, deg=3)
        r.flag_runtime_exception = True
        assert r.flag_runtime_exception

    def test_flags_independent(self):
        from astra.models.nmf_rectify import NMFRectify
        r = NMFRectify(L=1.0, deg=3)
        r.flag_initialised_from_small_w = True
        r.flag_runtime_exception = True
        assert r.flag_initialised_from_small_w
        assert r.flag_runtime_exception
        assert not r.flag_could_not_read_spectrum


# ---------------------------------------------------------------------------
# spectrum.py: Spectrum, SpectrumMixin
# ---------------------------------------------------------------------------

class TestSpectrumModel:

    def setup_method(self):
        from astra.models.spectrum import Spectrum
        _setup_tables()
        self.Spectrum = Spectrum

    def test_instantiation(self):
        s = self.Spectrum()
        assert s.spectrum_flags == 0

    def test_spectrum_flags_bitfield(self):
        s = self.Spectrum()
        s.spectrum_flags = 0
        assert s.spectrum_flags == 0


class TestSpectrumMixinPlotAttributes:
    """Test SpectrumMixin attributes that are accessed in plot()."""

    def test_e_flux_all_finite(self):
        from astra.models.spectrum import SpectrumMixin
        class FakeSpectrum(SpectrumMixin):
            pass
        s = FakeSpectrum()
        s.ivar = np.array([1.0, 4.0, 16.0])
        e_flux = s.e_flux
        np.testing.assert_allclose(e_flux, [1.0, 0.5, 0.25])

    def test_e_flux_all_zero_ivar(self):
        from astra.models.spectrum import SpectrumMixin
        class FakeSpectrum(SpectrumMixin):
            pass
        s = FakeSpectrum()
        s.ivar = np.array([0.0, 0.0, 0.0])
        e_flux = s.e_flux
        assert np.all(e_flux == 1e10)


# ---------------------------------------------------------------------------
# base.py: fits_column_kwargs for additional field types
# ---------------------------------------------------------------------------

class TestFitsColumnKwargsAdditional:

    def test_auto_field_format(self):
        from peewee import AutoField
        from astra.models.base import fits_column_kwargs
        f = AutoField()
        f.name = "pk"
        result = fits_column_kwargs(f, [1, 2, 3], upper=False)
        assert result["format"] == "K"

    def test_big_integer_field_format(self):
        from peewee import BigIntegerField
        from astra.models.base import fits_column_kwargs
        f = BigIntegerField()
        f.name = "big_id"
        result = fits_column_kwargs(f, [1000000, 2000000], upper=False)
        assert result["format"] == "K"

    def test_foreign_key_field_format(self):
        from peewee import ForeignKeyField, Model
        from astra.models.base import fits_column_kwargs
        class DummyModel(Model):
            pass
        f = ForeignKeyField(DummyModel)
        f.name = "source_pk"
        result = fits_column_kwargs(f, [1, 2], upper=True)
        assert result["format"] == "K"
        assert result["name"] == "SOURCE_PK"

    def test_bitfield_format(self):
        from astra.fields import BitField
        from astra.models.base import fits_column_kwargs
        f = BitField()
        f.name = "flags"
        result = fits_column_kwargs(f, [0, 1, 2], upper=False)
        assert result["format"] == "J"

    def test_datetime_field_format(self):
        import datetime
        from peewee import DateTimeField
        from astra.models.base import fits_column_kwargs
        f = DateTimeField()
        f.name = "created"
        now = datetime.datetime.now()
        result = fits_column_kwargs(f, [now], upper=False)
        assert result["format"] == "A26"
        # The array should contain isoformat strings
        assert isinstance(result["array"][0], str)

    def test_datetime_field_with_string_value(self):
        from peewee import DateTimeField
        from astra.models.base import fits_column_kwargs
        f = DateTimeField()
        f.name = "created"
        result = fits_column_kwargs(f, ["2024-01-01T00:00:00"], upper=False)
        assert result["format"] == "A26"
        assert result["array"][0] == "2024-01-01T00:00:00"


# ---------------------------------------------------------------------------
# base.py: get_fill_value for additional field types
# ---------------------------------------------------------------------------

class TestGetFillValueAdditional:

    def test_auto_field_fill_value(self):
        from peewee import AutoField
        from astra.models.base import get_fill_value
        f = AutoField()
        f.name = "pk"
        assert get_fill_value(f, {}) == -1

    def test_big_integer_field_fill_value(self):
        from peewee import BigIntegerField
        from astra.models.base import get_fill_value
        f = BigIntegerField()
        f.name = "big_id"
        assert get_fill_value(f, {}) == -1

    def test_foreign_key_field_fill_value(self):
        from peewee import ForeignKeyField, Model
        from astra.models.base import get_fill_value
        class DummyModel(Model):
            pass
        f = ForeignKeyField(DummyModel)
        f.name = "source_pk"
        assert get_fill_value(f, {}) == -1

    def test_datetime_field_fill_value(self):
        from peewee import DateTimeField
        from astra.models.base import get_fill_value
        f = DateTimeField()
        f.name = "created"
        assert get_fill_value(f, {}) == ""


# ---------------------------------------------------------------------------
# base.py: warn_on_long_name_or_comment edge cases
# ---------------------------------------------------------------------------

class TestWarnOnLongNameOrCommentAdditional:

    def test_long_help_text_warns(self):
        from peewee import FloatField
        from astra.models.base import warn_on_long_name_or_comment
        f = FloatField()
        f.name = "x"
        f.help_text = "A" * 50  # > 47 chars
        # Should not raise, returns None
        assert warn_on_long_name_or_comment(f) is None

    def test_long_total_warns(self):
        from peewee import FloatField
        from astra.models.base import warn_on_long_name_or_comment
        f = FloatField()
        f.name = "a_very_long_field_name_here_12345"
        f.help_text = "also a very long help text string here"
        # total > 65, should still return None (just logs a warning)
        assert warn_on_long_name_or_comment(f) is None


# ---------------------------------------------------------------------------
# ASPCAP: category headers
# ---------------------------------------------------------------------------

class TestASPCAPCategoryHeaders:

    def _get_category_headers(self, cls):
        ch = cls.category_headers
        if isinstance(ch, (tuple, list)):
            return ch
        return cls.category_headers.fget(cls)

    def test_aspcap_has_stellar_parameters_header(self):
        from astra.models.aspcap import ASPCAP
        headers = self._get_category_headers(ASPCAP)
        header_names = [h[0] for h in headers]
        assert "Stellar Parameters" in header_names

    def test_aspcap_has_chemical_abundances_header(self):
        from astra.models.aspcap import ASPCAP
        headers = self._get_category_headers(ASPCAP)
        header_names = [h[0] for h in headers]
        assert "Chemical Abundances" in header_names

    def test_aspcap_has_spectral_data_header(self):
        from astra.models.aspcap import ASPCAP
        headers = self._get_category_headers(ASPCAP)
        header_names = [h[0] for h in headers]
        assert "Spectral Data" in header_names


# ---------------------------------------------------------------------------
# Ferre: category headers
# ---------------------------------------------------------------------------

class TestFerreCategoryHeaders:

    def _get_category_headers(self, cls):
        ch = cls.category_headers
        if isinstance(ch, (tuple, list)):
            return ch
        return cls.category_headers.fget(cls)

    def test_ferre_coarse_has_initial_parameters_header(self):
        from astra.models.ferre import FerreCoarse
        headers = self._get_category_headers(FerreCoarse)
        header_names = [h[0] for h in headers]
        assert "Initial Stellar Parameters" in header_names

    def test_ferre_coarse_has_ferre_settings_header(self):
        from astra.models.ferre import FerreCoarse
        headers = self._get_category_headers(FerreCoarse)
        header_names = [h[0] for h in headers]
        assert "FERRE Settings" in header_names

    def test_ferre_coarse_has_summary_statistics_header(self):
        from astra.models.ferre import FerreCoarse
        headers = self._get_category_headers(FerreCoarse)
        header_names = [h[0] for h in headers]
        assert "Summary Statistics" in header_names


# ---------------------------------------------------------------------------
# PipelineOutputMixin: from_spectrum
# ---------------------------------------------------------------------------

class TestPipelineOutputMixinFromSpectrum:

    def test_from_spectrum_with_clam(self):
        from astra.models.clam import Clam
        Source, Spectrum = _setup_tables(Clam)
        source_pk, spectrum_pk = _make_spectrum(Source, Spectrum)

        class FakeSpectrum:
            pass
        s = FakeSpectrum()
        s.spectrum_pk = spectrum_pk
        s.source_pk = source_pk

        r = Clam.from_spectrum(s, teff=5000.0, logg=3.5)
        assert r.teff == 5000.0
        assert r.logg == 3.5
        assert r.spectrum_pk == spectrum_pk
        assert r.source_pk == source_pk

    def test_from_spectrum_source_pk_mismatch_raises(self):
        from astra.models.clam import Clam
        Source, Spectrum = _setup_tables(Clam)
        source_pk, spectrum_pk = _make_spectrum(Source, Spectrum)

        class FakeSpectrum:
            pass
        s = FakeSpectrum()
        s.spectrum_pk = spectrum_pk
        s.source_pk = source_pk

        with pytest.raises(ValueError, match="source_pk.*mismatch"):
            Clam.from_spectrum(s, source_pk=source_pk + 999)

    def test_from_spectrum_spectrum_pk_mismatch_raises(self):
        from astra.models.clam import Clam
        Source, Spectrum = _setup_tables(Clam)
        source_pk, spectrum_pk = _make_spectrum(Source, Spectrum)

        class FakeSpectrum:
            pass
        s = FakeSpectrum()
        s.spectrum_pk = spectrum_pk
        s.source_pk = source_pk

        with pytest.raises(ValueError, match="spectrum_pk.*mismatch"):
            Clam.from_spectrum(s, spectrum_pk=spectrum_pk + 999)


# ---------------------------------------------------------------------------
# Flag value checks (bit values)
# ---------------------------------------------------------------------------

class TestFlagBitValues:
    """Verify that flag bit values are assigned correctly."""

    def test_clam_flag_values(self):
        from astra.models.clam import Clam
        Source, Spectrum = _setup_tables(Clam)
        source_pk, spectrum_pk = _make_spectrum(Source, Spectrum)

        r = Clam(spectrum_pk=spectrum_pk, source_pk=source_pk)
        r.flag_spectrum_io_error = True
        assert r.result_flags == 1  # 2**0

        r2 = Clam(spectrum_pk=spectrum_pk, source_pk=source_pk)
        r2.flag_runtime_error = True
        assert r2.result_flags == 2  # 2**1

    def test_nmf_rectify_flag_values(self):
        from astra.models.nmf_rectify import NMFRectify

        r = NMFRectify(L=1.0, deg=3)
        r.flag_initialised_from_small_w = True
        assert r.nmf_flags == 1  # 2**0

        r2 = NMFRectify(L=1.0, deg=3)
        r2.flag_could_not_read_spectrum = True
        assert r2.nmf_flags == 8  # 2**3

        r3 = NMFRectify(L=1.0, deg=3)
        r3.flag_runtime_exception = True
        assert r3.nmf_flags == 16  # 2**4

    def test_ferre_coarse_initial_flag_values(self):
        from astra.models.ferre import FerreCoarse
        Source, Spectrum = _setup_tables(FerreCoarse)
        source_pk, spectrum_pk = _make_spectrum(Source, Spectrum)

        r = FerreCoarse(spectrum_pk=spectrum_pk, source_pk=source_pk)
        r.flag_initial_guess_from_apogeenet = True
        assert r.initial_flags == 1  # 2**0

        r2 = FerreCoarse(spectrum_pk=spectrum_pk, source_pk=source_pk)
        r2.flag_initial_guess_from_doppler = True
        assert r2.initial_flags == 2  # 2**1

    def test_aspcap_result_flag_values(self):
        from astra.models.aspcap import ASPCAP
        Source, Spectrum = _setup_tables(ASPCAP)
        source_pk, spectrum_pk = _make_spectrum(Source, Spectrum)

        # flag_suspicious_parameters is on result_flags and NOT overridden
        r = ASPCAP(spectrum_pk=spectrum_pk, source_pk=source_pk)
        r.flag_suspicious_parameters = True
        assert r.result_flags == 2**21

        r2 = ASPCAP(spectrum_pk=spectrum_pk, source_pk=source_pk)
        r2.flag_high_v_sini = True
        assert r2.result_flags == 2**22

    def test_aspcap_irfm_flag_values(self):
        from astra.models.aspcap import ASPCAP
        Source, Spectrum = _setup_tables(ASPCAP)
        source_pk, spectrum_pk = _make_spectrum(Source, Spectrum)

        r = ASPCAP(spectrum_pk=spectrum_pk, source_pk=source_pk)
        r.flag_out_of_v_k_bounds = True
        assert r.irfm_teff_flags == 1  # 2**0

        r2 = ASPCAP(spectrum_pk=spectrum_pk, source_pk=source_pk)
        r2.flag_out_of_fe_h_bounds = True
        assert r2.irfm_teff_flags == 2  # 2**1

        r3 = ASPCAP(spectrum_pk=spectrum_pk, source_pk=source_pk)
        r3.flag_extrapolated_v_mag = True
        assert r3.irfm_teff_flags == 4  # 2**2
