import os
os.environ["ASTRA_DATABASE_PATH"] = ":memory:"

import importlib.util
import numpy as np
import pytest


def _load_module_from_file(name, path):
    """Load a Python module directly from a file path, bypassing package __init__.py files."""
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# Determine the source root relative to this test file
_SRC = os.path.join(os.path.dirname(os.path.dirname(__file__)), "src")


# --- ferre/utils helpers (importable via normal path) ---

class TestFerreUtilsHelpers:

    def test_get_ferre_spectrum_name(self):
        from astra.pipelines.ferre.utils import get_ferre_spectrum_name
        assert get_ferre_spectrum_name(1, 2, 3) == "1_2_3"
        assert get_ferre_spectrum_name("a", "b") == "a_b"
        assert get_ferre_spectrum_name(0, 100, 200, 1, 50) == "0_100_200_1_50"

    def test_parse_ferre_spectrum_name(self):
        from astra.pipelines.ferre.utils import parse_ferre_spectrum_name
        result = parse_ferre_spectrum_name("0_100_200_1_50")
        assert result == dict(
            index=0,
            source_pk=100,
            spectrum_pk=200,
            initial_flags=1,
            upstream_pk=50,
        )

    def test_parse_ferre_spectrum_name_roundtrip(self):
        from astra.pipelines.ferre.utils import (
            get_ferre_spectrum_name,
            parse_ferre_spectrum_name,
        )
        name = get_ferre_spectrum_name(5, 10, 20, 0, 99)
        parsed = parse_ferre_spectrum_name(name)
        assert parsed["index"] == 5
        assert parsed["source_pk"] == 10
        assert parsed["spectrum_pk"] == 20
        assert parsed["initial_flags"] == 0
        assert parsed["upstream_pk"] == 99

    def test_int_or_none(self):
        from astra.pipelines.ferre.utils import int_or_none
        assert int_or_none("42") == 42
        assert int_or_none("0") == 0
        assert int_or_none("abc") is None
        assert int_or_none("") is None

    def test_get_ferre_label_name(self):
        from astra.pipelines.ferre.utils import get_ferre_label_name, TRANSLATE_LABELS
        ferre_labels = list(TRANSLATE_LABELS.values())
        # Known translations
        assert get_ferre_label_name("teff", ferre_labels) == "TEFF"
        assert get_ferre_label_name("logg", ferre_labels) == "LOGG"
        assert get_ferre_label_name("m_h", ferre_labels) == "METALS"
        # Pass-through when name is already a FERRE label
        assert get_ferre_label_name("TEFF", ferre_labels) == "TEFF"
        # Unknown name should raise
        with pytest.raises(ValueError):
            get_ferre_label_name("nonexistent_param", ferre_labels)

    def test_get_apogee_segment_indices(self):
        from astra.pipelines.ferre.utils import get_apogee_segment_indices
        start_indices, segment_pixels = get_apogee_segment_indices()
        assert len(start_indices) == 3
        assert len(segment_pixels) == 3
        assert sum(segment_pixels) == 3028 + 2495 + 1991  # = 7514

    def test_get_apogee_pixel_mask(self):
        from astra.pipelines.ferre.utils import get_apogee_pixel_mask
        mask = get_apogee_pixel_mask()
        assert mask.shape == (8575,)
        assert mask.dtype == bool
        assert mask.sum() == 7514


# --- aspcap/utils helpers (loaded directly to avoid astra.models chain) ---

class TestAspcapUtilsHelpers:

    @pytest.fixture(autouse=True)
    def load_module(self):
        self.mod = _load_module_from_file(
            "aspcap_utils",
            os.path.join(_SRC, "astra", "pipelines", "aspcap", "utils.py"),
        )

    def test_sanitise_parent_dir(self):
        assert self.mod.sanitise_parent_dir("/path/to/dir") == "/path/to/dir/"
        assert self.mod.sanitise_parent_dir("/path/to/dir/") == "/path/to/dir/"
        assert self.mod.sanitise_parent_dir("/path/to/dir///") == "/path/to/dir/"

    def test_get_lsf_grid_name(self):
        f = self.mod.get_lsf_grid_name
        assert f(1) == "d"
        assert f(50) == "d"
        assert f(51) == "c"
        assert f(145) == "c"
        assert f(146) == "b"
        assert f(245) == "b"
        assert f(246) == "a"
        assert f(300) == "a"

    def test_get_lsf_grid_name_out_of_range(self):
        f = self.mod.get_lsf_grid_name
        # Out of range returns None
        assert f(0) is None
        assert f(301) is None

    def test_approximate_log10_microturbulence(self):
        f = self.mod.approximate_log10_microturbulence
        # At logg=0: result = 0.372160 (only first coeff contributes, last term is always 0)
        result_0 = f(0)
        np.testing.assert_allclose(result_0, 0.372160, atol=1e-6)
        # Should return a numeric value for typical logg values
        result = f(2.5)
        assert np.isfinite(result)

    def test_get_species_label_references(self):
        refs = self.mod.get_species_label_references()
        # Should be a dict with element names as keys
        assert isinstance(refs, dict)
        assert "Fe" in refs
        assert "Ca" in refs
        # Each value is (label, is_x_m)
        for species, (label, is_x_m) in refs.items():
            assert isinstance(label, str)
            assert isinstance(is_x_m, bool)
        # CN should not be in the output (it's explicitly skipped)
        assert "CN" not in refs


# --- corv/spectral_resampling (loaded directly) ---

class TestSpectralResampling:

    @pytest.fixture(autouse=True)
    def load_module(self):
        self.mod = _load_module_from_file(
            "spectral_resampling",
            os.path.join(_SRC, "astra", "pipelines", "corv", "spectral_resampling.py"),
        )

    def test_make_bins_uniform(self):
        wavs = np.array([1.0, 2.0, 3.0, 4.0])
        edges, widths = self.mod.make_bins(wavs)
        assert len(edges) == 5
        assert len(widths) == 4
        # First edge: wavs[0] - (wavs[1] - wavs[0])/2 = 0.5
        assert edges[0] == 0.5
        # Last edge: wavs[-1] + (wavs[-1] - wavs[-2])/2 = 4.5
        assert edges[-1] == 4.5
        # Interior edges are midpoints
        np.testing.assert_array_equal(edges[1:-1], [1.5, 2.5, 3.5])
        # Widths should all be 1.0 for uniform spacing
        np.testing.assert_array_equal(widths, [1.0, 1.0, 1.0, 1.0])

    def test_make_bins_nonuniform(self):
        wavs = np.array([1.0, 3.0, 6.0])
        edges, widths = self.mod.make_bins(wavs)
        assert edges[0] == 0.0  # 1.0 - (3.0 - 1.0)/2
        assert edges[-1] == 7.5  # 6.0 + (6.0 - 3.0)/2
        np.testing.assert_allclose(edges[1], 2.0)
        np.testing.assert_allclose(edges[2], 4.5)

    def test_spectres_identity_resample(self):
        """Resampling onto the same grid should return the same fluxes."""
        wavs = np.linspace(4000, 7000, 100)
        flux = np.sin(wavs / 500.0) + 2.0
        result = self.mod.spectres(wavs, wavs, flux, verbose=False)
        np.testing.assert_allclose(result, flux, atol=1e-10)


# --- the_payne/utils (loaded directly) ---

class TestThePayneUtilsHelpers:

    @pytest.fixture(autouse=True)
    def load_module(self):
        self.mod = _load_module_from_file(
            "the_payne_utils",
            os.path.join(_SRC, "astra", "pipelines", "the_payne", "utils.py"),
        )

    def test_overlap_true(self):
        a = np.array([3.0, 5.0, 7.0])
        b = np.array([4.0, 8.0])
        assert bool(self.mod.overlap(a, b)) is True

    def test_overlap_false(self):
        a = np.array([1.0, 2.0, 3.0])
        b = np.array([5.0, 6.0])
        assert bool(self.mod.overlap(a, b)) is False

    def test_overlap_exact_boundary(self):
        a = np.array([5.0])
        b = np.array([5.0, 10.0])
        assert bool(self.mod.overlap(a, b)) is True

    def test_overlap_no_overlap_single_element(self):
        a = np.array([11.0])
        b = np.array([5.0, 10.0])
        assert bool(self.mod.overlap(a, b)) is False


# --- slam/laspec/wavelength (loaded directly) ---

class TestSlamWavelengthHelpers:

    @pytest.fixture(autouse=True)
    def load_module(self):
        self.mod = _load_module_from_file(
            "slam_wavelength",
            os.path.join(_SRC, "astra", "pipelines", "slam", "laspec", "wavelength.py"),
        )

    def test_vac2air_air2vac_roundtrip(self):
        wl_air = np.array([4000.0, 5500.0, 7000.0])
        wl_vac = self.mod.air2vac(wl_air)
        wl_air_back = self.mod.vac2air(wl_vac)
        np.testing.assert_allclose(wl_air_back, wl_air, atol=0.01)

    def test_air2vac_increases_wavelength(self):
        wl_air = np.array([5000.0])
        wl_vac = self.mod.air2vac(wl_air)
        assert wl_vac[0] > wl_air[0]

    def test_mdwave(self):
        wave = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
        assert self.mod.mdwave(wave) == 1.0
        wave2 = np.array([1.0, 3.0, 6.0, 10.0])
        assert self.mod.mdwave(wave2) == np.median([2.0, 3.0, 4.0])

    def test_wave_log10(self):
        wave = np.arange(3000, 5000, dtype=float)
        result = self.mod.wave_log10(wave)
        # Should start and end at the same values as the input
        np.testing.assert_allclose(result[0], 3000.0, atol=1)
        np.testing.assert_allclose(result[-1], 4999.0, atol=1)
        # Should be uniformly spaced in log10
        log_result = np.log10(result)
        diffs = np.diff(log_result)
        np.testing.assert_allclose(diffs, diffs[0], rtol=1e-10)

    def test_wave_log10_oversampled(self):
        wave = np.arange(4000, 5000, dtype=float)
        result = self.mod.wave_log10(wave, osr_ext=2)
        # Oversampled result should have roughly 2x the pixels
        assert len(result) > len(wave)


# --- slam/laspec/gauss (loaded directly) ---

class TestSlamGaussHelpers:

    @pytest.fixture(autouse=True)
    def load_module(self):
        self.mod = _load_module_from_file(
            "slam_gauss",
            os.path.join(_SRC, "astra", "pipelines", "slam", "laspec", "gauss.py"),
        )

    def test_model_gauss1_at_center(self):
        x = np.array([0.0])
        # Gaussian centered at 0 with amp=1, sigma=1: f(0) = 1*exp(0) = 1
        result = self.mod.model_gauss1([1.0, 0.0, 1.0], x)
        np.testing.assert_allclose(result, [1.0])

    def test_model_gauss1_at_sigma(self):
        # At x = sigma, f = amp * exp(-0.5)
        x = np.array([1.0])
        result = self.mod.model_gauss1([1.0, 0.0, 1.0], x)
        np.testing.assert_allclose(result, [np.exp(-0.5)])

    def test_model_gauss1_amplitude(self):
        x = np.array([5.0])
        # At the center (mean=5), value should equal amplitude
        result = self.mod.model_gauss1([3.5, 5.0, 2.0], x)
        np.testing.assert_allclose(result, [3.5])

    def test_res_gauss1_zero_residual(self):
        x = np.linspace(-5, 5, 50)
        p = [2.0, 0.0, 1.5]
        y = self.mod.model_gauss1(p, x)
        # Residual should be zero when data equals model
        residual = self.mod.res_gauss1(p, x, y)
        np.testing.assert_allclose(residual, 0.0, atol=1e-14)

    def test_res_gauss2_zero_residual(self):
        x = np.linspace(-10, 10, 100)
        p = [2.0, -3.0, 1.0, 1.5, 3.0, 2.0]
        y = self.mod.model_gauss1(p[:3], x) + self.mod.model_gauss1(p[3:], x)
        residual = self.mod.res_gauss2(p, x, y)
        np.testing.assert_allclose(residual, 0.0, atol=1e-14)


# --- snow_white/get_line_info_v3 helpers (loaded directly) ---

class TestSnowWhiteHelpers:

    @pytest.fixture(autouse=True)
    def load_module(self):
        self.mod = _load_module_from_file(
            "snow_white_lines",
            os.path.join(_SRC, "astra", "pipelines", "snow_white", "get_line_info_v3.py"),
        )

    def test_ex_d(self):
        # f(x) = a * exp(b * x^(-1/4))
        x = np.array([1.0])
        # When x=1, x^(-1/4)=1, so f = a * exp(b)
        result = self.mod.ex_d(x, 2.0, 0.5)
        np.testing.assert_allclose(result, [2.0 * np.exp(0.5)])

    def test_ex_d_vectorized(self):
        x = np.array([1.0, 16.0])
        # x=16: 16^(-1/4) = 1/2, so f = a * exp(b/2)
        result = self.mod.ex_d(x, 1.0, 2.0)
        expected = np.array([np.exp(2.0), np.exp(1.0)])
        np.testing.assert_allclose(result, expected)

    def test_bb_returns_positive(self):
        # Blackbody function should return positive values
        wavelengths = np.array([4000.0, 5000.0, 6000.0])
        result = self.mod.bb(wavelengths, 10000.0, 1.0)
        assert np.all(result > 0)

    def test_bb_hotter_is_brighter_in_blue(self):
        wl = np.array([4000.0])
        hot = self.mod.bb(wl, 20000, 1.0)
        cool = self.mod.bb(wl, 5000, 1.0)
        assert hot[0] > cool[0]
