"""Tests for astra.pipelines.snow_white.pca_model."""

import os
os.environ["ASTRA_DATABASE_PATH"] = ":memory:"

import pickle
from types import SimpleNamespace

import numpy as np
import pytest
from scipy.interpolate import RectBivariateSpline

from astra.pipelines.snow_white.pca_model import export_pca_model, load_pca_model, reconstruct_spectrum


def _make_pickled_model(path, n_components=4, n_pixels=50, seed=0):
    """Write a small model with the same layout as the pickled snow_white PCA model."""
    rng = np.random.default_rng(seed)
    teff_grid = np.linspace(4000, 130000, 12)
    logg_grid = np.linspace(600, 950, 8)
    pca = SimpleNamespace(
        mean_=rng.normal(10, 1, n_pixels),
        components_=rng.normal(0, 0.1, (n_components, n_pixels)),
        n_components_=n_components,
        whiten=False,
    )
    splines = [
        RectBivariateSpline(teff_grid, logg_grid, rng.normal(0, 1, (teff_grid.size, logg_grid.size)))
        for _ in range(n_components)
    ]
    original = dict(
        pca=pca,
        interp_coeffs=splines,
        teff_grid=teff_grid,
        logg_grid=logg_grid,
        wavelength=np.linspace(3500, 10800, n_pixels),
        eps=1e-12,
    )
    with open(path, "wb") as fp:
        pickle.dump(original, fp)
    return original


def _reference_spectrum(teff, logg, original):
    """The original pickle-based reconstruction."""
    pca = original["pca"]
    c = np.array([spline(teff, logg)[0, 0] for spline in original["interp_coeffs"]])
    return np.exp(pca.mean_ + np.dot(c, pca.components_))


class TestPcaModel:

    def test_export_reproduces_pickled_model(self, tmp_path):
        original = _make_pickled_model(tmp_path / "model.pkl")
        max_diff = export_pca_model(tmp_path / "model.pkl", tmp_path / "model.npz", n_check=200)
        assert max_diff == 0

        model = load_pca_model(tmp_path / "model.npz")
        for teff, logg in [(4000, 600), (12000, 800), (40000, 850), (130000, 950)]:
            np.testing.assert_array_equal(
                reconstruct_spectrum(teff, logg, model),
                _reference_spectrum(teff, logg, original)
            )

    def test_load_keeps_metadata(self, tmp_path):
        original = _make_pickled_model(tmp_path / "model.pkl")
        export_pca_model(tmp_path / "model.pkl", tmp_path / "model.npz", n_check=10)
        model = load_pca_model(tmp_path / "model.npz")
        np.testing.assert_array_equal(model["wavelength"], original["wavelength"])
        np.testing.assert_array_equal(model["teff_grid"], original["teff_grid"])
        np.testing.assert_array_equal(model["logg_grid"], original["logg_grid"])
        assert model["eps"] == original["eps"]
        assert model["coeffs"].shape[0] == original["pca"].n_components_

    def test_export_rejects_splines_with_different_knots(self, tmp_path):
        original = _make_pickled_model(tmp_path / "model.pkl")
        rng = np.random.default_rng(1)
        teff_grid = np.linspace(4000, 130000, 10)
        original["interp_coeffs"][-1] = RectBivariateSpline(
            teff_grid, original["logg_grid"], rng.normal(0, 1, (teff_grid.size, original["logg_grid"].size))
        )
        with open(tmp_path / "model.pkl", "wb") as fp:
            pickle.dump(original, fp)

        with pytest.raises(ValueError, match="does not share knots"):
            export_pca_model(tmp_path / "model.pkl", tmp_path / "model.npz")
