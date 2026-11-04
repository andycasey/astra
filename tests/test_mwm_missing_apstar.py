"""
Tests for preparing APOGEE mwmStar/mwmVisit spectra when files in the database are missing on disk.

The apogee_drp tables can have rows for apStar (and apVisit) files that are not on disk. These tests
check that `prepare_apogee_resampled_visit_and_coadd_spectra_from_apstar`:

- falls back to an older apStar (e.g. `dr17`) when the newest one (`sdss5`) is missing;
- resamples visits whose apStar is missing from their apVisit, with `in_stack=False`;
- still returns visits when no apStar file exists at all (no coadd);
- omits visits whose apVisit is missing, or whose resampling fails, rather than writing zeros;
- reports all of this through the returned missing-file flags.

File access is stubbed: `absolute_path` points at temporary files, and the pixel arrays return
small constant arrays (or raise `FileNotFoundError`), so no real FITS files are needed.
"""
import os
os.environ["ASTRA_DATABASE_PATH"] = ":memory:"

import numpy as np
import pytest

from astra.models.base import database
from astra.models.source import Source
from astra.models.spectrum import Spectrum
from astra.models.apogee import ApogeeVisitSpectrum, ApogeeVisitSpectrumInApStar, ApogeeCoaddedSpectrumInApStar
from astra.models.mwm import ApogeeCombinedSpectrum, ApogeeRestFrameVisitSpectrum
import astra.products.apogee as apogee_products

MODELS = [
    Source, Spectrum,
    ApogeeVisitSpectrum, ApogeeVisitSpectrumInApStar, ApogeeCoaddedSpectrumInApStar,
    ApogeeCombinedSpectrum, ApogeeRestFrameVisitSpectrum,
]
P = ApogeeCombinedSpectrum().wavelength.size
APREDS = ("1.6", "dr17")
APRED = {"sdss5": "1.6", "dr17": "dr17"}
# Flux values that identify where a visit's spectrum came from.
APSTAR_FLUX = {"sdss5": 5.0, "dr17": 17.0}
RESAMPLED_FLUX = 99.0


class FakeContinuumModel:

    def fit(self, flux, ivar, full_output=False):
        meta = dict(rchi2=1.0, theta=np.zeros(3), W=np.ones(3), rectified_model_flux=np.ones_like(flux))
        return (np.ones_like(flux), ), meta

    def _theta_step(self, flux, ivar, rectified_model_flux):
        return None, np.ones_like(flux)


@pytest.fixture
def env(tmp_path, monkeypatch):
    database.drop_tables(MODELS)
    database.create_tables(MODELS)

    state = dict(missing_apvisit_mjds=set(), failing_resample_mjds=set())

    def apstar_path(self):
        return str(tmp_path / f"apStar-{self.release}-{self.telescope}-{self.obj}.fits")

    for model in (ApogeeCoaddedSpectrumInApStar, ApogeeVisitSpectrumInApStar):
        monkeypatch.setattr(model, "absolute_path", property(apstar_path))

    def apstar_pixel_array(name):
        def get(self):
            if not os.path.exists(self.absolute_path):
                raise FileNotFoundError(self.absolute_path)
            return {
                "wavelength": ApogeeCombinedSpectrum().wavelength,
                "flux": np.full(P, APSTAR_FLUX[self.release]),
                "ivar": np.ones(P),
                "pixel_flags": np.zeros(P, dtype=np.uint64),
            }[name]
        return property(get)

    def apvisit_pixel_array(name):
        def get(self):
            if self.mjd in state["missing_apvisit_mjds"]:
                raise FileNotFoundError(f"apVisit for mjd={self.mjd}")
            return {
                "wavelength": np.arange(10, dtype=float) + self.mjd,
                "flux": np.ones(10),
                "ivar": np.ones(10),
                "pixel_flags": np.zeros(10, dtype=np.uint64),
            }[name]
        return property(get)

    for name in ("wavelength", "flux", "ivar", "pixel_flags"):
        monkeypatch.setattr(ApogeeCoaddedSpectrumInApStar, name, apstar_pixel_array(name))
        monkeypatch.setattr(ApogeeVisitSpectrumInApStar, name, apstar_pixel_array(name))
        monkeypatch.setattr(ApogeeVisitSpectrum, name, apvisit_pixel_array(name))

    def fake_resample(wavelength, new_wavelength, flux, ivar, n_res=None, pixel_flags=None):
        # The apVisit wavelength stub encodes the MJD in its first value.
        if int(wavelength[0]) in state["failing_resample_mjds"]:
            raise ValueError("resampling failed")
        P_ = new_wavelength.size
        return (np.full(P_, RESAMPLED_FLUX), np.ones(P_), np.zeros(P_, dtype=np.uint64))

    monkeypatch.setattr(apogee_products, "resample", fake_resample)
    monkeypatch.setattr(apogee_products, "apogee_continuum_model", FakeContinuumModel())

    state["tmp_path"] = tmp_path
    yield state
    database.drop_tables(MODELS)


def _source():
    return Source.create(sdss_id=12345, healpix=1)


def _write_apstar(env, release, obj="2M00000000+0000000"):
    path = env["tmp_path"] / f"apStar-{release}-apo25m-{obj}.fits"
    path.touch()
    return path


def _coadd(source, release, obj="2M00000000+0000000"):
    return ApogeeCoaddedSpectrumInApStar.create(
        source=source,
        spectrum_pk=Spectrum.create().pk,
        release=release,
        apred=APRED[release],
        obj=obj,
        telescope="apo25m",
    )


def _visit(source, release, mjd, in_apstar=True, obj="2M00000000+0000000"):
    visit = ApogeeVisitSpectrum.create(
        source=source,
        spectrum_pk=Spectrum.create().pk,
        release=release,
        apred=APRED[release],
        plate="1",
        telescope="apo25m",
        fiber=1,
        mjd=mjd,
        field="field",
        prefix="ap",
    )
    if in_apstar:
        ApogeeVisitSpectrumInApStar.create(
            source=source,
            spectrum_pk=Spectrum.create().pk,
            drp_spectrum_pk=visit.spectrum_pk,
            release=release,
            apred=APRED[release],
            obj=obj,
            telescope="apo25m",
            plate="1",
            mjd=mjd,
            fiber=1,
        )
    return visit


def _prepare(source):
    return apogee_products.prepare_apogee_resampled_visit_and_coadd_spectra_from_apstar(source, "APO", APREDS)


def _by_mjd(visits):
    return {v.mjd: v for v in visits}


def test_all_files_present_prefers_sdss5(env):
    source = _source()
    _coadd(source, "dr17"); _coadd(source, "sdss5")
    _write_apstar(env, "dr17"); _write_apstar(env, "sdss5")
    _visit(source, "dr17", 56000)
    _visit(source, "sdss5", 59000)

    coadd, visits, flags = _prepare(source)

    assert coadd.release == "sdss5"
    assert np.all(coadd.flux == APSTAR_FLUX["sdss5"])
    visits = _by_mjd(visits)
    assert visits[56000].in_stack and np.all(visits[56000].flux == APSTAR_FLUX["dr17"])
    assert visits[59000].in_stack and np.all(visits[59000].flux == APSTAR_FLUX["sdss5"])
    assert not any(flags.values())


def test_missing_sdss5_apstar_falls_back_to_dr17(env):
    source = _source()
    _coadd(source, "dr17"); _coadd(source, "sdss5")
    _write_apstar(env, "dr17") # sdss5 apStar is in the database but not on disk
    _visit(source, "dr17", 56000)
    _visit(source, "sdss5", 59000)

    coadd, visits, flags = _prepare(source)

    assert coadd is not None and coadd.release == "dr17"
    assert np.all(coadd.flux == APSTAR_FLUX["dr17"])

    visits = _by_mjd(visits)
    assert set(visits) == {56000, 59000}
    assert visits[56000].in_stack and np.all(visits[56000].flux == APSTAR_FLUX["dr17"])
    # sdss5 visit: apStar missing, so resampled from the apVisit and not in the stack.
    assert not visits[59000].in_stack and np.all(visits[59000].flux == RESAMPLED_FLUX)

    assert flags["missing_apstar_file"]
    assert not flags["missing_apvisit_file"]
    assert not flags["apogee_visits_without_coadd"]

    assert ApogeeCombinedSpectrum.select().count() == 1
    assert ApogeeRestFrameVisitSpectrum.select().count() == 2


def test_no_apstar_on_disk_still_returns_visits(env):
    source = _source()
    _coadd(source, "sdss5") # in the database, not on disk
    _visit(source, "sdss5", 59000)
    _visit(source, "sdss5", 59001)

    coadd, visits, flags = _prepare(source)

    assert coadd is None
    assert len(visits) == 2
    for visit in visits:
        assert not visit.in_stack
        assert np.all(visit.flux == RESAMPLED_FLUX)
        assert np.all(np.isnan(visit.continuum))

    assert flags["missing_apstar_file"]
    assert flags["apogee_visits_without_coadd"]

    assert ApogeeCombinedSpectrum.select().count() == 0
    assert ApogeeRestFrameVisitSpectrum.select().count() == 2


def test_no_apstar_rows_in_database_still_returns_visits(env):
    # The DRP never made an apStar for this source: visits only, no apStar rows at all.
    source = _source()
    _visit(source, "sdss5", 59000, in_apstar=False)

    coadd, visits, flags = _prepare(source)

    assert coadd is None
    assert len(visits) == 1
    assert not visits[0].in_stack and np.all(visits[0].flux == RESAMPLED_FLUX)
    assert np.all(np.isnan(visits[0].continuum))
    assert not flags["missing_apstar_file"]
    assert flags["apogee_visits_without_coadd"]


def test_no_apstar_and_no_visits_returns_nothing(env):
    source = _source()
    _coadd(source, "sdss5") # in the database, not on disk

    coadd, visits, flags = _prepare(source)

    assert coadd is None and visits is None
    assert flags["missing_apstar_file"]
    assert not flags["apogee_visits_without_coadd"]


def test_missing_apvisit_is_omitted(env):
    source = _source()
    _coadd(source, "sdss5")
    _write_apstar(env, "sdss5")
    _visit(source, "sdss5", 59000)
    _visit(source, "sdss5", 59001, in_apstar=False)
    _visit(source, "sdss5", 59002, in_apstar=False)
    env["missing_apvisit_mjds"].add(59001)

    coadd, visits, flags = _prepare(source)

    assert coadd.release == "sdss5"
    visits = _by_mjd(visits)
    assert set(visits) == {59000, 59002}
    assert visits[59000].in_stack and np.all(visits[59000].flux == APSTAR_FLUX["sdss5"])
    assert not visits[59002].in_stack and np.all(visits[59002].flux == RESAMPLED_FLUX)
    assert flags["missing_apvisit_file"]
    assert not flags["missing_apstar_file"]


def test_failed_resample_is_omitted_not_zeros(env):
    source = _source()
    _coadd(source, "sdss5")
    _write_apstar(env, "sdss5")
    _visit(source, "sdss5", 59000, in_apstar=False)
    _visit(source, "sdss5", 59001, in_apstar=False)
    env["failing_resample_mjds"].add(59000)

    coadd, visits, flags = _prepare(source)

    assert [v.mjd for v in visits] == [59001]
    assert np.all(visits[0].flux == RESAMPLED_FLUX)
    assert ApogeeRestFrameVisitSpectrum.select().count() == 1
