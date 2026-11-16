"""
Tests for the BOSS DRP `BadTraceTAB` mask, stored as `flag_bad_trace_tab`.

Checks that:

- `has_bad_trace_tab` is True only when the spectrum is flagged (missing/null -> False);
- the mask file is loaded safely (unknown run2d or missing file -> nothing flagged);
- spAll rows are matched to the mask by `SPEC_FILE` value, not row order;
- flagged visits are excluded from BOSS coadds and star-level RVs;
- visit-level pipeline results (BossNet, MDwarfType, LineForest, Corv) set a `result_flags` bit
  that forces `flag_bad`, both in Python and in the SQL expression used for summary files.

Everything runs against in-memory SQLite and temporary files: no live database or $MWM_ASTRA files.
"""
import os
os.environ["ASTRA_DATABASE_PATH"] = ":memory:"

import fitsio
import numpy as np
import pytest
from peewee import SqliteDatabase

from astra.models.source import Source
from astra.models.spectrum import Spectrum
from astra.models.boss import BossVisitSpectrum, has_bad_trace_tab
from astra.models.mwm import BossCombinedSpectrum, BossRestFrameVisitSpectrum
from astra.models.bossnet import BossNet
from astra.models.mdwarftype import MDwarfType
from astra.models.line_forest import LineForest
from astra.models.corv import Corv
from astra.migrations import boss as boss_migration
import astra.specutils.continuum.nmf.boss as nmf_boss


class FakeBossContinuumModel:
    def __init__(self, *args, **kwargs):
        pass


# `astra.products.boss` builds a `BossNMFContinuum()` at import, which loads a components file
# from $MWM_ASTRA. Swap in a fake class just for that import so no files are needed.
_BossNMFContinuum = nmf_boss.BossNMFContinuum
nmf_boss.BossNMFContinuum = FakeBossContinuumModel
try:
    import astra.products.boss as boss_products
finally:
    nmf_boss.BossNMFContinuum = _BossNMFContinuum


class FakeSpectrum:
    """A spectrum-like object; `flag_bad_trace_tab` is only set when given."""

    _missing = object()

    def __init__(self, flag_bad_trace_tab=_missing, spectrum_pk=1, source_pk=1, **kwargs):
        self.spectrum_pk = spectrum_pk
        self.source_pk = source_pk
        if flag_bad_trace_tab is not FakeSpectrum._missing:
            self.flag_bad_trace_tab = flag_bad_trace_tab
        for k, v in kwargs.items():
            setattr(self, k, v)


# ---------------------------------------------------------------------------
# has_bad_trace_tab
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("value,expected", [
    (True, True),
    (False, False),
    (None, False),
    (np.bool_(True), True),
    (np.bool_(False), False),
])
def test_has_bad_trace_tab_values(value, expected):
    result = has_bad_trace_tab(FakeSpectrum(flag_bad_trace_tab=value))
    assert result is expected


def test_has_bad_trace_tab_missing_attribute_is_false():
    assert has_bad_trace_tab(FakeSpectrum()) is False


def test_has_bad_trace_tab_on_models():
    # Visit-level models carry the column and default to False.
    assert has_bad_trace_tab(BossVisitSpectrum()) is False
    assert has_bad_trace_tab(BossVisitSpectrum(flag_bad_trace_tab=True)) is True
    assert has_bad_trace_tab(BossRestFrameVisitSpectrum(flag_bad_trace_tab=True)) is True
    # Coadds never carry the flag (flagged visits are excluded from them).
    assert "flag_bad_trace_tab" not in BossCombinedSpectrum._meta.fields
    assert has_bad_trace_tab(BossCombinedSpectrum()) is False


# ---------------------------------------------------------------------------
# Loading the mask file
# ---------------------------------------------------------------------------

def _write_mask(path, spec_files, flags):
    data = np.zeros(len(spec_files), dtype=[("SPEC_FILE", "U42"), ("BadTraceTAB", "?"), ("SPECOBJID", "U29")])
    data["SPEC_FILE"] = spec_files
    data["BadTraceTAB"] = flags
    data["SPECOBJID"] = ""  # empty for many real rows: matching must not rely on it
    fitsio.write(str(path), data, extname="SPALL", clobber=True)


def test_load_mask_unknown_run2d_returns_empty():
    assert boss_migration.load_bad_trace_tab_spec_files("not_a_run2d") == set()


def test_load_mask_missing_file_returns_empty(tmp_path, monkeypatch):
    monkeypatch.setitem(
        boss_migration.BAD_TRACE_TAB_MASK_PATHS, "v6_2_1", str(tmp_path / "does_not_exist.fits")
    )
    assert boss_migration.load_bad_trace_tab_spec_files("v6_2_1") == set()


def test_load_mask_returns_only_flagged_spec_files(tmp_path, monkeypatch):
    path = tmp_path / "spAll_mask.fits"
    _write_mask(
        path,
        ["spec-015000-59146-1.fits", "spec-015000-59146-2.fits  ", "spec-015000-59146-u0218.fits"],
        [False, True, True],
    )
    monkeypatch.setitem(boss_migration.BAD_TRACE_TAB_MASK_PATHS, "v6_2_1", str(path))
    assert boss_migration.load_bad_trace_tab_spec_files("v6_2_1") == {
        "spec-015000-59146-2.fits",
        "spec-015000-59146-u0218.fits",
    }


# ---------------------------------------------------------------------------
# Matching spAll rows to the mask
# ---------------------------------------------------------------------------

def test_match_is_by_value_not_row_order():
    bad = {"b.fits", "d.fits"}
    spec_files = np.array(["d.fits", "a.fits", "b.fits", "c.fits"])
    assert boss_migration.match_bad_trace_tab(spec_files, bad).tolist() == [True, False, True, False]


def test_match_strips_padding():
    bad = {"b.fits"}
    spec_files = np.array(["a.fits  ", "b.fits  "])
    assert boss_migration.match_bad_trace_tab(spec_files, bad).tolist() == [False, True]


def test_match_empty_mask_flags_nothing():
    spec_files = np.array(["a.fits", "b.fits"])
    result = boss_migration.match_bad_trace_tab(spec_files, set())
    assert result.dtype == bool
    assert result.tolist() == [False, False]


def test_match_spec_files_not_in_mask_are_not_flagged():
    result = boss_migration.match_bad_trace_tab(np.array(["x.fits", "y.fits"]), {"z.fits"})
    assert result.tolist() == [False, False]


# ---------------------------------------------------------------------------
# Coadd and RV selection
# ---------------------------------------------------------------------------

def _good_visit(**kwargs):
    kwds = dict(snr=20.0, xcsao_rxc=10.0, zwarning_flags=0, xcsao_v_rad=5.0)
    kwds.update(kwargs)
    return FakeSpectrum(**kwds)


@pytest.mark.parametrize("is_mwm_wd", [False, True])
def test_coadd_excludes_flagged_visit(is_mwm_wd):
    assert boss_products.include_boss_spectrum_in_coadd(_good_visit(flag_bad_trace_tab=False), is_mwm_wd)
    assert not boss_products.include_boss_spectrum_in_coadd(_good_visit(flag_bad_trace_tab=True), is_mwm_wd)


def test_coadd_unflagged_or_missing_is_included():
    assert boss_products.include_boss_spectrum_in_coadd(_good_visit(flag_bad_trace_tab=None), False)
    assert boss_products.include_boss_spectrum_in_coadd(_good_visit(), False)


def test_rv_calculation_excludes_flagged_visit():
    assert boss_products.include_boss_spectrum_in_rv_calculation(_good_visit(flag_bad_trace_tab=False))
    assert not boss_products.include_boss_spectrum_in_rv_calculation(_good_visit(flag_bad_trace_tab=True))
    assert boss_products.include_boss_spectrum_in_rv_calculation(_good_visit())


# ---------------------------------------------------------------------------
# Visit-level pipeline outputs
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("flagged", [False, True])
def test_bossnet_from_spectrum(flagged):
    s = FakeSpectrum(flag_bad_trace_tab=flagged)
    r = BossNet.from_spectrum(s, teff=5000.0, logg=4.5, fe_h=0.0)
    assert r.flag_bad_trace_tab is flagged
    assert bool(r.flag_bad) is flagged


def test_bossnet_from_spectrum_exception_path_keeps_both_flags():
    r = BossNet.from_spectrum(FakeSpectrum(flag_bad_trace_tab=True), flag_runtime_exception=True)
    assert r.flag_runtime_exception
    assert r.flag_bad_trace_tab
    assert r.flag_bad


def test_bossnet_from_coadd_like_spectrum_is_not_flagged():
    r = BossNet.from_spectrum(FakeSpectrum(), teff=5000.0, logg=4.5, fe_h=0.0)
    assert not r.flag_bad_trace_tab
    assert not r.flag_bad


def _mdwarf_inputs():
    from astra.pipelines.mdwarftype import _mdwarf_type
    wavelength = 10**(3.5523 + 1e-4 * np.arange(4648))
    flux = 1 + 1e-4 * (wavelength - 7500)  # smooth, so the rectified flux is ~0
    crop = (5000 <= wavelength) & (wavelength <= 8800)
    # Template 0 ("K5.0") matches the rectified spectrum; template 1 does not.
    template_flux = np.full((2, wavelength.size), np.nan)
    template_flux[0, crop] = 0.0
    template_flux[1, crop] = 0.5
    template_type = [("K5.0", "0.0"), ("M1.0", "0.0")]
    return _mdwarf_type, wavelength, flux, template_flux, template_type


@pytest.mark.parametrize("flagged,expected_result_flags", [(False, 1), (True, 5)])
def test_mdwarftype_worker(flagged, expected_result_flags):
    _mdwarf_type, wavelength, flux, template_flux, template_type = _mdwarf_inputs()
    s = FakeSpectrum(
        flag_bad_trace_tab=flagged,
        wavelength=wavelength,
        flux=flux,
        ivar=np.ones_like(flux),
    )
    (r, ) = _mdwarf_type([s], template_flux, template_type)
    assert r.spectral_type == "K5.0"
    # `flag_suspicious` (K5.0) must not be overwritten by the trace flag, or vice versa.
    assert r.flag_suspicious
    assert r.flag_bad_trace_tab is flagged
    assert r.result_flags == expected_result_flags
    assert r.flag_bad


@pytest.mark.parametrize("flagged", [False, True])
def test_line_forest_model(flagged):
    r = LineForest(spectrum_pk=1, source_pk=1, flag_bad_trace_tab=has_bad_trace_tab(FakeSpectrum(flag_bad_trace_tab=flagged)))
    assert r.flag_bad_trace_tab is flagged
    assert bool(r.flag_bad) is flagged


@pytest.mark.parametrize("flagged", [False, True])
def test_corv_from_spectrum_keeps_existing_flags(flagged):
    r = Corv.from_spectrum(FakeSpectrum(flag_bad_trace_tab=flagged), flag_not_da_type=True)
    assert r.flag_not_da_type
    assert r.flag_bad_trace_tab is flagged
    assert bool(r.flag_bad) is flagged


# ---------------------------------------------------------------------------
# `flag_bad` as a SQL expression (used when writing summary files)
# ---------------------------------------------------------------------------

def _bossnet(s):
    return BossNet.from_spectrum(s, teff=5000.0, logg=4.5, fe_h=0.0)


def _mdwarftype(s):
    r = MDwarfType.from_spectrum(s, spectral_type="M1.0", sub_type=0.0)
    r.flag_bad_trace_tab = has_bad_trace_tab(s)
    return r


def _line_forest(s):
    return LineForest.from_spectrum(s, flag_bad_trace_tab=has_bad_trace_tab(s))


def _corv(s):
    return Corv.from_spectrum(s, v_rad=1.0)


SQL_MODELS = [Source, Spectrum, BossNet, MDwarfType, LineForest, Corv]


@pytest.fixture
def sql_database():
    # Private in-memory database: other test modules create tables with foreign keys to
    # Source/Spectrum in the shared one.
    test_database = SqliteDatabase(":memory:")
    with test_database.bind_ctx(SQL_MODELS, bind_refs=False, bind_backrefs=False):
        # Create tables without secondary indexes: LineForest's ArrayFields declare
        # Postgres-only GIN indexes that SQLite cannot create.
        for model in SQL_MODELS:
            model._schema.create_table(safe=True)
        yield test_database
    test_database.close()


@pytest.mark.parametrize("model,make", [
    (BossNet, _bossnet),
    (MDwarfType, _mdwarftype),
    (LineForest, _line_forest),
    (Corv, _corv),
])
def test_flag_bad_sql_expression(sql_database, model, make):
    source_pk = Source.create().pk
    clean_pk = Spectrum.create(spectrum_flags=0).pk
    flagged_pk = Spectrum.create(spectrum_flags=0).pk

    make(FakeSpectrum(flag_bad_trace_tab=False, spectrum_pk=clean_pk, source_pk=source_pk)).save()
    make(FakeSpectrum(flag_bad_trace_tab=True, spectrum_pk=flagged_pk, source_pk=source_pk)).save()

    assert [pk for (pk, ) in model.select(model.spectrum_pk).where(model.flag_bad).tuples()] == [flagged_pk]
    assert [pk for (pk, ) in model.select(model.spectrum_pk).where(~model.flag_bad).tuples()] == [clean_pk]
