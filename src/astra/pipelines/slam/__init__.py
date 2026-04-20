__slam_version__ = "1.2019.0109.4"

# === Compatibility fixes for loading pickled models ===
# These must be at module level so they're applied when worker processes import this module
import sys

# Sklearn compatibility: _PredictScorer was renamed to _Scorer in sklearn >= 1.0
import sklearn.metrics._scorer as _scorer_module
if not hasattr(_scorer_module, '_PredictScorer'):
    _scorer_module._PredictScorer = _scorer_module._Scorer

# === End sklearn compatibility fix ===

from .slam3 import Slam3 as SlamCode

# Module alias: the pickled model was created when 'slam' was a top-level module
# Register this module (astra.pipelines.slam) as 'slam' for backwards compatibility
# This must be done AFTER imports to avoid circular import issues
from . import slam3 as _slam3_module
from . import standardization as _standardization_module
sys.modules['slam'] = sys.modules[__name__]
sys.modules['slam.slam3'] = _slam3_module
sys.modules['slam.standardization'] = _standardization_module

from .laspec.convolution import conv_spec, fwhm2resolution
from .laspec.qconv import conv_spec_Gaussian
from .laspec.normalization import normalize_spectrum, normalize_spectra_block
from .laspec.binning import rebin, rebin_batch
from .predict import extract_svr_arrays

import os
import numpy as np
from tqdm import tqdm
from joblib import load
from peewee import JOIN, fn, ModelSelect
from astra import task, __version__, log
from astra.utils import expand_path
from astra.models import BossCombinedSpectrum
from astra.models.slam import Slam
from astropy.table import Table
from typing import Iterable, Optional
from astra.models import Source


def _prefetch_sources(spectra):
    """Bulk-load Source objects to avoid N+1 queries.

    Modifies spectra in-place by setting the source cache on each object.
    Returns the list of spectra (materialized if it was an iterator).
    """
    if not isinstance(spectra, list):
        spectra = list(spectra)

    source_pks = set()
    for s in spectra:
        # source_id is the raw FK value (peewee convention)
        pk = s.__data__.get("source_pk") or getattr(s, "source_id", None)
        if pk is not None:
            source_pks.add(pk)

    if not source_pks:
        return spectra

    # Bulk fetch all Source objects in one query
    sources_by_pk = {}
    # Query in batches of 5000 to avoid overly large IN clauses
    source_pks = list(source_pks)
    for i in range(0, len(source_pks), 5000):
        batch = source_pks[i:i + 5000]
        for src in Source.select().where(Source.pk.in_(batch)):
            sources_by_pk[src.pk] = src

    # Pre-populate the FK cache on each spectrum
    for spectrum in spectra:
        pk = spectrum.__data__.get("source_pk") or getattr(spectrum, "source_id", None)
        if pk in sources_by_pk:
            spectrum.source = sources_by_pk[pk]

    return spectra


def _check_selection(source):
    """Check if a source passes SLAM selection criteria.

    Returns (passes_magnitude_cut, passes_program_cut).
    """
    passes_magnitude_cut = (
        source.g_mag is not None
        and source.rp_mag is not None
        and source.plx is not None
        and source.plx > 0
        and (source.g_mag - source.rp_mag) > 0.56
        and (source.g_mag + 5 + 5 * np.log10(source.plx / 1000)) > 5.553
    )
    passes_program_cut = (
        ("mwm_yso" in source.sdss5_cartons["program"])
        or ("mwm_snc" in source.sdss5_cartons["program"])
    )
    return passes_magnitude_cut, passes_program_cut


@task
def slam_filter(spectra: Iterable[BossCombinedSpectrum], **kwargs) -> Iterable[Slam]:
    spectra = _prefetch_sources(spectra)
    for spectrum in spectra:
        source = spectrum.source
        passes_magnitude_cut, passes_program_cut = _check_selection(source)
        if not (passes_magnitude_cut or passes_program_cut):
            yield Slam.from_spectrum(
                spectrum,
                flag_not_magnitude_cut=not passes_magnitude_cut,
                flag_not_carton_match=not passes_program_cut
            )


def _get_n_jobs(n_jobs):
    """Resolve n_jobs: None -> auto-detect from available CPUs."""
    if n_jobs is None:
        try:
            return len(os.sched_getaffinity(0))
        except AttributeError:
            return os.cpu_count() or 1
    return n_jobs


def _yield_chunk_results(Xinit, Rpred, chunk_spectra):
    """Yield Slam results for one processed chunk."""
    Xpred = np.array([r["x"] for r in Rpred])
    Xpred_err = np.array([np.diag(r["pcov"]) for r in Rpred])

    for i, spectrum in enumerate(chunk_spectra):
        kwds = dict(
            fe_h_niu=Xpred[i, 0],
            e_fe_h_niu=Xpred_err[i, 0],
            fe_h=Xpred[i, 1],
            e_fe_h=Xpred_err[i, 1],
            alpha_fe=Xpred[i, 2],
            e_alpha_fe=Xpred_err[i, 2],
            teff=Xpred[i, 3],
            e_teff=Xpred_err[i, 3],
            logg=Xpred[i, 4],
            e_logg=Xpred_err[i, 4],

            initial_fe_h_niu=Xinit[i, 0],
            initial_fe_h=Xinit[i, 1],
            initial_alpha_fe=Xinit[i, 2],
            initial_teff=Xinit[i, 3],
            initial_logg=Xinit[i, 4],
            status=Rpred[i]["status"],
            success=Rpred[i]["success"],
            optimality=Rpred[i]["optimality"],
            chi2=np.nan,
            rchi2=np.nan
        )
        kwds.update(
            flag_teff_outside_bounds=(kwds["teff"] < 2800) or (kwds["teff"] > 4500),
            flag_fe_h_outside_bounds=(kwds["fe_h"] < -1) or (kwds["fe_h"] > 0.5),
            flag_bad_optimizer_status=(kwds["status"] > 0 and kwds["status"] != 2) | (kwds["status"] < 0),
        )
        yield Slam.from_spectrum(spectrum, **kwds)


#  According to the Bible, we're roughly dealing with an absolute magnitude range M_G in [7.57, 13.35] for M dwarfs between 4000 and 3000 K. It might be worth including this cut when training/running the SLAM
@task
def slam(
    spectra: Iterable[BossCombinedSpectrum],
    page=None,
    limit=None,
    n_jobs=None,
    batch_size=500,
    **kwargs
) -> Iterable[Slam]:

    n_jobs = _get_n_jobs(n_jobs)
    log.info(f"SLAM using n_jobs={n_jobs}")

    wave_interp = Table.read(expand_path("$MWM_ASTRA/pipelines/slam/dM_train_wave_standard.csv"))['wave']
    dump_path = expand_path("$MWM_ASTRA/pipelines/slam/Train_FGK_LAMOST_M_BOSS_alpha_from_ASPCAP_teff_logg_from_ApogeeNet_nobinaries.dump")

    import warnings
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore', category=UserWarning, message='.*unpickle.*')
        Pre = load(dump_path)

    # Extract SVR arrays once for vectorized prediction
    svr_arrays = extract_svr_arrays(Pre.sms)

    # --- Phase 1: Bulk prefetch sources & filter spectra ---
    spectra = _prefetch_sources(spectra)

    wave_batch = []
    flux_batch = []
    ferr_batch = []
    used_spectra = []

    for spectrum in tqdm(spectra, desc="Filtering"):
        source = spectrum.source
        passes_magnitude_cut, passes_program_cut = _check_selection(source)

        if not (passes_magnitude_cut or passes_program_cut):
            yield Slam.from_spectrum(
                spectrum,
                flag_not_magnitude_cut=not passes_magnitude_cut,
                flag_not_carton_match=not passes_program_cut
            )
            continue

        try:
            if spectrum.flux.size == 0:
                continue
        except:
            continue


        try:
            if isinstance(spectrum, BossCombinedSpectrum):
                wave = spectrum.wavelength
            else:
                wave = spectrum.wavelength / (1 + spectrum.xcsao_v_rad / 299792.458)
            wave_batch.append(wave)
            flux_batch.append(spectrum.flux)
            ferr_batch.append(spectrum.e_flux)
        except Exception:
            log.exception(f"Failed to read spectrum {spectrum.spectrum_pk}")
            continue

        used_spectra.append(spectrum)

    if not used_spectra:
        print(f"Returning!")
        return

    # --- Phase 2: Vectorized batch rebinning ---
    log.info(f"Rebinning {len(used_spectra)} spectra")
    flux_boss, ivar_boss = rebin_batch(wave_batch, flux_batch, ferr_batch, wave_interp)

    # --- Phase 3: Process in chunks (normalize -> predict) ---
    n_total = len(used_spectra)
    for start in tqdm(range(0, n_total, batch_size), desc="Processing chunks"):
        end = min(start + batch_size, n_total)
        chunk_flux = flux_boss[start:end]
        chunk_ivar = ivar_boss[start:end]
        chunk_spectra = used_spectra[start:end]
        chunk_size = end - start

        log.info(f"Processing chunk [{start}:{end}] of {n_total}")

        # Normalize
        flux_norm, flux_cont = normalize_spectra_block(
            wave_interp, chunk_flux,
            (6001.755, 8957.321),
            10.,
            p=(1E-8, 1E-7),
            q=0.7, eps=1E-19, rsv_frac=2., n_jobs=n_jobs, verbose=5
        )
        flux_norm[flux_norm > 2.] = 1
        flux_norm[flux_norm < 0] = 0
        ivars_norm = chunk_ivar * flux_cont**2

        # Quick chi2 search for initial labels
        log.info("Predicting labels (first pass)")
        Xinit = Pre.predict_labels_quick(flux_norm, ivars_norm, n_jobs=n_jobs, verbose=5)

        # Full optimization with vectorized SVR
        log.info("Predicting labels (optimization)")
        Rpred = Pre.predict_labels_multi_fast(
            Xinit, flux_norm, ivars_norm,
            n_jobs=n_jobs, verbose=5, svr_arrays=svr_arrays
        )

        # Yield results for this chunk
        yield from _yield_chunk_results(Xinit, Rpred, chunk_spectra)



if __name__ == "__main__":

    from astra.models import Source, ASPCAP, BossVisitSpectrum, Slam, BossCombinedSpectrum
    '''
    spectra = list(
        BossVisitSpectrum
        .select()
        .join(Source)
        .join(ASPCAP)
        .switch(BossVisitSpectrum)
        .join(Slam, on=(BossVisitSpectrum.source_pk == Slam.source_pk))
        .where(
            (BossVisitSpectrum.run2d == "v6_1_3")
        &   (ASPCAP.flag_as_m_dwarf_for_calibration)
        &   (ASPCAP.v_astra == "0.6.0")
        &   (Slam.v_astra == "0.6.0")
        )

        .limit(10)
    )

    spectra = list(
        BossCombinedSpectrum
        .select()
        .join(Slam, on=(BossCombinedSpectrum.spectrum_pk == Slam.spectrum_pk))
        .where(Slam.v_astra == "0.6.0")
        .limit(100)
    )
    '''
    from astropy.table import Table
    from astra.utils import expand_path
    t = Table.read(expand_path("$MWM_ASTRA/pipelines/slam/SLAM_test_astra_0.6.0.fits"))

    spectra = list(
        BossCombinedSpectrum
        .select()
        .where(BossCombinedSpectrum.spectrum_pk.in_(list(t["spectrum_pk"])))
    )


    from astra.pipelines.slam import slam
    results = list(slam(spectra))

    # match up to the rows in the table
    import numpy as np
    spectrum_pks = np.array([r.spectrum_pk for r in results])

    t.sort("spectrum_pk")
    t_spectrum_pks = np.array(t["spectrum_pk"])

    indices = np.array([t_spectrum_pks.searchsorted(s) for s in spectrum_pks])
    t = t[indices]

    assert np.all(np.array(t["spectrum_pk"]) == spectrum_pks)

    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(2, 2, figsize=(10, 10))
    label_names = [
        ("teff_pre", "teff", "initial_teff"),
        ("logg_pre", "logg", "initial_logg"),
        ("feh_niu_pre", "fe_h_niu", "initial_fe_h_niu"),
        ("alph_m_pre", "alpha_fe", "initial_alpha_fe")
    ]
    vrad = [getattr(r, "xcsao_v_rad") for r in spectra]

    for i, ax in enumerate(axes.flat):
        xlabel, ylabel, zlabel = label_names[i]
        x = t[xlabel]
        y = [getattr(r, ylabel) for r in results]
        z = [getattr(r, zlabel) for r in results]

        scat = ax.scatter(x, y, c=vrad)
        plt.colorbar(scat, ax=ax)

        ax.set_xlabel(xlabel)
        ax.set_ylabel(ylabel)
        limits = np.array([ax.get_xlim(), ax.get_ylim()])
        limits = (np.min(limits), np.max(limits))
        ax.plot(limits, limits, c="k", ls="--")
        ax.set_xlim(limits)
        ax.set_ylim(limits)



    raise a


    before = list(Slam.select().where(Slam.v_astra == "0.6.0").where(Slam.spectrum_pk.in_([s.spectrum_pk for s in spectra])))
    after = list(slam(spectra))

    index = np.argsort([s.spectrum_pk for s in spectra])
    before_sorted = [before[i] for i in index]
