import concurrent.futures
import numpy as np
from mdwarf_contin.normalize import ContinuumNormalize
from typing import Iterable, Union
from peewee import chunked
from tqdm import tqdm

from astra import task
from astra.models.mwm import BossVisitSpectrum, BossCombinedSpectrum
from astra.models.mdwarf_standardize import MDwarfStandardize
from astra.utils import log


@task
def mdwarfstandardize(
    spectra: Iterable[Union[BossVisitSpectrum, BossCombinedSpectrum]],
    max_workers: int = 4,
    batch_size: int = 10_000,
    **kwargs
) -> Iterable[MDwarfStandardize]:
    """
    Standardize an M dwarf and return its pseudo-continuum
    """
    executor = concurrent.futures.ProcessPoolExecutor(max_workers)

    futures = []
    batch_size = batch_size or int(np.ceil(len(spectra) / max_workers))
    for chunk in tqdm(chunked(spectra, batch_size), total=1, desc="Submitting work"):
        futures.append(executor.submit(_mdwarf_standardize, chunk))

    with tqdm(total=len(futures), desc="Collecting futures") as pb:
        for future in concurrent.futures.as_completed(futures):
            yield from future.result()
            pb.update()


def _check_selection(source):
    """Check if a source is likely M dwarf
    
    Returns passes_cut
    """
    passes_cut = (
        source.g_mag is not None
        and source.rp_mag is not None
        and source.plx is not None
        and source.plx > 0
        and (source.g_mag - source.rp_mag) > 0.56
        and (source.g_mag + 5 + 5 * np.log10(source.plx / 1000)) > 8.16
        and (source.g_mag + 5 + 5 * np.log10(source.plx / 1000)) < (10 * (source.g_mag - source.rp_mag) + 5)
    )
    return passes_cut


def _mdwarf_standardize(spectra):

    results = []
    for spectrum in spectra:
        source = spectrum.source
        passes_cut = _check_selection(source)
        if not passes_cut:
            results.append(
                MDwarfStandardize.from_spectrum(
                    spectrum,
                    flag_not_dwarf=not passes_cut
                )
            )
            continue
        try:
            loglam = np.log10(spectrum.wavelength)
            ev_mask = loglam > 3.6
            norm = ContinuumNormalize(loglam[ev_mask],
                                      spectrum.flux[ev_mask])
            norm.find_continuum()
            pseudo_continuum = np.zeros_like(spectrum.flux) + np.nan
            pseudo_continuum[ev_mask] = norm.continuum

            results.append(
                MDwarfStandardize(
                    spectrum_pk=spectrum.spectrum_pk,
                    source_pk=spectrum.source_pk,
                    pseudo_continuum=pseudo_continuum
                )
            )
        except:
            log.exception(f"Exception in MDwarfStandardize for spectrum {spectrum}")
            results.append(
                MDwarfStandardize.from_spectrum(
                    spectrum,
                    flag_cont_fail=True
                )
            )
            continue

    return results
