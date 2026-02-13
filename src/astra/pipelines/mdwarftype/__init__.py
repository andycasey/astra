import concurrent.futures
import numpy as np
import os
import threading
import queue
from scipy.optimize import curve_fit
from typing import Iterable, Union
from tqdm import tqdm

from astra import task
from astra.models.mdwarftype import MDwarfType
from astra.models.boss import BossVisitSpectrum
from astra.models.mwm import BossCombinedSpectrum
from astra.utils import log, expand_path


@task
def mdwarftype(
    spectra: Iterable[Union[BossVisitSpectrum, BossCombinedSpectrum]],
    template_list: str = "$MWM_ASTRA/pipelines/MDwarfType/template.list",
    max_workers: int = 64,
    **kwargs
) -> Iterable[MDwarfType]:
    """
    Classify a single M dwarf from spectral templates.
    """

    template_flux, template_type = read_template_fluxes_and_types(template_list)

    executor = concurrent.futures.ProcessPoolExecutor(max_workers)

    # Queue to receive futures from the submitter thread
    future_queue = queue.Queue()
    SENTINEL = object()

    def submit_spectra():
        """Submit spectra to executor in a separate thread."""
        for spectrum in spectra:
            future = executor.submit(_mdwarf_type, spectrum, template_flux, template_type)
            future_queue.put(future)
        future_queue.put(SENTINEL)

    # Start submitting in background thread
    submitter = threading.Thread(target=submit_spectra)
    submitter.start()

    # Collect and yield results as futures complete
    pending = set()
    done_submitting = False

    while True:
        # Grab any new futures from the queue (non-blocking)
        while True:
            try:
                item = future_queue.get_nowait()
                if item is SENTINEL:
                    done_submitting = True
                else:
                    pending.add(item)
            except queue.Empty:
                break

        if not pending:
            if done_submitting:
                break
            # Wait a bit for more futures to be submitted
            try:
                item = future_queue.get(timeout=0.01)
                if item is SENTINEL:
                    done_submitting = True
                else:
                    pending.add(item)
            except queue.Empty:
                continue

        # Wait for any pending future to complete
        if pending:
            done, pending = concurrent.futures.wait(
                pending,
                timeout=0.1,
                return_when=concurrent.futures.FIRST_COMPLETED
            )
            for future in done:
                yield future.result()

    submitter.join()
    executor.shutdown(wait=False)


def _mdwarf_type(spectrum, template_flux, template_type):
    """Process a single spectrum and return an MDwarfType result."""
    try:
        # TODO: replace this with astra.specutils.continuum.Scalar
        mask = (7495 <= spectrum.wavelength) * (spectrum.wavelength <= 7505)
        continuum = np.nanmean(spectrum.flux[mask])

        flux = rectification_spectrum(spectrum.flux / continuum)
        ivar = rectification_ivar(spectrum.ivar, flux)
        chi2s = np.nansum((flux - template_flux)**2 * ivar, axis=1)
        index = np.argmin(chi2s)
        chi2 = chi2s[index]
        rchi2 = chi2 / (flux.size - 2)

        spectral_type, sub_type = template_type[index]

        result_flags = 1 if spectral_type == "K5.0" else 0

        return MDwarfType(
            spectrum_pk=spectrum.spectrum_pk,
            source_pk=spectrum.source_pk,
            spectral_type=spectral_type,
            sub_type=sub_type,
            continuum=continuum,
            rchi2=rchi2,
            result_flags=result_flags
        )
    except:
        return MDwarfType.from_spectrum(spectrum, flag_exception=True)


def read_template_fluxes_and_types(template_list):
    with open(expand_path(template_list), "r") as fp:
        template_list = list(map(str.strip, fp.readlines()))

    template_flux = list(map(read_and_resample_template, template_list))
    template_type = list(map(get_template_type, template_list))
    return (template_flux, template_type)


def get_template_type(path):
    _, spectral_type, sub_type = os.path.basename(path).split("_")
    sub_type = sub_type[:-4]
    return spectral_type, sub_type


def read_and_resample_template(path):
    log_wl, flux = np.loadtxt(
        expand_path(path),
        skiprows=1,
        delimiter=",",
        usecols=(1, 2)
    ).T
    common_wavelength = 10**(3.5523 + 0.0001 * np.arange(4648))
    # Interpolate to the BOSS wavelength grid
    return np.interp(common_wavelength, 10**log_wl, flux, left=np.nan, right=np.nan)

def quad_func(x, a, b, c):
    return (a * (x ** 2)) + (b * x) + c


def rectification_spectrum(spectrum):
    best_fit = curve_fit(quad_func, np.arange(0, len(spectrum)), spectrum) #fits quadratic function
    rectified_spec = (spectrum / quad_func(np.arange(0, len(spectrum)), *best_fit[0]) ) - 1 #divides spectrum by best fit
    return rectified_spec


def rectification_ivar(ivar,spectrum):
    best_fit = curve_fit(quad_func, np.arange(0, len(spectrum)), spectrum) #fits quadratic function
    rectified_ivar = (ivar * (quad_func(np.arange(0, len(spectrum)), *best_fit[0]))**2 ) #divides spectrum by best fit
    return rectified_ivar
