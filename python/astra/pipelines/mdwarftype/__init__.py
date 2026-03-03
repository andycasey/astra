import concurrent.futures
import numpy as np
import os
from scipy.optimize import curve_fit
from typing import Iterable
from peewee import chunked
from tqdm import tqdm

from astra import task
from astra.models.spectrum import SpectrumMixin
from astra.models.mdwarftype import MDwarfType
from astra.utils import log, expand_path


@task
def mdwarftype(
    spectra: Iterable[SpectrumMixin],
    template_list: str = "$MWM_ASTRA/pipelines/MDwarfType/template.list",
    max_workers: int = 4,
    batch_size: int = 10_000
) -> Iterable[MDwarfType]:
    """
    Classify a single M dwarf from spectral templates.
    """

    template_flux, template_type = read_template_fluxes_and_types(template_list)
    
    # rectify the spectra
    common_wavelength = 10**(3.5523 + 0.0001 * np.arange(4648))
    mask = (7495 <= common_wavelength) * (common_wavelength <= 7505)
    crop = (5000 <= common_wavelength) & (common_wavelength <= 8800) 
    rectified_template_flux = np.zeros_like(template_flux)
    for i, f in enumerate(template_flux):
        continuum = np.nanmean(f[mask])
        rectified_template_flux[i, crop], _ = rectification_spectrum(f[crop] / continuum)

    executor = concurrent.futures.ProcessPoolExecutor(max_workers)

    futures = []
    batch_size = batch_size or int(np.ceil(len(spectra) / max_workers))
    for chunk in tqdm(chunked(spectra, batch_size), total=1, desc="Submitting work"):
        #checked_chunk = []
        #for spectrum in chunk:
        #    if "yso" not in set(spectrum.source.sdss5_cartons["alt_program"]):
        #        continue
        #    checked_chunk.append(spectrum)
        #if len(checked_chunk) > 0:
        #    futures.append(executor.submit(_mdwarf_type, checked_chunk, template_flux, template_type))
        futures.append(executor.submit(_mdwarf_type, chunk, rectified_template_flux, template_type))

    with tqdm(total=len(futures), desc="Collecting futures") as pb:
        for future in concurrent.futures.as_completed(futures):
            yield from future.result()
            pb.update()



def _mdwarf_type(spectra, template_flux, template_type):

    results = []
    for spectrum in spectra:
        try:                
            #continuum_method: str = "astra.tools.continuum.Scalar", # --> mean
            #continuum_kwargs: dict = dict(mask=[(0, 7495), (7505, 11_000)]),
            #continuum, continuum_meta = f_continuum.fit(spectrum.flux, spectrum.ivar)
            # TODO: replace this with astra.specutils.continuum.Scalar
            mask = (7495 <= spectrum.wavelength) * (spectrum.wavelength <= 7505)
            crop = (5000 <= spectrum.wavelength) & (spectrum.wavelength <= 8800)  # crop to common range
            continuum = np.nanmean(spectrum.flux[mask])

            flux, rectified_cont = rectification_spectrum(spectrum.flux[crop] / continuum)
            ivar = rectification_ivar(spectrum.ivar[crop], rectified_cont * continuum)  # need to add mean continuum to scale!
            chi2s = np.nansum((flux - template_flux[:, crop])**2 * ivar, axis=1)
            index = np.argmin(chi2s)
            chi2 = chi2s[index]
            # only include nonzero ivar and where template_flux is finite in DOF
            dof = np.sum((ivar > 0) & np.isfinite(ivar) & np.isfinite(template_flux[index, crop])) - 2
            rchi2 = chi2 / dof
            
            spectral_type, sub_type = template_type[index]

            result_flags = 1 if spectral_type == "K5.0" else 0

            results.append(
                MDwarfType(
                    spectrum_pk=spectrum.spectrum_pk,
                    source_pk=spectrum.source_pk,
                    spectral_type=spectral_type,
                    sub_type=sub_type,
                    continuum=continuum,
                    rchi2=rchi2,
                    result_flags=result_flags
                )
            )
        except:
            log.exception(f"Exception in MDwarfType for spectrum {spectrum}")
            continue

    return results


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
    ev_fine = np.isfinite(spectrum)
    best_fit = curve_fit(quad_func, np.arange(0, len(spectrum))[ev_fine], spectrum[ev_fine]) #fits quadratic function
    rectified_cont = quad_func(np.arange(0, len(spectrum)), *best_fit[0])
    rectified_spec = (spectrum /rectified_cont ) - 1 #divides spectrum by best fit
    return rectified_spec, rectified_cont


def rectification_ivar(ivar, rectified_cont):
    rectified_ivar = (ivar * rectified_cont**2 ) #divides spectrum by best fit
    return rectified_ivar