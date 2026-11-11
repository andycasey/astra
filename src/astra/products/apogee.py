"""Functions to create APOGEE-related products."""

import os
import numpy as np
from peewee import JOIN, fn
from astra.models.source import Source
from astra.models.apogee import ApogeeVisitSpectrum, ApogeeVisitSpectrumInApStar, ApogeeCoaddedSpectrumInApStar
from astra.models.boss import BossVisitSpectrum
from astra.specutils.resampling import resample, pixel_weighted_spectrum
from astra.specutils.continuum.nmf.apogee import ApogeeNMFContinuum

from astra.migrations.utils import enumerate_new_spectrum_pks
from astra.models.base import database
from astra.models.nmf_rectify import NMFRectify

from astra.models.mwm import ApogeeCombinedSpectrum, ApogeeRestFrameVisitSpectrum
from astra.fields import BasePixelArrayAccessor
from astra import __version__
from astra.utils import log
from astra.products.utils import (
    get_fields_and_pixel_arrays,
    get_fill_value,
)

apogee_continuum_model = ApogeeNMFContinuum()
apogee_continuum_model.components[:, np.all(apogee_continuum_model.components == 0, axis=0)] = np.nan # clip the edges that we don't model :-)


def prepare_apogee_resampled_visit_and_coadd_spectra(source, observatory=None, apreds=None, fill_value=None):
    return prepare_apogee_resampled_visit_and_coadd_spectra_from_apstar(source, observatory=observatory, apreds=apreds, fill_values=fill_value)


def prepare_apogee_resampled_visit_and_coadd_spectra_from_apstar(source, observatory=None, apreds=None, fill_values=None):

    # Files that the database knows about but are not on disk. Keys match `MWMSpectrumProductStatus` flags (prefixed `flag_`).
    missing_file_flags = {
        "missing_apstar_file": False,
        "missing_apvisit_file": False,
        "apogee_visits_without_coadd": False,
    }

    # Get a SDSS spectrum first by sorting by release
    sdss_id = source.sdss_id
    q = (
        ApogeeCoaddedSpectrumInApStar
        .select()
        .where(ApogeeCoaddedSpectrumInApStar.source_pk == source.pk)
    )
    if observatory is not None:
        q = q.where(ApogeeCoaddedSpectrumInApStar.telescope.startswith(observatory.lower()))
    if apreds is not None:
        q = q.where(ApogeeCoaddedSpectrumInApStar.apred.in_(apreds))

    telescope = observatory.lower()[:3]
    q = (
        q
        .order_by(ApogeeCoaddedSpectrumInApStar.release.desc()) # prefer `sdss5` over `dr17`
        .objects()
    )

    coadd_fields = get_fields_and_pixel_arrays((ApogeeCombinedSpectrum, ))

    coadd_data = {"sdss_id": sdss_id}

    try:
        candidates = list(q)
    except:
        return (None, None, missing_file_flags)

    # Use the most recent release whose apStar file is actually on disk.
    result = None
    for candidate in candidates:
        if os.path.exists(candidate.absolute_path):
            result = candidate
            break
        log.warning(f"apStar file in database but missing on disk: {candidate.absolute_path}")
        missing_file_flags["missing_apstar_file"] = True

    if result is not None:
        result_data = getattr(result, "__data__", None) or {}
        for name, field in coadd_fields.items():
            if name in ("spectrum_pk", "pk", "v_astra", "source", "modified", "created"):
                continue

            try:
                if isinstance(field, BasePixelArrayAccessor):
                    # Pixel arrays load lazily from disk via the accessor.
                    value = getattr(result, name)
                elif name in result_data:
                    value = result_data[name]
                else:
                    value = getattr(result, name)
            except FileNotFoundError:
                break
            except:
                if name == "sdss_id":
                    value = sdss_id
                elif name in ("continuum", "nmf_rectified_model_flux", "input_spectrum_pks", "nmf_rchi2", "nmf_flags"):
                    continue
                else:
                    value = get_fill_value(field, fill_values)
                    #raise RuntimeError(f"Cannot find {name} on {result}")

            if value is None:
                value = get_fill_value(field, fill_values)
            coadd_data[name] = value

    # No apStar file on disk means no coadd, but we still prepare the visits for the mwmVisit.
    coadd_spectrum = None
    if "flux" in coadd_data:
        # Do NMF
        (coadd_continuum, ), meta = apogee_continuum_model.fit(coadd_data["flux"], coadd_data["ivar"], full_output=True)

        coadd_data.update(
            nmf_rchi2=meta["rchi2"],
            continuum_theta=meta["theta"],
            log10_W=np.log10(meta["W"]),
            continuum=coadd_continuum,
            nmf_rectified_model_flux=meta["rectified_model_flux"]
        )

        coadd_spectrum = ApogeeCombinedSpectrum(**coadd_data)
        for key in ("flux", "ivar", "continuum", "nmf_rectified_model_flux"):
            setattr(coadd_spectrum, key, coadd_data[key])

    # Now get the visits. First we will get the visits from the apStar, and then get the ones that aren't.
    q = (
        ApogeeVisitSpectrum
        .select(
            ApogeeVisitSpectrum,
            ApogeeVisitSpectrumInApStar.pk.alias("in_apstar_pk"),
        )
        .join(
            ApogeeVisitSpectrumInApStar,
            JOIN.LEFT_OUTER,
            on=(ApogeeVisitSpectrum.spectrum_pk == ApogeeVisitSpectrumInApStar.drp_spectrum_pk),
        )
        .where(ApogeeVisitSpectrum.source_pk == source.pk)
    )
    if telescope is not None:
        q = q.where(ApogeeVisitSpectrum.telescope.startswith(telescope.lower()))

    if apreds is not None:
        q = q.where(ApogeeVisitSpectrum.apred.in_(apreds))

    q = (
        q
        .order_by(ApogeeVisitSpectrum.mjd.asc())
        .objects()
    )

    visit_data = []
    visit_fields = get_fields_and_pixel_arrays((ApogeeRestFrameVisitSpectrum, ))
    for spectrum in q.iterator():
        spectrum_data = getattr(spectrum, "__data__", None) or {}
        in_apstar_pk = spectrum_data.get("in_apstar_pk", getattr(spectrum, "in_apstar_pk", None))
        visit = {
            "in_stack": in_apstar_pk is not None,
            "in_apstar_pk": in_apstar_pk,
            "sdss_id": sdss_id,
        }
        omit_visit = False
        for name, field in visit_fields.items():
            if name in ("pk", "v_astra", "modified", "created", "nmf_rchi2", "nmf_rectified_model_flux", "continuum", "nmf_flags") or name in visit:
                continue
            elif name == "date_obs":
                value = spectrum_data.get("date_obs")
                if value is not None:
                    value = value.isoformat()
            elif name == "sdss_id":
                value = sdss_id
            elif name == "healpix":
                value = source.healpix
            elif isinstance(field, BasePixelArrayAccessor):
                # Pixel arrays load lazily from disk via the accessor.
                try:
                    value = getattr(spectrum, name)
                except FileNotFoundError:
                    log.exception(f"apVisit file missing on disk for {spectrum}; omitting this visit")
                    missing_file_flags["missing_apvisit_file"] = True
                    omit_visit = True
                    break
            elif name in spectrum_data:
                value = spectrum_data[name]
            else:
                value = getattr(spectrum, name)

            if value is None:
                value = get_fill_value(field, fill_values)

            visit[name] = value
        if not omit_visit:
            visit_data.append(visit)

    q = (
        ApogeeVisitSpectrumInApStar
        .select()
        .where(ApogeeVisitSpectrumInApStar.source_pk == source.pk)
    )
    if telescope is not None:
        q = q.where(ApogeeVisitSpectrumInApStar.telescope.startswith(telescope.lower()))

    if apreds is not None:
        q = q.where(ApogeeVisitSpectrumInApStar.apred.in_(apreds))

    apvisit_in_apstar_spectra = { r.pk: r for r in q.iterator() }

    # Create visit flux arrays, either from the apVisit or from the apStar
    wavelength = ApogeeCombinedSpectrum().wavelength
    N, P = (len(visit_data), wavelength.size)
    visit_flux = np.zeros((N, P))
    visit_ivar = np.zeros((N, P))
    visit_pixel_flags = np.zeros((N, P), dtype=np.uint64)
    keep = np.ones(N, dtype=bool)
    for i, visit in enumerate(visit_data):
        if visit["in_stack"] and not os.path.exists(apvisit_in_apstar_spectra[visit["in_apstar_pk"]].absolute_path):
            # The apStar is in the database but not on disk, so resample this visit from the apVisit instead.
            visit["in_stack"] = False
            missing_file_flags["missing_apstar_file"] = True

        if visit["in_stack"]:
            spectrum = apvisit_in_apstar_spectra[visit["in_apstar_pk"]]
            visit_flux[i] = spectrum.flux
            visit_ivar[i] = spectrum.ivar
            visit_pixel_flags[i] = spectrum.pixel_flags
        else:
            # resample with 0 v_rad
            try:
                visit_flux[i], visit_ivar[i], visit_pixel_flags[i] = resample(
                    visit["wavelength"],
                    wavelength,
                    visit["flux"],
                    visit["ivar"],
                    n_res=(5, 4.25, 3.5),
                    pixel_flags=visit["pixel_flags"]
                )
            except:
                # Omit the visit rather than writing an all-zero spectrum.
                log.exception(f"Exception when trying to resample visit {visit}; omitting this visit")
                keep[i] = False

    if not np.all(keep):
        visit_data = [visit for visit, k in zip(visit_data, keep) if k]
        visit_flux, visit_ivar, visit_pixel_flags = (visit_flux[keep], visit_ivar[keep], visit_pixel_flags[keep])
        N = len(visit_data)

    if coadd_spectrum is None:
        if N == 0:
            return (None, None, missing_file_flags)
        # The visit continuum is fit using the coadd's NMF model, so without a coadd there is no continuum.
        missing_file_flags["apogee_visits_without_coadd"] = True
        visit_continuum = np.full((N, P), np.nan)
    else:
        theta, visit_continuum = apogee_continuum_model._theta_step(
            visit_flux,
            visit_ivar,
            coadd_spectrum.nmf_rectified_model_flux
        )
    visit_spectra = []
    for i, visit in enumerate(visit_data):
        visit_spectrum = ApogeeRestFrameVisitSpectrum(
            source_pk=source.pk,
            drp_spectrum_pk=visit.pop("spectrum_pk"),
            **visit
        )
        visit_spectrum.flux = visit_flux[i]
        visit_spectrum.ivar = visit_ivar[i]
        visit_spectrum.pixel_flags = visit_pixel_flags[i]
        visit_spectrum.continuum = visit_continuum[i]
        visit_spectra.append(visit_spectrum)

    new_spectra = ([coadd_spectrum] if coadd_spectrum is not None else []) + visit_spectra
    for spectrum_pk, spectrum in enumerate_new_spectrum_pks(new_spectra):
        spectrum.spectrum_pk = spectrum_pk

    with database.atomic():
        if coadd_spectrum is not None:
            (
                ApogeeCombinedSpectrum
                .delete()
                .where(
                    (ApogeeCombinedSpectrum.sdss_id == source.sdss_id)
                &   (ApogeeCombinedSpectrum.telescope == coadd_spectrum.telescope)
                &   (ApogeeCombinedSpectrum.apred == coadd_spectrum.apred)
                &   (ApogeeCombinedSpectrum.release == coadd_spectrum.release)
                &   (ApogeeCombinedSpectrum.v_astra == coadd_spectrum.v_astra)
                )
                .execute()
            )
            coadd_spectrum.save()
        if visit_spectra:
            # Remove any existing entries before we create this one.
            releases = tuple(set([s.release for s in visit_spectra]))
            (
                ApogeeRestFrameVisitSpectrum
                .delete()
                .where(
                    (ApogeeRestFrameVisitSpectrum.sdss_id == source.sdss_id)
                &   (ApogeeRestFrameVisitSpectrum.telescope == visit_spectra[0].telescope)
                &   (ApogeeRestFrameVisitSpectrum.apred.in_(apreds))
                &   (ApogeeRestFrameVisitSpectrum.release.in_(releases))
                &   (ApogeeRestFrameVisitSpectrum.v_astra == visit_spectra[0].v_astra)
                )
                .execute()
            )
            ApogeeRestFrameVisitSpectrum.bulk_create(visit_spectra)

    return (coadd_spectrum, visit_spectra, missing_file_flags)
