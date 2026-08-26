import os
import numpy as np
import datetime
import h5py as h5
from peewee import DeferredForeignKey, fn
from playhouse.hybrid import hybrid_property
from astra.fields import (
    AutoField, FloatField, BooleanField, DateTimeField, BigIntegerField, IntegerField, TextField,
    ForeignKeyField, PixelArray, BitField, LogLambdaArrayAccessor
)
from astra.models.base import BaseModel
from astra.models.spectrum import (Spectrum, SpectrumMixin)
from astra.models.source import Source
from astra.fields import BasePixelArrayAccessor


from astropy.constants import c
from astropy import units as u

C_KM_S = c.to(u.km / u.s).value

def _get_array(dir, key, index):
    with h5.File(f"{dir}/apMADGICS_out_{key}.h5", "r") as fp:
        return fp[key][index]


class ARJLFluxAccessor(BasePixelArrayAccessor):

    """A class to access ARJL pixel-based arrays."""

    def __get__(self, instance, instance_type=None):
        if instance is not None:
            self._initialise_pixel_array(instance)
            try:
                return instance.__pixel_data__[self.name]
            except KeyError:

                x_starLines_v0 = _get_array(instance.component_dir, "x_starLines_v0", instance.row_index)
                x_residuals_v0 = _get_array(instance.component_dir, "x_residuals_v0", instance.row_index)
                x_starContinuum_v0 = _get_array(instance.component_dir, "x_starContinuum_v0", instance.row_index)

                value = 1 + x_starLines_v0 + x_residuals_v0 / x_starContinuum_v0
                if self.transform is not None:
                    value = self.transform(value, None, instance)
                instance.__pixel_data__.setdefault(self.name, value[125:])

            finally:
                return instance.__pixel_data__[self.name]

        return self.field


class ARJLStarLinesFluxAccessor(BasePixelArrayAccessor):

    """A class to access ARJL pixel-based arrays (1 + x_starLines_v0, without the residuals term)."""

    def __get__(self, instance, instance_type=None):
        if instance is not None:
            self._initialise_pixel_array(instance)
            try:
                return instance.__pixel_data__[self.name]
            except KeyError:

                x_starLines_v0 = _get_array(instance.component_dir, "x_starLines_v0", instance.row_index)

                value = 1 + x_starLines_v0
                if self.transform is not None:
                    value = self.transform(value, None, instance)
                instance.__pixel_data__.setdefault(self.name, value[125:])

            finally:
                return instance.__pixel_data__[self.name]

        return self.field


class ARJLStarLinesInverseVarianceAccessor(BasePixelArrayAccessor):

    """A class to access ARJL pixel-based arrays (posterior uncertainty of x_starLines_v0)."""

    def __get__(self, instance, instance_type=None):
        if instance is not None:
            self._initialise_pixel_array(instance)
            try:
                return instance.__pixel_data__[self.name]
            except KeyError:
                x_starLines_err_v0 = _get_array(instance.component_dir, "x_starLines_err_v0", instance.row_index)

                value = 1 / x_starLines_err_v0**2
                if self.transform is not None:
                    value = self.transform(value, None, instance)
                instance.__pixel_data__.setdefault(self.name, value[125:])

            finally:
                return instance.__pixel_data__[self.name]

        return self.field


class ARJLPixelFlagsAccessor(BasePixelArrayAccessor):

    """A class to access ARJL pixel-based arrays."""

    def __get__(self, instance, instance_type=None):
        if instance is not None:
            self._initialise_pixel_array(instance)
            try:
                return instance.__pixel_data__[self.name]
            except KeyError:
                value = _get_array(instance.component_dir, "finalmsk", instance.row_index)
                if self.transform is not None:
                    value = self.transform(value, None, instance)

                instance.__pixel_data__.setdefault(self.name, value[125:])
            finally:
                return instance.__pixel_data__[self.name]

        return self.field

class ARJLInverseVarianceAccessor(BasePixelArrayAccessor):

    """A class to access ARJL pixel-based arrays."""

    def __get__(self, instance, instance_type=None):
        if instance is not None:
            self._initialise_pixel_array(instance)
            try:
                return instance.__pixel_data__[self.name]
            except KeyError:
                fluxerr2 = _get_array(instance.component_dir, "fluxerr2", instance.row_index)
                x_starContinuum_v0 = _get_array(instance.component_dir, "x_starContinuum_v0", instance.row_index)

                value = (1/fluxerr2) * x_starContinuum_v0**2
                if self.transform is not None:
                    value = self.transform(value, None, instance)
                instance.__pixel_data__.setdefault(self.name, value[125:])

            finally:
                return instance.__pixel_data__[self.name]

        return self.field


class ARJLVisitSpectrum(BaseModel, SpectrumMixin):

    """An ApogeeReduction.jl-reduced visit spectrum."""

    pk = AutoField()

    #> Identifiers
    spectrum_pk = ForeignKeyField(
        Spectrum,
        null=True,
        index=True,
        unique=True,
        lazy_load=False,
        column_name="spectrum_pk"
    )
    # Won't appear in a header group because it is first referenced in `Source`.
    source = ForeignKeyField(
        Source,
        # We want to allow for spectra to be unassociated with a source so that
        # we can test with fake spectra, etc, but any pipeline should run their
        # own checks to make sure that spectra and sources are linked.
        null=True,
        index=True,
        column_name="source_pk",
        backref="arjl_visit_spectra",
    )

    created = DateTimeField(default=datetime.datetime.now)
    modified = DateTimeField(default=datetime.datetime.now)

    catalogid = BigIntegerField(index=True, null=True)
    sdss_id = BigIntegerField(index=True, null=True)

    component_dir = TextField(null=False)
    row_index = IntegerField(index=True, null=False)
    v_arjl = TextField(null=True)

    #> Data Product Keywords
    release = TextField(index=True)
    plate = TextField(index=True)
    telescope = TextField(index=True)
    fiber = IntegerField(index=True)
    mjd = IntegerField(index=True)
    field = TextField(index=True)
    obj = TextField(null=True)
    adjfiberindx = IntegerField()

    #> Radial Velocities
    v_rad = FloatField()
    v_rel = FloatField()
    v_rad_flags = BitField(default=0)
    v_rad_minchi2_final = FloatField()
    v_rad_pix_var = FloatField()
    v_rad_pixoff_disc_final = FloatField()
    v_rad_pixoff_final = FloatField()
    v_rad_chi2_residuals = FloatField()

    #> APOGEE DR17 DRP Metadata
    drp_snr = FloatField()
    drp_starflag = BitField(default=0)
    drp_vhelio = FloatField()
    drp_vrel = FloatField()
    drp_vrelerr = FloatField()
    dr17_teff = FloatField()
    dr17_logg = FloatField()
    dr17_x_h = FloatField()
    dr17_vsini = FloatField()

    #> Spectral Data
    wavelength = PixelArray(
        accessor_class=LogLambdaArrayAccessor,
        accessor_kwargs=dict(
            crval=4.179,
            cdelt=6e-6,
            naxis=8575,
        ),
    )

class ARJLTHVisitSpectrum(ARJLVisitSpectrum):
    flux = PixelArray(accessor_class=ARJLFluxAccessor)
    ivar = PixelArray(accessor_class=ARJLInverseVarianceAccessor)
    pixel_flags = PixelArray(accessor_class=ARJLPixelFlagsAccessor)

class ARJLDDVisitSpectrum(ARJLVisitSpectrum):
    flux = PixelArray(accessor_class=ARJLFluxAccessor)
    ivar = PixelArray(accessor_class=ARJLInverseVarianceAccessor)
    pixel_flags = PixelArray(accessor_class=ARJLPixelFlagsAccessor)



def shift(array, pixels, fill_value=0):
    pixels = np.asarray(pixels).astype(int)

    if pixels.ndim == 0:
        pixels = int(pixels)
        pad = fill_value * np.ones(abs(pixels))
        if pixels >= 0:
            return np.hstack([array[pixels:], pad])
        else:
            return np.hstack([pad, array[:pixels]])

    N, P = array.shape
    col_idx = np.arange(P)
    src_idx = col_idx[np.newaxis, :] + pixels[:, np.newaxis]  # (N, P)

    valid = (src_idx >= 0) & (src_idx < P)
    row_idx = np.arange(N)[:, np.newaxis] * np.ones((1, P), dtype=int)

    out = np.where(valid, array[row_idx, np.clip(src_idx, 0, P - 1)], fill_value)
    return out


transform_to_rest = lambda a, _, instance: shift(a, instance.v_rad_pixoff_final)

class ARJLTHRestFrameVisitSpectrum(ARJLTHVisitSpectrum):
    flux = PixelArray(accessor_class=ARJLFluxAccessor, transform=transform_to_rest)
    ivar = PixelArray(accessor_class=ARJLInverseVarianceAccessor, transform=transform_to_rest)
    pixel_flags = PixelArray(accessor_class=ARJLPixelFlagsAccessor, transform=transform_to_rest)


class ARJLDDRestFrameVisitSpectrum(ARJLDDVisitSpectrum):
    flux = PixelArray(accessor_class=ARJLFluxAccessor, transform=transform_to_rest)
    ivar = PixelArray(accessor_class=ARJLInverseVarianceAccessor, transform=transform_to_rest)
    pixel_flags = PixelArray(accessor_class=ARJLPixelFlagsAccessor, transform=transform_to_rest)


class ARJLTHStarLinesVisitSpectrum(ARJLTHVisitSpectrum):
    # ivar uses ARJLInverseVarianceAccessor (same as ARJLTHVisitSpectrum), not
    # ARJLStarLinesInverseVarianceAccessor: the posterior uncertainty of x_starLines_v0
    # badly underestimates the true errors, so we borrow the TH error model as a stand-in.
    flux = PixelArray(accessor_class=ARJLStarLinesFluxAccessor)
    ivar = PixelArray(accessor_class=ARJLInverseVarianceAccessor)
    pixel_flags = PixelArray(accessor_class=ARJLPixelFlagsAccessor)


class ARJLDDStarLinesVisitSpectrum(ARJLDDVisitSpectrum):
    flux = PixelArray(accessor_class=ARJLStarLinesFluxAccessor)
    ivar = PixelArray(accessor_class=ARJLStarLinesInverseVarianceAccessor)
    pixel_flags = PixelArray(accessor_class=ARJLPixelFlagsAccessor)


class ARJLTHStarLinesRestFrameVisitSpectrum(ARJLTHStarLinesVisitSpectrum):
    flux = PixelArray(accessor_class=ARJLStarLinesFluxAccessor, transform=transform_to_rest)
    ivar = PixelArray(accessor_class=ARJLInverseVarianceAccessor, transform=transform_to_rest)
    pixel_flags = PixelArray(accessor_class=ARJLPixelFlagsAccessor, transform=transform_to_rest)


class ARJLDDStarLinesRestFrameVisitSpectrum(ARJLDDStarLinesVisitSpectrum):
    flux = PixelArray(accessor_class=ARJLStarLinesFluxAccessor, transform=transform_to_rest)
    ivar = PixelArray(accessor_class=ARJLStarLinesInverseVarianceAccessor, transform=transform_to_rest)
    pixel_flags = PixelArray(accessor_class=ARJLPixelFlagsAccessor, transform=transform_to_rest)


def _get_coadd_array(dir, basename, key, index):
    with h5.File(f"{dir}/{basename}", "r") as fp:
        return fp[key][index]


class ARJLCoaddedPixelArrayAccessor(BasePixelArrayAccessor):

    """
    A class to access ARJL coadd pixel arrays (flux, ivar, pixel_flags).

    Unlike the visit accessors above, this data doesn't come from the DRP's apMADGICS
    outputs: it's computed by Astra itself (see `astra.migrations.arjl.build_arjl_coadds`)
    and written to a small per-batch HDF5 file that we own, with one row per (source, telescope).

    The filename comes from `instance.coadd_basename`, not from these accessor kwargs: TH/DD
    and StarLines coadds are different tables (see the four leaf classes below), each writing
    to its own file in the same `component_dir` so they don't collide with one another.
    """

    def __get__(self, instance, instance_type=None):
        if instance is not None:
            self._initialise_pixel_array(instance)
            try:
                return instance.__pixel_data__[self.name]
            except KeyError:
                value = _get_coadd_array(instance.component_dir, instance.coadd_basename, self.column_name, instance.row_index)
                if self.transform is not None:
                    value = self.transform(value, None, instance)
                instance.__pixel_data__.setdefault(self.name, value)
            finally:
                return instance.__pixel_data__[self.name]

        return self.field


class ARJLCoaddedSpectrum(BaseModel, SpectrumMixin):

    """
    A per-source, per-telescope pixel-weighted coadd of ApogeeReduction.jl visit spectra.

    DRAFT. Open questions before this is real:
      - What defines a 'good' visit for `n_good_visits` / the RV statistics? (e.g. some cut
        on `v_rad_flags`, `drp_starflag`, or `v_rad_chi2_residuals`)
      - ARJL visit flux is already continuum-normalized, but that normalization may not be
        consistent between exposures of the same star. Revisit whether visits need to be
        rescaled before stacking (see `scale_by_pseudo_continuum` on `pixel_weighted_spectrum`).
      - Do TH/DD and StarLines-only coadds deserve four near-identical tables (as with the
        visit spectra), or should this collapse into one table with a `kind` column?
      - Is per-telescope grouping the right key, or should apo25m/lco25m visits of the same
        star ever be combined?
    """

    pk = AutoField()

    # Not a DB column: which per-batch HDF5 file (in `component_dir`) this leaf model's
    # pixel arrays live in. Every subclass below must set its own, distinct value so that
    # TH/DD and StarLines coadds don't collide when they share the same `component_dir`.
    coadd_basename = "arjlStarCoadd.h5"

    #> Identifiers
    spectrum_pk = ForeignKeyField(
        Spectrum,
        null=True,
        index=True,
        unique=True,
        lazy_load=False,
        column_name="spectrum_pk"
    )
    source = ForeignKeyField(
        Source,
        null=True,
        index=True,
        column_name="source_pk",
        backref="arjl_coadded_spectra",
    )

    created = DateTimeField(default=datetime.datetime.now)
    modified = DateTimeField(default=datetime.datetime.now)

    sdss_id = BigIntegerField(index=True, null=True)
    catalogid = BigIntegerField(index=True, null=True)

    #> Storage (see `ARJLCoaddedPixelArrayAccessor`)
    component_dir = TextField(null=False)
    row_index = IntegerField(index=True, null=False)
    v_arjl = TextField(null=True)

    #> Data Product Keywords
    release = TextField(index=True)
    telescope = TextField(index=True)

    #> Observing Span
    min_mjd = IntegerField(null=True)
    max_mjd = IntegerField(null=True)

    #> Number and Quality of Visits
    n_visits = IntegerField(null=True)
    n_good_visits = IntegerField(null=True)

    #> Summary Statistics
    snr = FloatField(null=True)
    mean_fiber = FloatField(null=True)
    std_fiber = FloatField(null=True)
    v_rad_flags = BitField(default=0)  # bitwise-OR of visit v_rad_flags
    drp_starflag = BitField(default=0)  # bitwise-OR of visit drp_starflag

    #> Radial Velocity
    # Inverse-variance weighted mean of visit v_rad, using v_rad_pix_var (converted from
    # pixels to km/s) as the per-visit weight -- see weighted_average() in migrations/arjl.py.
    v_rad = FloatField(null=True)
    e_v_rad = FloatField(null=True)
    std_v_rad = FloatField(null=True)

    #> APOGEE DR17 DRP Metadata
    # Carried over from one of the coadded visits (it's a per-star crossmatch value, not
    # per-visit, so it's the same across all visits of a star). ASPCAP's
    # get_initial_arjl_guesses() reads these to seed its initial guess -- without them here,
    # running aspcap on this model raises AttributeError before any FERRE work happens.
    dr17_teff = FloatField(null=True)
    dr17_logg = FloatField(null=True)
    dr17_x_h = FloatField(null=True)
    dr17_vsini = FloatField(null=True)

    #> Spectral Data
    wavelength = PixelArray(
        accessor_class=LogLambdaArrayAccessor,
        accessor_kwargs=dict(
            crval=4.179,
            cdelt=6e-6,
            naxis=8575,
        ),
    )
    flux = PixelArray(accessor_class=ARJLCoaddedPixelArrayAccessor)
    ivar = PixelArray(accessor_class=ARJLCoaddedPixelArrayAccessor)
    pixel_flags = PixelArray(accessor_class=ARJLCoaddedPixelArrayAccessor)

    class Meta:
        indexes = (
            (
                (
                    "sdss_id",
                    "release",
                    "v_arjl",
                    "telescope",
                ),
                True,
            ),
        )


class ARJLTHCoaddedSpectrum(ARJLCoaddedSpectrum):
    """Coadd of `ARJLTHRestFrameVisitSpectrum` visits."""
    coadd_basename = "arjlStarCoadd_th.h5"


class ARJLDDCoaddedSpectrum(ARJLCoaddedSpectrum):
    """Coadd of `ARJLDDRestFrameVisitSpectrum` visits."""
    coadd_basename = "arjlStarCoadd_dd.h5"


class ARJLTHStarLinesCoaddedSpectrum(ARJLCoaddedSpectrum):
    """Coadd of `ARJLTHStarLinesRestFrameVisitSpectrum` visits."""
    coadd_basename = "arjlStarCoadd_th_starlines.h5"


class ARJLDDStarLinesCoaddedSpectrum(ARJLCoaddedSpectrum):
    """Coadd of `ARJLDDStarLinesRestFrameVisitSpectrum` visits."""
    coadd_basename = "arjlStarCoadd_dd_starlines.h5"
