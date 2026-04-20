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
    flux = PixelArray(accessor_class=ARJLFluxAccessor)
    ivar = PixelArray(accessor_class=ARJLInverseVarianceAccessor)
    pixel_flags = PixelArray(accessor_class=ARJLPixelFlagsAccessor)


class ARJLTHVisitSpectrum(ARJLVisitSpectrum):
    pass

class ARJLDDVisitSpectrum(ARJLVisitSpectrum):
    pass



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


#class ARHJLTHCoaddedSpectrum()
