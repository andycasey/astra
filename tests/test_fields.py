
import os
os.environ["ASTRA_DATABASE_PATH"] = ":memory:"

import numpy as np


def test_help_text_inheritance_on_fields():
    from peewee import Model
    from astra.fields import FloatField
    from astra.glossary import Glossary

    dec_help_text = "Something I wrpte"

    class DummyModel(Model):
        ra = FloatField()
        some_field_that_is_not_in_glossary = FloatField()
        dec = FloatField(help_text=dec_help_text)

    assert DummyModel.ra.help_text == Glossary.ra
    assert DummyModel.some_field_that_is_not_in_glossary.help_text == None
    assert DummyModel.dec.help_text == dec_help_text


def test_help_text_inheritance_on_flags():

    from peewee import Model
    from astra.fields import BitField
    from astra.glossary import Glossary

    class DummyModel(Model):
        flags = BitField()
        flag_sdss4_apogee_faint = flags.flag()

    overwrite_help_text = "MOO"
    class DummyModel2(Model):
        flags = BitField()
        flag_sdss4_apogee_faint = flags.flag(help_text=overwrite_help_text)

    assert DummyModel2.flag_sdss4_apogee_faint.help_text == overwrite_help_text


def test_glossary_mixin_all_field_types():
    """All custom field types should inherit GlossaryFieldMixin behavior."""
    from peewee import Model
    from astra.fields import (
        IntegerField, FloatField, TextField, BooleanField,
        BigIntegerField, SmallIntegerField, DateTimeField, AutoField,
    )
    from astra.glossary import Glossary

    class FieldTypesModel(Model):
        teff = FloatField()
        snr = FloatField()
        mjd = IntegerField()
        release = TextField()

    # All of these are in the Glossary, so help_text should be auto-populated
    assert FieldTypesModel.teff.help_text == Glossary.teff
    assert FieldTypesModel.snr.help_text == Glossary.snr
    assert FieldTypesModel.mjd.help_text == Glossary.mjd
    assert FieldTypesModel.release.help_text == Glossary.release


def test_bitfield_flag_auto_increment():
    """BitField.flag() should auto-increment flag values as powers of 2."""
    from peewee import Model
    from astra.fields import BitField

    class FlagModel(Model):
        flags = BitField(default=0)
        flag_a = flags.flag()
        flag_b = flags.flag()
        flag_c = flags.flag()

    assert FlagModel.flag_a._value == 1
    assert FlagModel.flag_b._value == 2
    assert FlagModel.flag_c._value == 4


def test_bitfield_flag_explicit_value():
    """BitField.flag() should accept explicit values and continue from there."""
    from peewee import Model
    from astra.fields import BitField

    class FlagModel(Model):
        flags = BitField(default=0)
        flag_a = flags.flag(value=8)
        flag_b = flags.flag()  # should be 16 (8 << 1)

    assert FlagModel.flag_a._value == 8
    assert FlagModel.flag_b._value == 16


def test_bitfield_default_zero():
    """BitField should default to 0."""
    from peewee import Model
    from astra.fields import BitField

    class FlagModel(Model):
        flags = BitField()

    assert FlagModel.flags.default == 0


def test_base_pixel_array_accessor_init():
    """BasePixelArrayAccessor should store all init parameters."""
    from astra.fields import BasePixelArrayAccessor

    class DummyModel:
        pass

    class DummyField:
        pass

    accessor = BasePixelArrayAccessor(
        model=DummyModel,
        field=DummyField,
        name="flux",
        ext=1,
        column_name="FLUX",
        transform=lambda x: x * 2,
        help_text="Test flux",
    )

    assert accessor.model is DummyModel
    assert accessor.field is DummyField
    assert accessor.name == "flux"
    assert accessor.ext == 1
    assert accessor.column_name == "FLUX"
    assert accessor.help_text == "Test flux"
    assert accessor.transform is not None


def test_base_pixel_array_accessor_initialise():
    """_initialise_pixel_array should create __pixel_data__ if missing."""
    from astra.fields import BasePixelArrayAccessor

    accessor = BasePixelArrayAccessor(
        model=None, field=None, name="test", ext=None, column_name="test"
    )

    class Instance:
        pass

    inst = Instance()
    assert not hasattr(inst, "__pixel_data__")
    accessor._initialise_pixel_array(inst)
    assert hasattr(inst, "__pixel_data__")
    assert inst.__pixel_data__ == {}

    # Calling again should not reset existing data
    inst.__pixel_data__["key"] = "value"
    accessor._initialise_pixel_array(inst)
    assert inst.__pixel_data__["key"] == "value"


def test_base_pixel_array_accessor_set():
    """BasePixelArrayAccessor.__set__ should store values in __pixel_data__."""
    from astra.fields import BasePixelArrayAccessor

    accessor = BasePixelArrayAccessor(
        model=None, field=None, name="flux", ext=None, column_name="flux"
    )

    class Instance:
        pass

    inst = Instance()
    accessor.__set__(inst, np.array([1.0, 2.0, 3.0]))
    assert "flux" in inst.__pixel_data__
    np.testing.assert_array_equal(inst.__pixel_data__["flux"], [1.0, 2.0, 3.0])


def test_log_lambda_array_accessor_wavelength():
    """LogLambdaArrayAccessor should compute 10**(crval + cdelt * arange(naxis))."""
    from astra.fields import LogLambdaArrayAccessor

    crval = 3.5
    cdelt = 0.0001
    naxis = 100

    accessor = LogLambdaArrayAccessor(
        model=None,
        field=None,
        name="wavelength",
        ext=None,
        column_name="wavelength",
        crval=crval,
        cdelt=cdelt,
        naxis=naxis,
    )

    class Instance:
        pass

    inst = Instance()
    result = accessor.__get__(inst, type(inst))

    expected = 10 ** (crval + cdelt * np.arange(naxis))
    np.testing.assert_array_almost_equal(result, expected)
    assert len(result) == naxis


def test_log_lambda_array_accessor_caching():
    """LogLambdaArrayAccessor should cache the computed wavelength array."""
    from astra.fields import LogLambdaArrayAccessor

    accessor = LogLambdaArrayAccessor(
        model=None,
        field=None,
        name="wavelength",
        ext=None,
        column_name="wavelength",
        crval=3.5,
        cdelt=0.0001,
        naxis=10,
    )

    class Instance:
        pass

    inst = Instance()
    result1 = accessor.__get__(inst, type(inst))
    result2 = accessor.__get__(inst, type(inst))
    # Should be the exact same object (cached)
    assert result1 is result2


def test_log_lambda_array_accessor_returns_field_when_no_instance():
    """LogLambdaArrayAccessor should return the field when instance is None."""
    from astra.fields import LogLambdaArrayAccessor

    sentinel = object()
    accessor = LogLambdaArrayAccessor(
        model=None,
        field=sentinel,
        name="wavelength",
        ext=None,
        column_name="wavelength",
        crval=3.5,
        cdelt=0.0001,
        naxis=10,
    )

    result = accessor.__get__(None, None)
    assert result is sentinel


def test_pixel_array_accessor_fits_returns_field_when_no_instance():
    """PixelArrayAccessorFITS should return the field when instance is None."""
    from astra.fields import PixelArrayAccessorFITS

    sentinel = object()
    accessor = PixelArrayAccessorFITS(
        model=None, field=sentinel, name="flux", ext=1, column_name="FLUX"
    )
    result = accessor.__get__(None, None)
    assert result is sentinel


def test_pixel_array_accessor_hdf_returns_field_when_no_instance():
    """PixelArrayAccessorHDF should return the field when instance is None."""
    from astra.fields import PixelArrayAccessorHDF

    sentinel = object()
    accessor = PixelArrayAccessorHDF(
        model=None, field=sentinel, name="flux", ext=None, column_name="FLUX"
    )
    result = accessor.__get__(None, None)
    assert result is sentinel


def test_pickled_pixel_array_accessor_returns_field_when_no_instance():
    """PickledPixelArrayAccessor should return the field when instance is None."""
    from astra.fields import PickledPixelArrayAccessor

    sentinel = object()
    accessor = PickledPixelArrayAccessor(
        model=None, field=sentinel, name="flux", ext=None, column_name="flux"
    )
    result = accessor.__get__(None, None)
    assert result is sentinel
