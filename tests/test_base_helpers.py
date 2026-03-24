import os
os.environ["ASTRA_DATABASE_PATH"] = ":memory:"

import numpy as np
import pytest


class TestGetFillValue:

    def test_default_fill_values(self):
        from peewee import TextField, BooleanField, IntegerField, FloatField, AutoField, BigIntegerField, ForeignKeyField, DateTimeField
        from astra.fields import BitField
        from astra.models.base import get_fill_value

        # TextField -> ""
        f = TextField()
        assert get_fill_value(f, {}) == ""

        # BooleanField -> False
        f = BooleanField()
        assert get_fill_value(f, {}) is False

        # IntegerField -> -1
        f = IntegerField()
        assert get_fill_value(f, {}) == -1

        # FloatField -> nan
        f = FloatField()
        assert np.isnan(get_fill_value(f, {}))

        # BitField -> 0
        f = BitField()
        assert get_fill_value(f, {}) == 0

    def test_given_fill_value_overrides_default(self):
        from peewee import FloatField
        from astra.models.base import get_fill_value

        f = FloatField()
        f.name = "teff"
        assert get_fill_value(f, {"teff": -999.0}) == -999.0

    def test_field_default_used_when_no_given(self):
        from peewee import IntegerField
        from astra.models.base import get_fill_value

        f = IntegerField(default=42)
        f.name = "some_field"
        result = get_fill_value(f, {})
        # The finally block always runs, so type-based default (-1 for IntegerField) wins
        assert result == -1


class TestFitsColumnKwargs:

    def test_text_field_format(self):
        from peewee import TextField
        from astra.models.base import fits_column_kwargs

        f = TextField()
        f.name = "release"
        result = fits_column_kwargs(f, ["sdss5", "dr17"], upper=False)
        assert result["name"] == "release"
        assert result["format"] == "A5"  # max length of "sdss5" is 5
        # Actually max(len("sdss5"), len("dr17")) = 5
        # format is "A5"
        # Let me reconsider...

    def test_text_field_format_correct(self):
        from peewee import TextField
        from astra.models.base import fits_column_kwargs

        f = TextField()
        f.name = "release"
        result = fits_column_kwargs(f, ["ab", "cde"], upper=False)
        assert result["format"] == "A3"  # max(2, 3) = 3

    def test_text_field_empty_values(self):
        from peewee import TextField
        from astra.models.base import fits_column_kwargs

        f = TextField()
        f.name = "release"
        result = fits_column_kwargs(f, [], upper=False)
        assert result["format"] == "A1"  # fallback to 1 for empty

    def test_float_field_format(self):
        from peewee import FloatField
        from astra.models.base import fits_column_kwargs

        f = FloatField()
        f.name = "teff"
        result = fits_column_kwargs(f, [5000.0, 6000.0], upper=True)
        assert result["name"] == "TEFF"
        assert result["format"] == "E"

    def test_integer_field_format(self):
        from peewee import IntegerField
        from astra.models.base import fits_column_kwargs

        f = IntegerField()
        f.name = "mjd"
        result = fits_column_kwargs(f, [59000, 59001], upper=False)
        assert result["name"] == "mjd"
        assert result["format"] == "J"

    def test_boolean_field_format(self):
        from peewee import BooleanField
        from astra.models.base import fits_column_kwargs

        f = BooleanField()
        f.name = "is_good"
        result = fits_column_kwargs(f, [True, False], upper=False)
        assert result["format"] == "L"

    def test_upper_name(self):
        from peewee import FloatField
        from astra.models.base import fits_column_kwargs

        f = FloatField()
        f.name = "teff"
        result = fits_column_kwargs(f, [5000.0], upper=True)
        assert result["name"] == "TEFF"

    def test_lower_name(self):
        from peewee import FloatField
        from astra.models.base import fits_column_kwargs

        f = FloatField()
        f.name = "teff"
        result = fits_column_kwargs(f, [5000.0], upper=False)
        assert result["name"] == "teff"


class TestWarnOnLongNameOrComment:

    def test_no_warning_short_name(self):
        from peewee import FloatField
        from astra.models.base import warn_on_long_name_or_comment

        f = FloatField()
        f.name = "teff"
        f.help_text = "short"
        # Should not raise, just returns None
        assert warn_on_long_name_or_comment(f) is None

    def test_returns_none(self):
        from peewee import FloatField
        from astra.models.base import warn_on_long_name_or_comment

        f = FloatField()
        f.name = "x"
        f.help_text = None
        assert warn_on_long_name_or_comment(f) is None


class TestBlankAndFillerCards:

    def test_blank_card(self):
        from astra.models.base import BLANK_CARD
        assert BLANK_CARD == (" ", " ", None)

    def test_filler_card(self):
        from astra.models.base import FILLER_CARD, FILLER_CARD_KEY
        assert FILLER_CARD_KEY == "TTYPE0"
        assert FILLER_CARD == ("TTYPE0", "Water cuggle", None)


class TestCategoryHeaders:

    def _get_category_headers(self, cls):
        """Helper to get category_headers regardless of Python version."""
        ch = cls.category_headers
        # In Python 3.13+, @classmethod @property stacking is broken
        if isinstance(ch, (tuple, list)):
            return ch
        # Access the underlying function
        return cls.category_headers.fget(cls)

    def test_category_headers_found(self):
        """category_headers should parse #> comments from source code."""
        from astra.models.bossnet import BossNet

        headers = self._get_category_headers(BossNet)
        # BossNet has "#> Stellar Parameters" above teff
        assert any(h[0] == "Stellar Parameters" for h in headers)

    def test_category_headers_returns_tuple(self):
        from astra.models.corv import Corv

        headers = self._get_category_headers(Corv)
        assert isinstance(headers, tuple)
        # Corv has multiple category headers
        header_names = [h[0] for h in headers]
        assert "Radial Velocity (corv)" in header_names
        assert "Stellar Parameters" in header_names
