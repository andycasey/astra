import os
os.environ["ASTRA_DATABASE_PATH"] = ":memory:"

import pytest


class TestSnowWhiteIntermediateOutputPath:

    def test_intermediate_output_path_formatting(self):
        from astra.models.snow_white import SnowWhite
        from astra import __version__

        r = SnowWhite()
        r.spectrum_pk = 12345

        path = r.intermediate_output_path
        # spectrum_pk = 12345
        # str(12345)[-4:-2] = "23", str(12345)[-2:] = "45"
        assert path == f"$MWM_ASTRA/{__version__}/pipelines/snow_white/23/45/12345.fits"

    def test_intermediate_output_path_short_pk(self):
        from astra.models.snow_white import SnowWhite
        from astra import __version__

        r = SnowWhite()
        r.spectrum_pk = 7

        path = r.intermediate_output_path
        # str(7) = "7", "7"[-4:-2] = "", "7"[-2:] = "7"
        # format spec :0>2 pads to 2 chars: "" -> "00", "7" -> "07"
        assert path == f"$MWM_ASTRA/{__version__}/pipelines/snow_white/00/07/7.fits"

    def test_intermediate_output_path_two_digit_pk(self):
        from astra.models.snow_white import SnowWhite
        from astra import __version__

        r = SnowWhite()
        r.spectrum_pk = 42

        path = r.intermediate_output_path
        # str(42)[-4:-2] = "", str(42)[-2:] = "42"
        assert path == f"$MWM_ASTRA/{__version__}/pipelines/snow_white/00/42/42.fits"


class TestTheCannonIntermediateOutputPath:

    def test_intermediate_output_path(self):
        from astra.models.the_cannon import TheCannon

        r = TheCannon()
        r.source_pk = 98765
        r.spectrum_pk = 11111
        r.v_astra = 12345

        path = r.intermediate_output_path
        # source_pk = 98765: "98765"[-4:-2] = "87", "98765"[-2:] = "65"
        assert path == "$MWM_ASTRA/12345/pipelines/TheCannon/87/65/98765-11111.pkl"


class TestThePayneIntermediateOutputPath:

    def test_intermediate_output_path(self):
        from astra.models.the_payne import ThePayne

        r = ThePayne()
        r.spectrum_pk = 56789
        r.v_astra = 99

        path = r.intermediate_output_path
        # spectrum_pk = 56789: "56789"[-4:-2] = "67", "56789"[-2:] = "89"
        assert path == "$MWM_ASTRA/99/pipelines/ThePayne/intermediate/67/89/56789.pkl"


class TestSlamIntermediateOutputPath:

    def test_intermediate_output_path(self):
        from astra.models.slam import Slam

        r = Slam()
        r.spectrum_pk = 34567
        r.v_astra = 100

        path = r.intermediate_output_path
        # spectrum_pk = 34567: "34567"[-4:-2] = "45", "34567"[-2:] = "67"
        assert path == "$MWM_ASTRA/100/pipelines/slam/45/67/34567.pkl"


class TestMWMPathMixins:

    def test_mwm_star_path(self):
        from astra.models.mwm import MWMStarMixin

        class FakeStar(MWMStarMixin):
            class Meta:
                table_name = "fake_star"

        r = FakeStar()
        r.sdss_id = 12345
        r.v_astra = 100

        path = r.path
        # n = "12345", n[-4:-2] = "23", n[-2:] = "45"
        assert path == "$MWM_ASTRA/100/spectra/star/23/45/mwmStar-100-12345.fits"

    def test_mwm_star_path_short_id(self):
        from astra.models.mwm import MWMStarMixin

        class FakeStar(MWMStarMixin):
            class Meta:
                table_name = "fake_star2"

        r = FakeStar()
        r.sdss_id = 5
        r.v_astra = 1

        path = r.path
        # n = f"{5:0>4.0f}" = "0005", n[-4:-2] = "00", n[-2:] = "05"
        assert path == "$MWM_ASTRA/1/spectra/star/00/05/mwmStar-1-5.fits"

    def test_mwm_visit_path(self):
        from astra.models.mwm import MWMVisitMixin

        class FakeVisit(MWMVisitMixin):
            class Meta:
                table_name = "fake_visit"

        r = FakeVisit()
        r.sdss_id = 98765
        r.v_astra = 200

        path = r.path
        # n = "98765", n[-4:-2] = "87", n[-2:] = "65"
        assert path == "$MWM_ASTRA/200/spectra/visit/87/65/mwmVisit-200-98765.fits"

    def test_mwm_ext_lambdas(self):
        from astra.models.mwm import get_boss_ext, get_apogee_ext

        class FakeInstance:
            pass

        apo25m = FakeInstance()
        apo25m.telescope = "apo25m"

        lco25m = FakeInstance()
        lco25m.telescope = "lco25m"

        apo1m = FakeInstance()
        apo1m.telescope = "apo1m"

        assert get_boss_ext(apo25m) == 1
        assert get_boss_ext(lco25m) == 2
        assert get_apogee_ext(apo25m) == 3
        assert get_apogee_ext(lco25m) == 4
        assert get_apogee_ext(apo1m) == 3
