import datetime
from astra import __version__
from astra.fields import (AutoField, DateTimeField, FloatField, TextField, IntegerField, ForeignKeyField, BitField)
from astra.models.source import Source
from astra.models.spectrum import Spectrum
from astra.models.pipeline import PipelineOutputMixin
from astra.models.boss import has_bad_trace_tab
from playhouse.hybrid import hybrid_property

class Corv(PipelineOutputMixin):

    """A result from the `corv` pipeline."""

    #> Radial Velocity (corv)
    v_rad = FloatField(null=True)
    e_v_rad = FloatField(null=True)

    #> Stellar Parameters
    teff = FloatField(null=True)
    e_teff = FloatField(null=True)
    logg = FloatField(null=True)
    e_logg = FloatField(null=True)

    #> Initial values
    initial_teff = FloatField(null=True)
    initial_logg = FloatField(null=True)
    initial_v_rad = FloatField(null=True)

    #> Summary Statistics
    rchi2 = FloatField(null=True)
    result_flags = BitField(default=0)

    flag_not_mwm_wd = result_flags.flag(2**5, help_text="Object is not in the `mwm_wd` program")
    flag_no_wd_classification = result_flags.flag(2**6, help_text="No SnowWhite classification available")
    flag_not_da_type = result_flags.flag(2**7, help_text="Object is not classified as DA-type by SnowWhite")
    flag_bad_trace_tab = result_flags.flag(2**8, help_text="Input spectrum has DRP BadTraceTAB set: red/blue channel of different targets")

    @hybrid_property
    def flag_bad(self):
        return self.flag_bad_trace_tab

    @flag_bad.expression
    def flag_bad(self):
        return self.flag_bad_trace_tab

    @classmethod
    def from_spectrum(cls, spectrum, **kwargs):
        kwds = kwargs.copy()
        kwds["flag_bad_trace_tab"] = has_bad_trace_tab(spectrum)
        return super().from_spectrum(spectrum, **kwds)
