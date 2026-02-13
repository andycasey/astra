from playhouse.hybrid import hybrid_property
from astra.fields import (FloatField, TextField, BitField)
from astra.models.pipeline import PipelineOutputMixin

class MDwarfType(PipelineOutputMixin):

    """M-dwarf type classifier."""

    #> M Dwarf Type
    spectral_type = TextField(null=True)
    sub_type = FloatField(null=True)
    rchi2 = FloatField(null=True)
    continuum = FloatField(null=True)
    result_flags = BitField(default=0)

    flag_suspicious = result_flags.flag(2**0, "Spectral type is K5.0, suspicious for M dwarf")
    flag_exception = result_flags.flag(2**1, "Runtime exception during processing")

    @hybrid_property
    def flag_bad(self):
        return (self.result_flags > 0)

    @flag_bad.expression
    def flag_bad(self):
        return (self.result_flags > 0)
