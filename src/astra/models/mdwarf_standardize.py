from astra.fields import (
    BitField,
    ArrayField,
    FloatField,
    TextField,
    IntegerField,
    PixelArray, BitField, LogLambdaArrayAccessor,
    BasePixelArrayAccessor
)
from astra.models.pipeline import PipelineOutputMixin


class MDwarfStandardize(PipelineOutputMixin):
    """
    A result from the mdwarf_standardize pipeline
    """
    pseudo_continuum =  PixelArray()

    result_flags = BitField(default=0)
    flag_not_dwarf = result_flags.flag(2**0)
    flag_cont_fail = result_flags.flag(2**1)
