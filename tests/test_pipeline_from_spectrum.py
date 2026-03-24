import os
os.environ["ASTRA_DATABASE_PATH"] = ":memory:"

import pytest
from astra.models.base import database


class TestPipelineOutputMixinFromSpectrum:

    def setup_method(self):
        from astra.models.source import Source
        from astra.models.spectrum import Spectrum
        from astra.models.pipeline import PipelineOutputMixin

        self.Source = Source
        self.Spectrum = Spectrum
        self.PipelineOutputMixin = PipelineOutputMixin

        database.create_tables([Source, Spectrum, PipelineOutputMixin])
        self.source_pk = Source.create().pk
        self.spectrum_pk = Spectrum.create().pk

    def _make_fake_spectrum(self, source_pk=None, spectrum_pk=None):
        class FakeSpectrum:
            pass
        s = FakeSpectrum()
        s.source_pk = source_pk or self.source_pk
        s.spectrum_pk = spectrum_pk or self.spectrum_pk
        return s

    def test_from_spectrum_basic(self):
        s = self._make_fake_spectrum()
        r = self.PipelineOutputMixin.from_spectrum(s)
        assert r.source_pk == self.source_pk
        assert r.spectrum_pk == self.spectrum_pk

    def test_from_spectrum_source_pk_mismatch_raises(self):
        s = self._make_fake_spectrum()
        with pytest.raises(ValueError, match="source_pk"):
            self.PipelineOutputMixin.from_spectrum(s, source_pk=99999)

    def test_from_spectrum_spectrum_pk_mismatch_raises(self):
        s = self._make_fake_spectrum()
        with pytest.raises(ValueError, match="spectrum_pk"):
            self.PipelineOutputMixin.from_spectrum(s, spectrum_pk=99999)

    def test_from_spectrum_matching_source_pk_ok(self):
        s = self._make_fake_spectrum()
        # Providing the same source_pk should not raise
        r = self.PipelineOutputMixin.from_spectrum(s, source_pk=self.source_pk)
        assert r.source_pk == self.source_pk

    def test_from_spectrum_matching_spectrum_pk_ok(self):
        s = self._make_fake_spectrum()
        r = self.PipelineOutputMixin.from_spectrum(s, spectrum_pk=self.spectrum_pk)
        assert r.spectrum_pk == self.spectrum_pk

    def test_from_spectrum_passes_extra_kwargs(self):
        """Extra kwargs should be forwarded to the model constructor."""
        s = self._make_fake_spectrum()
        r = self.PipelineOutputMixin.from_spectrum(s, tag="test_tag")
        assert r.tag == "test_tag"
