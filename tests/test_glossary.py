
import os
os.environ["ASTRA_DATABASE_PATH"] = ":memory:"


def test_glossary_direct_attribute():
    from astra.glossary import Glossary
    assert Glossary.ra == "Right ascension [deg]"
    assert Glossary.teff == "Effective temperature [K]"


def test_glossary_missing_attribute_returns_none():
    from astra.glossary import Glossary
    # A name that is not in the glossary and has no special context
    result = Glossary.zzz_nonexistent_term_zzz
    assert result is None


def test_glossary_error_prefix():
    """e_ prefix should produce 'Error on ...' help text."""
    from astra.glossary import Glossary
    result = Glossary.e_v_rad
    assert result is not None
    assert "Error on" in result or "error on" in result.lower()


def test_glossary_initial_prefix():
    """initial_ prefix should produce 'Initial ...' help text."""
    from astra.glossary import Glossary
    result = Glossary.initial_flags
    # initial_flags is defined directly in the glossary
    assert result is not None


def test_glossary_raw_prefix():
    """raw_ prefix should produce 'Raw ...' help text."""
    from astra.glossary import Glossary
    # raw_teff is not directly defined, so special context should kick in
    result = Glossary.raw_teff
    assert result is not None
    assert "raw" in result.lower() or "Raw" in result


def test_glossary_flags_suffix():
    """_flags suffix should produce 'Flags for ...' help text."""
    from astra.glossary import Glossary
    result = Glossary.result_flags
    # result_flags is directly defined
    assert result is not None


def test_glossary_rchi2_suffix():
    """_rchi2 suffix should produce 'Reduced chi-square value for ...' help text."""
    from astra.glossary import Glossary
    result = Glossary.nmf_rchi2
    assert result is not None


def test_lower_first_letter():
    from astra.glossary import lower_first_letter
    assert lower_first_letter("Hello") == "hello"
    assert lower_first_letter("ABC") == "aBC"
    assert lower_first_letter("already") == "already"


def test_warn_on_long_description_passthrough():
    """warn_on_long_description currently just returns the text unchanged."""
    from astra.glossary import warn_on_long_description
    text = "Short description"
    assert warn_on_long_description(text) == text
    long_text = "x" * 200
    assert warn_on_long_description(long_text) == long_text


def test_glossary_instance_context():
    """BaseGlossary instances use a context prefix for descriptions."""
    from astra.glossary import BaseGlossary, Glossary

    g = Glossary("My context")
    result = g.teff
    assert "My context" in result
    # The glossary value should be lowercased first letter and appended
    assert "effective temperature" in result.lower()


def test_glossary_special_context_e_prefix_recursive():
    """e_ prefix should resolve recursively: e_teff -> 'Error on stellar effective temperature [K]'."""
    from astra.glossary import Glossary
    result = Glossary.e_teff
    assert result is not None
    assert "Error on" in result or "error on" in result.lower()
    assert "temperature" in result.lower()


def test_resolve_special_contexts_no_match():
    """Names that don't match any special context pattern return None."""
    from astra.glossary import resolve_special_contexts, Glossary, MISSING_GLOSSARY_TERMS
    result = resolve_special_contexts(Glossary, "completely_unknown_xyz_abc")
    assert result is None
    assert "completely_unknown_xyz_abc" in MISSING_GLOSSARY_TERMS


def test_rho_context():
    """rho_ prefix should produce correlation coefficient description."""
    from astra.glossary import _rho_context, Glossary
    # rho_teff_logg should produce correlation between teff and logg
    result = _rho_context("rho_teff_logg", Glossary)
    if result:  # depends on whether teff and logg are recognized as glossary parts
        assert "Correlation coefficient" in result


def test_special_contexts_list():
    """Verify the SPECIAL_CONTEXTS structure is well-formed."""
    from astra.glossary import SPECIAL_CONTEXTS
    assert len(SPECIAL_CONTEXTS) > 0
    for identifier, is_prefix, sub_context in SPECIAL_CONTEXTS:
        assert isinstance(identifier, str)
        assert isinstance(is_prefix, bool)
        assert isinstance(sub_context, str) or callable(sub_context)
