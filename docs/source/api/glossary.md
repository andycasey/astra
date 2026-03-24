# Glossary System

The glossary system (`astra.glossary`) provides centralized, human-readable descriptions for database fields.
It is used automatically by `GlossaryFieldMixin` to populate `help_text` on Peewee fields.

## `Glossary`

```python
class Glossary(BaseGlossary, metaclass=GlossaryType)
```

A class whose attributes are field-name-to-description mappings. Access any attribute
to get its description string.

```python
from astra.glossary import Glossary

Glossary.teff
# "Effective temperature [K]"

Glossary.logg
# "Surface gravity [dex]"

Glossary.sdss_id
# "SDSS unique source identifier"
```

## Special prefix/suffix resolution

If a field name is not defined directly, the glossary resolves it by checking for
known prefixes and suffixes:

| Pattern | Rule | Example |
|---|---|---|
| `e_X` | "Error on" + description of `X` | `Glossary.e_teff` -> `"Error on effective temperature [K]"` |
| `X_flags` | "Flags for" + description of `X` | `Glossary.teff_flags` -> `"Flags for effective temperature [K]"` |
| `initial_X` | "Initial" + description of `X` | `Glossary.initial_teff` -> `"Initial effective temperature [K]"` |
| `X_rchi2` | "Reduced chi-square value for" + description of `X` | `Glossary.teff_rchi2` -> `"Reduced chi-square value for effective temperature [K]"` |
| `raw_X` | "Raw" + description of `X` | `Glossary.raw_teff` -> `"Raw effective temperature [K]"` |
| `rho_X_Y` | "Correlation coefficient between X and Y" | `Glossary.rho_teff_logg` -> `"Correlation coefficient between TEFF and LOGG"` |

## Context usage

The `Glossary` can be instantiated with a context string to prepend to all descriptions:

```python
from astra.glossary import Glossary

g = Glossary("SLAM")
g.teff
# "SLAM effective temperature [K]"
```

## How fields use the glossary

When a field (e.g. `FloatField`, `BitField`) is bound to a model and no `help_text` is
provided, the `GlossaryFieldMixin.bind()` method automatically sets:

```python
self.help_text = getattr(Glossary, self.name, None)
```

This means naming a model field `teff` will automatically give it the help text
`"Effective temperature [K]"` without any manual annotation.
