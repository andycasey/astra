# MDwarfType

MDwarfType assigns spectral types to M dwarf stars by matching observed BOSS spectra
against a library of spectral templates.

## What it does

MDwarfType determines:

- Spectral type (e.g., "M0.0", "M3.5", "K7.0")
- Numerical sub-type
- A goodness-of-fit statistic

## How it works

1. **Template preparation**: A library of M dwarf spectral templates (covering types
   K5 through late M) is loaded and resampled onto the standard BOSS wavelength grid.
   Each template is rectified by fitting and dividing out a quadratic continuum, restricted
   to the wavelength range 5000--8800 Angstroms.

2. **Continuum normalization**: The observed spectrum is normalized by its mean flux in a
   narrow window around 7500 Angstroms (7495--7505 A), then rectified with the same
   quadratic procedure used for the templates.

3. **Template matching**: The chi-squared statistic is computed between the rectified
   observed spectrum and each rectified template, weighted by the inverse variance. The
   template with the lowest chi-squared is selected as the best match.

4. **Result**: The spectral type and sub-type from the best-matching template are reported,
   along with the reduced chi-squared of the match.

5. **Parallelism**: Spectra are processed in batches using a process pool.

## Output fields

| Field | Description |
| --- | --- |
| `spectral_type` | Best-matching spectral type string (e.g., "M3.5") |
| `sub_type` | Numerical sub-type (e.g., 3.5) |
| `rchi2` | Reduced chi-squared of the best template match |
| `continuum` | Mean continuum level (flux in the 7500 A normalization window) |
| `result_flags` | Bitfield summarizing result quality |

## Flags

| Flag | Bit | Meaning |
| --- | --- | --- |
| `flag_suspicious` | 2^0 | Best-matching spectral type is K5.0, which is suspicious for an M dwarf target |
| `flag_exception` | 2^1 | A runtime exception occurred during processing |

### Composite flags

- **`flag_bad`**: Set if any flag bit is nonzero (i.e., `result_flags > 0`).

## Key caveats

- MDwarfType is a template-matching classifier, not a physical model fit. The spectral
  type reflects the best match from a discrete set of templates.
- If the best match is K5.0, the `flag_suspicious` flag is raised because K5 is at the
  boundary of the M dwarf regime and may indicate a misclassified target.
- The template library and wavelength coverage (5000--8800 A) are optimized for BOSS
  optical spectra. The method relies on features in the red optical.
- The quadratic rectification removes the broad-band spectral shape, so the classification
  is driven by molecular band strengths and other spectral features rather than the overall
  continuum slope.
