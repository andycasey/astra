# NMF Rectify

NMF Rectify performs continuum normalization of stellar spectra using Non-negative Matrix
Factorization (NMF). It provides a data-driven continuum estimate that is used by
downstream pipelines.

## What it does

NMF Rectify separates the observed spectrum into a continuum component and a
rectified (continuum-normalized) component. It produces:

- NMF coefficients (`log10_W`) that describe the spectral features
- Continuum coefficients (`continuum_theta`) that describe the smooth continuum shape
- Goodness-of-fit statistics

## How it works

The continuum model has two components:

1. **NMF spectral component**: A set of pre-trained NMF basis vectors (components)
   capture the spectral line features. The NMF coefficients `W` are non-negative weights
   on these components. The rectified model spectrum is `1 - W * components`.

2. **Sinusoidal continuum**: The smooth continuum shape is modeled as a sum of sinusoidal
   functions with a specified length scale (`L`) and degree (`deg`). This flexible
   functional form can represent slowly-varying continuum shapes without overfitting
   spectral lines.

The pipeline fits both components simultaneously, optimizing the NMF weights and continuum
coefficients to minimize the residuals between the model and the observed flux.

### BOSS spectra

For BOSS spectra, the pipeline operates per-source: all visit spectra for a given source
are fit jointly. Visit spectra are shifted to the rest frame using the XCSAO radial
velocity, resampled onto a common wavelength grid, and fit with shared NMF coefficients
but per-visit continuum coefficients. Visits are filtered to require SNR > 3, adequate
cross-correlation quality (XCSAO R > 6, unless the source is in the `mwm_wd` program),
and no DRP warning flags.

### APOGEE spectra

For APOGEE coadded spectra, each spectrum is fit individually.

### Initial guess

The initial guess for the NMF coefficients is obtained using a "small W" heuristic
(flagged as `flag_initialised_from_small_w`), which provides a conservative starting
point for the optimization.

## Output fields

| Field | Description |
| --- | --- |
| `log10_W` | Log10 of the NMF coefficients (array) |
| `continuum_theta` | Continuum model coefficients (array) |
| `L` | Sinusoidal length scale used for the continuum model |
| `deg` | Sinusoidal degree used for the continuum model |
| `rchi2` | Reduced chi-squared for this spectrum |
| `joint_rchi2` | Joint reduced chi-squared from the simultaneous fit (BOSS: across all visits for the source) |
| `nmf_flags` | Bitfield describing processing flags |

## Flags

The `nmf_flags` bitfield uses the following bits:

| Flag | Bit | Meaning |
| --- | --- | --- |
| `flag_initialised_from_small_w` | 2^0 | NMF coefficients were initialized using the small-W heuristic |
| `flag_could_not_read_spectrum` | 2^3 | The spectrum could not be read (I/O error) |
| `flag_runtime_exception` | 2^4 | A runtime exception occurred during fitting |

## Key caveats

- NMF Rectify is a preprocessing step, not a science pipeline. Its outputs (continuum
  and rectified spectra) are consumed by downstream pipelines such as SLAM and Grok.
- The NMF components are pre-trained and fixed. They capture typical stellar spectral
  features but may not accurately represent unusual or exotic spectra.
- For BOSS spectra, the joint fit across visits means that the NMF coefficients are
  shared. If one visit has a very different spectral shape (e.g., due to variability),
  this can affect the fit quality for all visits.
- The `rchi2` is computed per spectrum, while `joint_rchi2` reflects the fit quality
  across all spectra in the joint fit.
