# CLAM

CLAM (Continuum and Labels from Analytic Marginalization) fits stellar parameters from
APOGEE spectra using grid interpolation combined with a sinusoidal continuum model. It
operates in log-flux space and uses NMF-based spectral decomposition.

## What it does

CLAM determines:

- Effective temperature (`teff`)
- Surface gravity (`logg`)
- Overall metallicity (`m_h`)
- Carbon abundance (`c_m`)
- Nitrogen abundance (`n_m`)
- Microturbulent velocity (`v_micro`)
- Projected rotational velocity (`v_sini`)

## How it works

1. **Grid preparation**: A pre-computed grid of NMF spectral weights (`W`) and components
   (`H`) is loaded. The grid is parameterized by v_sini, [N/M], [C/M], [M/H], v_micro,
   log(g), and Teff (in log10 space for Teff).

2. **Continuum model**: The continuum is modeled using a sinusoidal design matrix with
   7 modes per APOGEE detector region. Three detector regions are used:
   - 15120--15820 A (green chip)
   - 15840--16440 A (red chip, blue half)
   - 16450--16960 A (red chip, red half)

   This gives 21 continuum coefficients total.

3. **Forward model**: The forward model combines:
   - Rectified flux from the NMF decomposition: `exp(W * (-H))`
   - Continuum from the sinusoidal basis: `exp(A_continuum * theta)`

   Everything is computed in log-flux space for numerical stability.

4. **Initial guess**: An iterative grid search steps through the parameter grid at
   progressively finer resolution to find a good starting point. The continuum
   coefficients are solved analytically (via LU decomposition) at each grid point
   while the NMF weights are evaluated from the grid.

5. **Optimization**: Starting from the initial guess, `scipy.optimize.curve_fit` is
   used to refine all parameters simultaneously (stellar labels + continuum coefficients).
   The Jacobian is computed analytically using the grid interpolator's derivative.

6. **Custom grid interpolator**: CLAM uses a custom `RegularGridInterpolator` that is
   approximately 5x faster than SciPy's implementation for slinear interpolation.

## Output fields

| Field | Description |
| --- | --- |
| `teff` | Effective temperature (K) |
| `e_teff` | Uncertainty on Teff |
| `logg` | Surface gravity (log cm/s^2) |
| `e_logg` | Uncertainty on log(g) |
| `m_h` | Overall metallicity [M/H] (dex) |
| `e_m_h` | Uncertainty on [M/H] |
| `n_m` | Nitrogen abundance [N/M] (dex) |
| `e_n_m` | Uncertainty on [N/M] |
| `c_m` | Carbon abundance [C/M] (dex) |
| `e_c_m` | Uncertainty on [C/M] |
| `v_micro` | Microturbulent velocity (km/s) |
| `e_v_micro` | Uncertainty on v_micro |
| `v_sini` | Projected rotational velocity (km/s) |
| `e_v_sini` | Uncertainty on v sin i |
| `initial_*` | Initial guess values for each label |
| `rchi2` | Reduced chi-squared of the fit |
| `result_flags` | Bitfield describing result quality |

## Flags

| Flag | Bit | Meaning |
| --- | --- | --- |
| `flag_spectrum_io_error` | 2^0 | Could not read the spectrum (I/O error) |
| `flag_runtime_error` | 2^1 | A runtime error occurred during fitting |

## Key caveats

- CLAM operates in log-flux space. Pixels with zero or negative flux are problematic
  and will be masked.
- The grid has finite boundaries in all dimensions. The optimizer is bounded by the
  grid limits, so results at the edges may be truncated.
- Model scatter (an empirical estimate of systematic model uncertainties) is added in
  quadrature to the observational uncertainties. The first and last 50 pixels of the
  APOGEE wavelength grid are masked by setting their model variance to infinity.
- The sinusoidal continuum model with 7 modes per region is flexible enough to capture
  typical APOGEE continuum shapes but may struggle with unusual continua (e.g., heavily
  reddened or emission-line objects).
- Uncertainties are derived from the covariance matrix of `curve_fit` and may
  underestimate the true uncertainties, particularly when the model is a poor fit.
- CLAM is under active development and some features (such as the initial guess routine)
  may evolve between versions.
