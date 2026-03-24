# Grok

Grok estimates stellar parameters and projected rotational velocity (v sin i) from
high-resolution APOGEE spectra using grid-based fitting. It operates on rest-frame
coadded APOGEE spectra.

## What it does

Grok determines:

- Effective temperature (`teff`)
- Surface gravity (`logg`)
- Overall metallicity (`m_h`)
- Microturbulent velocity (`v_micro`)
- Projected rotational velocity (`v_sini`)
- Carbon abundance (`c_m`) and nitrogen abundance (`n_m`), when not using a subset grid

Results are provided at three levels of refinement: coarse grid search, grid-slice
interpolation, and quadratic interpolation along marginalized chi-squared profiles.

## How it works

1. **Grid loading**: A pre-computed grid of synthetic APOGEE spectra is loaded from an
   HDF5 file. The grid spans a multi-dimensional parameter space (Teff, logg, [M/H],
   v_micro, v_sini, and optionally [C/M] and [N/M]). By default, a subset grid is used
   with [C/M] = [N/M] = 0 and v_micro = 1.0 km/s.

2. **Rotational broadening**: The grid is convolved with rotational broadening kernels
   for a sequence of v_sini values (default: 0, 5, 7.5, 10, 20, 30, 50, 75, 100 km/s)
   using a limb-darkening coefficient of 0.6.

3. **Error inflation**: Flux uncertainties are inflated around significant skylines and
   spikes, and bad pixels are masked. This preprocessing step is similar to what ASPCAP
   applies.

4. **NMF-assisted continuum**: By default, the NMF rectified model flux from the
   NMF Rectify pipeline is used to assist in continuum determination.

5. **Coarse grid search**: The best-matching grid node is found by evaluating chi-squared
   at grid points using a hierarchical refinement strategy (default: 3 refinement levels).
   The continuum is estimated as the ratio of the observed to model flux at each grid point.

6. **Refined estimates**: Two additional estimates are produced:
   - **Slice interpolation** (`slice_*`): For each parameter, the chi-squared profile
     is extracted along a 1D slice through the best grid node, and a quadratic is fit
     to the three points nearest the minimum.
   - **Quadratic marginalization** (`quad_*`): For each parameter, the chi-squared is
     minimized over all other dimensions, and a quadratic is fit to the resulting 1D
     profile.

7. **Julia backend**: The core grid operations (loading, convolution, node search) are
   implemented in Julia via `juliacall` for performance.

## Output fields

| Field | Description |
| --- | --- |
| `coarse_teff` | Teff from the coarse grid search (K) |
| `coarse_logg` | log(g) from the coarse grid search |
| `coarse_m_h` | [M/H] from the coarse grid search |
| `coarse_v_micro` | Microturbulence from the coarse grid search (km/s) |
| `coarse_v_sini` | v sin i from the coarse grid search (km/s) |
| `coarse_chi2` | Chi-squared at the best coarse grid node |
| `node_*` | Grid node values at the best-fit position |
| `slice_*` | Quadratic-interpolated values from 1D slices through the best node |
| `quad_*` | Quadratic-interpolated values from marginalized chi-squared profiles |

## Key caveats

- Grok requires Julia and the `juliacall` Python package. The Julia code is loaded at
  runtime from the `Grok.jl` file in the pipeline directory.
- The default subset grid fixes [C/M] = [N/M] = 0 and v_micro = 1.0 km/s. If you need
  carbon, nitrogen, or microturbulence as free parameters, set `use_subset=False`.
- The grid has finite boundaries. Parameters at or near grid edges may be unreliable.
- The `coarse_*` fields report the nearest grid node; the `slice_*` and `quad_*` fields
  provide interpolated values that may lie between grid nodes.
- Results depend on the quality of the NMF continuum rectification when `use_nmf_flux=True`
  (the default). Poor NMF fits can propagate into biased stellar parameters.
- The v_sini measurement is discretized to the set of broadening values provided. The
  quadratic interpolation between these values provides a finer estimate but is still
  limited by the spacing of the broadening grid.
- A separate `measure_vsini` task is available that uses a CLAM-based approach to estimate
  v_sini independently.
