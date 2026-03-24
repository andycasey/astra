# SLAM

SLAM (Stellar Labels Machine) estimates stellar parameters for M dwarfs from BOSS
coadded spectra. It was developed by Bo Zhang and adapted for SDSS-V by Zach Way.

## What it does

SLAM determines the following stellar labels for M dwarf stars:

- Effective temperature (`teff`)
- Surface gravity (`logg`)
- Iron abundance (`fe_h`)
- Iron abundance from NIU calibration (`fe_h_niu`)
- Alpha-element abundance (`alpha_fe`)

## How it works

1. **Target selection**: Spectra are filtered by photometric and program criteria before
   fitting. Objects must satisfy at least one of:
   - **Magnitude cut**: Gaia `G - RP > 0.56` and absolute G-band magnitude > 5.553
     (with valid parallax).
   - **Program match**: The source is assigned to the `mwm_yso` or `mwm_snc` program.

   Objects failing both criteria are flagged and skipped.

2. **Spectral preparation**: Observed BOSS spectra are rebinned onto a standard
   wavelength grid used during SLAM training. For coadded spectra, no radial velocity
   correction is applied; for visit spectra, the XCSAO radial velocity is used to
   shift to the rest frame.

3. **Continuum normalization**: Spectra are block-normalized using the `laspec`
   normalization routines over the wavelength range 6002--8957 Angstroms with a
   polynomial approach (quantile 0.7).

4. **Label prediction**: Labels are predicted in two passes:
   - A fast initial prediction (`predict_labels_quick`) provides starting guesses.
   - A refined prediction (`predict_labels_multi`) uses the initial guesses to
     perform a full optimization, returning labels, covariance, and optimizer status.

5. **Post-processing flags**: After fitting, results are flagged if the effective
   temperature or metallicity falls outside the expected bounds, or if the optimizer
   reports a bad status.

## Output fields

| Field | Description |
| --- | --- |
| `teff` | Effective temperature (K) |
| `e_teff` | Uncertainty on effective temperature |
| `logg` | Surface gravity (log cm/s^2) |
| `e_logg` | Uncertainty on surface gravity |
| `fe_h` | Iron abundance [Fe/H] (dex) |
| `e_fe_h` | Uncertainty on [Fe/H] |
| `fe_h_niu` | Iron abundance from NIU calibration (dex) |
| `e_fe_h_niu` | Uncertainty on NIU [Fe/H] |
| `alpha_fe` | Alpha-element abundance [alpha/Fe] (dex) |
| `e_alpha_fe` | Uncertainty on [alpha/Fe] |
| `rho_*` | Correlation coefficients between pairs of labels |
| `initial_*` | Initial guesses for each label from the quick prediction |
| `success` | Whether the optimizer converged |
| `status` | Optimizer exit status code |
| `optimality` | Whether the solution satisfies optimality conditions |
| `chi2` | Chi-squared of the fit |
| `rchi2` | Reduced chi-squared of the fit |
| `result_flags` | Bitfield summarizing result quality |

### Correlation coefficients

The model provides pairwise correlation coefficients between the fitted labels
(e.g., `rho_teff_logg`, `rho_teff_fe_h`, etc.), which quantify covariances in
the label estimates.

## Flags

| Flag | Bit | Meaning |
| --- | --- | --- |
| `flag_bad_optimizer_status` | 2^0 | Optimizer returned a bad status (not 0 or 2) |
| `flag_teff_outside_bounds` | 2^1 | Teff is outside [2800, 4500] K |
| `flag_fe_h_outside_bounds` | 2^2 | [Fe/H] is outside [-1.0, +0.5] dex |
| `flag_outside_photometry_range` | 2^3 | Source photometry is outside expected M dwarf range |
| `flag_not_magnitude_cut` | 2^4 | Source fails the photometric magnitude cut |
| `flag_not_carton_match` | 2^5 | Source is not in the `mwm_yso` or `mwm_snc` programs |

### Composite flags

- **`flag_warn`**: Set if `flag_bad_optimizer_status` or `flag_outside_photometry_range` is set.
- **`flag_bad`**: Set if any of `flag_teff_outside_bounds`, `flag_fe_h_outside_bounds`,
  `flag_outside_photometry_range`, or `flag_bad_optimizer_status` is set.

## Key caveats

- SLAM is trained on M dwarf spectra. Applying it to stars outside the M dwarf parameter
  range (approximately 2800--4500 K) will produce unreliable results, which is indicated
  by `flag_teff_outside_bounds`.
- The photometric filtering criteria (Gaia color and absolute magnitude) are designed to
  select M dwarfs. Stars without valid Gaia parallax will only be analyzed if they belong
  to the `mwm_yso` or `mwm_snc` programs.
- The `fe_h_niu` label is an alternative metallicity calibration; both metallicity
  estimates are provided for comparison.
- Spectral data arrays (model flux, continuum) are stored in intermediate pickle files
  and can be accessed through the result object.
