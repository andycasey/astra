# Snow White

Snow White is the white dwarf analysis pipeline in Astra. It classifies white dwarf
spectra into spectral subtypes and fits effective temperature (Teff) and surface gravity
(log g) for DA-type white dwarfs using an emulator-based Balmer line fitting method.

Snow White runs on BOSS combined spectra for sources in the `mwm_wd` carton program.

## Overview

The pipeline has two stages:

1. **Classification** -- A random forest classifier assigns a white dwarf spectral type
   (DA, DB, DC, DQ, DZ, and many subtypes) based on spectral line features.
2. **DA fitting** -- If the source is classified as DA (or DA:), the pipeline fits
   Teff and log g by fitting Balmer absorption lines with a PCA-based spectral emulator.

## Classification

Snow White extracts a set of spectral line features from the input spectrum and feeds
them into a pre-trained random forest classifier. The feature extraction
(`get_line_info_v3.line_info`) works as follows:

1. The spectrum is scaled so that the total flux sums to the number of pixels (a
   simple normalization).
2. The flux is binned by pairs of pixels and interpolated onto a standard wavelength
   grid (3850--8300 Angstroms, 1 Angstrom spacing).
3. A pseudo-continuum is estimated using a spline fit through predefined anchor points.
4. For each white dwarf subtype (DA, DB, DQ, DZ, WD+MS, peculiar, hot DQ), the ratio
   of the observed flux to the pseudo-continuum (or to a blackbody fit for WD+MS and
   peculiar types) is computed in diagnostic wavelength windows defined by feature files.
5. H-alpha emission features are measured separately.
6. All features are concatenated into a single feature vector.

The random forest outputs class probabilities for 24 white dwarf subtypes (see
[Output fields](#output-fields) below). The classification is assigned as:

- The most probable class, if its probability is at least 0.5.
- A dual classification (e.g., `DA/DB`) if the second-most-probable class has a
  probability ratio greater than 0.6 relative to the top class.
- An uncertain classification (e.g., `DA:`) otherwise.

## DA fitting

For spectra classified as DA or DA:, Snow White fits Teff and log g by fitting the
profiles of the Balmer hydrogen absorption lines.

### Spectral emulator

The fitting uses a PCA-based spectral emulator (`Emulator_DA`) trained on a grid of
DA white dwarf model atmospheres. The emulator performs PCA decomposition on the model
grid and uses linear interpolation in (log10(Teff), log g) space to predict PCA weights
at arbitrary parameter values. This allows rapid generation of model spectra without
repeatedly reading from the full grid.

### Fitting procedure

1. **Grid search** -- The normalized spectrum is compared against a pre-computed grid of
   DA models (`da_flux_cube.npy`) to find a coarse best-fit Teff. If a photometric
   Teff and log g solution exists in a reference table (matched by Gaia DR3 source ID),
   that is used as the starting point instead.

2. **Line selection** -- Different sets of Balmer lines are used depending on the
   temperature regime:
   - Hot (Teff > 40,000 K): a dedicated hot line list
   - Warm (16,000--40,000 K): the standard line list
   - Cool (8,000--16,000 K): a cool line list
   - Very cool (Teff < 8,000 K): a very-cool line list

3. **Balmer line fitting** -- Each Balmer line is individually normalized by fitting a
   linear pseudo-continuum to the line wings. The emulator generates a model spectrum
   at trial (Teff, log g, wavelength shift) values, the model lines are normalized the
   same way, and residuals are computed. The fit uses `lmfit.minimize` with
   `least_squares` (soft L1 loss) to find the best parameters. The parameter bounds are:
   - Teff: 4,000--120,000 K
   - log g: 6.01--9.49 dex (internally stored as 601--949, divided by 100)
   - Wavelength shift: -80 to +80 Angstroms

4. **Uncertainty estimation** -- After convergence, the fit is repeated using
   `leastsq` (Levenberg-Marquardt) starting from the best-fit values to estimate
   formal parameter uncertainties. A noise model is applied to the formal uncertainties
   to produce the reported values:
   - `e_teff = 1.5 * raw_e_teff + 100` K
   - `e_logg = 2 * raw_e_logg + 0.05` dex

### Hot/cold solution disambiguation

Because the strength of hydrogen Balmer lines peaks near Teff ~ 13,000 K, a given
set of line profiles can be consistent with both a hotter and a cooler temperature
solution. Snow White handles this ambiguity as follows:

- If a photometric starting point was available (from the Gaia-matched reference table),
  only the single fit from that starting point is used.
- Otherwise, the pipeline fits the spectrum twice: once starting from the best grid
  temperature, and once starting from the opposite side of the 13,000 K Balmer maximum.
  The correct solution is chosen by comparing the predicted Gaia BP-RP color from each
  model against the observed BP-RP color. The solution whose synthetic color is closest
  to the observed value is adopted.
- If the observed Gaia BP and RP magnitudes are not available, the pipeline cannot
  disambiguate and sets `flag_missing_bp_rp_mag`.

## Output fields

### Classification probabilities

| Field | Description |
| --- | --- |
| `classification` | Assigned spectral type string (e.g., `DA`, `DB`, `DA/DB`, `DA:`) |
| `p_cv` | Probability of CV (cataclysmic variable) |
| `p_da` | Probability of DA |
| `p_dab` | Probability of DAB |
| `p_dabz` | Probability of DABZ |
| `p_dah` | Probability of DAH (magnetic DA) |
| `p_dahe` | Probability of DAHe |
| `p_dao` | Probability of DAO |
| `p_daz` | Probability of DAZ |
| `p_da_ms` | Probability of DA+MS (DA with main-sequence companion) |
| `p_db` | Probability of DB |
| `p_dba` | Probability of DBA |
| `p_dbaz` | Probability of DBAZ |
| `p_dbh` | Probability of DBH |
| `p_dbz` | Probability of DBZ |
| `p_db_ms` | Probability of DB+MS (DB with main-sequence companion) |
| `p_dc` | Probability of DC (featureless) |
| `p_dc_ms` | Probability of DC+MS |
| `p_do` | Probability of DO |
| `p_dq` | Probability of DQ |
| `p_dqz` | Probability of DQZ |
| `p_dqpec` | Probability of DQpec (peculiar DQ) |
| `p_dz` | Probability of DZ |
| `p_dza` | Probability of DZA |
| `p_dzb` | Probability of DZB |
| `p_dzba` | Probability of DZBA |
| `p_mwd` | Probability of magnetic white dwarf |
| `p_hotdq` | Probability of hot DQ |

### Stellar parameters (DA only)

| Field | Description |
| --- | --- |
| `teff` | Effective temperature (K) |
| `e_teff` | Uncertainty in Teff (K), after noise model |
| `logg` | Surface gravity (log g, dex) |
| `e_logg` | Uncertainty in log g (dex), after noise model |
| `raw_e_teff` | Formal (pre-noise-model) uncertainty in Teff |
| `raw_e_logg` | Formal (pre-noise-model) uncertainty in log g |
| `v_rel` | Relative velocity used in the fit (km/s) |

### Spectral data

| Field | Description |
| --- | --- |
| `wavelength` | Wavelength array (log-lambda spaced, 4648 pixels starting at log10(lambda) = 3.5523) |
| `model_flux` | Best-fit DA model flux, resampled to the wavelength grid (DA fits only) |

## Flags

Snow White uses a `result_flags` bit field. The individual flags are:

| Flag | Bit | Description |
| --- | --- | --- |
| `flag_low_snr` | 2^0 | Signal-to-noise ratio is 8 or below. Results may be unreliable. |
| `flag_unconverged` | 2^1 | The fit did not converge. |
| `flag_teff_grid_edge_bad` | 2^2 | Teff is at the edge of the model grid. |
| `flag_logg_grid_edge_bad` | 2^3 | log g is at the edge of the model grid. |
| `flag_no_flux` | 2^4 | The spectrum has no flux (all zeros or invalid). No results are produced. |
| `flag_not_mwm_wd` | 2^5 | The source is not in the `mwm_wd` program. No analysis is performed. |
| `flag_missing_bp_rp_mag` | 2^6 | Gaia BP or RP magnitude is missing, so the hot/cold solution could not be disambiguated. No stellar parameters are reported. |

## Things to know

- **Only DA white dwarfs get stellar parameters.** Other subtypes (DB, DC, DQ, DZ,
  etc.) receive a classification and class probabilities, but no Teff or log g.

- **The noise model inflates formal uncertainties.** The reported `e_teff` and `e_logg`
  include an empirical correction applied to the formal fitting uncertainties. If you
  need the raw formal errors, use `raw_e_teff` and `raw_e_logg`.

- **Hot/cold ambiguity requires Gaia photometry.** Without BP and RP magnitudes,
  the pipeline cannot choose between the hot and cold solutions and will set
  `flag_missing_bp_rp_mag` without reporting stellar parameters.

- **Low S/N results are flagged but still reported.** When the spectrum has S/N <= 8,
  `flag_low_snr` is set. The fitted parameters are still present in the output, but
  should be treated with caution.

- **Classification notation:**
  - A plain type like `DA` means the classifier is confident (probability >= 0.5).
  - A type with a colon like `DA:` means the top class had probability < 0.5, and the
    second class was not close enough to warrant a dual classification.
  - A dual type like `DA/DB` means the top two classes had similar probabilities
    (ratio > 0.6).

- **Input spectra.** Snow White operates on BOSS combined spectra
  (`BossCombinedSpectrum`). Only sources assigned to the `mwm_wd` carton program
  are analyzed; all others are immediately flagged with `flag_not_mwm_wd`.

- **Grid boundaries.** The emulator covers Teff from 4,000 to 120,000 K and log g from
  6.01 to 9.49. Fits that land on these boundaries should be interpreted carefully.
