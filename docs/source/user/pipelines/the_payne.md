# The Payne

The Payne is a neural network-based spectral emulator that estimates stellar labels by fitting a forward model to observed APOGEE spectra. Unlike pipelines that directly predict labels from spectra, The Payne uses a trained neural network as a fast spectral emulator and optimizes labels to best match the observed spectrum.

## What it does

The Payne estimates a comprehensive set of stellar labels from APOGEE spectra:

- **Stellar parameters**: Teff, log g, microturbulence (v_turb), macroturbulence (v_macro)
- **Chemical abundances**: [C/H], [N/H], [O/H], [Na/H], [Mg/H], [Al/H], [Si/H], [P/H], [S/H], [K/H], [Ca/H], [Ti/H], [V/H], [Cr/H], [Mn/H], [Fe/H], [Co/H], [Ni/H], [Cu/H], [Ge/H]
- **Isotope ratio**: 12C/13C
- **Radial velocity**: v_rel (if enabled via `v_rad_tolerance`)
- **Fit quality**: chi-squared and reduced chi-squared

## How it works

### Neural network emulator

The Payne uses a simple feedforward neural network with two hidden layers and leaky ReLU activations to emulate stellar spectra. Given a set of stellar labels, the network rapidly predicts the corresponding spectrum:

```
input labels -> Dense + Leaky ReLU -> Dense + Leaky ReLU -> Dense -> predicted spectrum
```

The network weights and biases are loaded from a pre-trained model (`payne_apogee_nn.pkl`). The labels are internally scaled to a [0, 1] range using pre-computed `x_min` and `x_max` values.

### Fitting procedure

For each observed spectrum, The Payne:

1. **Continuum-normalizes** the observed spectrum using a Chebyshev polynomial (degree 4 by default) fit across three APOGEE detector regions:
   - 15,100 -- 15,793 Angstroms
   - 15,880 -- 16,417 Angstroms
   - 16,499 -- 17,000 Angstroms

2. **Interpolates** the observed spectrum onto the model wavelength grid.

3. **Optimizes** the stellar labels using `scipy.optimize.curve_fit` with the Trust Region Reflective (TRF) method, minimizing the chi-squared difference between the model and observed spectrum.

4. **Estimates uncertainties** from the covariance matrix of the fit. Correlation coefficients between all pairs of labels are also computed and stored.

### Radial velocity

Radial velocity fitting is controlled by the `v_rad_tolerance` parameter (default: 0, meaning disabled). When enabled, an additional label representing the radial velocity is optimized simultaneously with the stellar labels, and the model spectrum is Doppler-shifted accordingly.

### Pixel masking

A mask (`payne_apogee_mask.npy`) can be applied to exclude certain spectral pixels from the fit. Masked pixels have their inverse variance set to zero.

### Noise model

A post-hoc noise model is applied to adjust formal uncertainties. The raw (formal) uncertainties from the fit covariance matrix are stored as `raw_e_*` fields, while the corrected uncertainties (using empirical calibration from `ThePayne_corrections.pkl`) are stored as `e_*` fields.

## Output fields

### Stellar parameters

| Field | Type | Description |
|-------|------|-------------|
| `teff` | float | Effective temperature (K) |
| `e_teff` | float | Uncertainty in Teff |
| `logg` | float | Surface gravity (log10(cm/s^2)) |
| `e_logg` | float | Uncertainty in log g |
| `v_turb` | float | Microturbulent velocity (km/s) |
| `e_v_turb` | float | Uncertainty in v_turb |
| `v_macro` | float | Macroturbulent velocity (km/s) |
| `e_v_macro` | float | Uncertainty in v_macro |
| `v_rel` | float | Relative radial velocity (km/s) |

### Chemical abundances

Each abundance is reported as [X/H] in dex:

| Fields | Element |
|--------|---------|
| `c_h`, `e_c_h` | Carbon |
| `n_h`, `e_n_h` | Nitrogen |
| `o_h`, `e_o_h` | Oxygen |
| `na_h`, `e_na_h` | Sodium |
| `mg_h`, `e_mg_h` | Magnesium |
| `al_h`, `e_al_h` | Aluminum |
| `si_h`, `e_si_h` | Silicon |
| `p_h`, `e_p_h` | Phosphorus |
| `s_h`, `e_s_h` | Sulfur |
| `k_h`, `e_k_h` | Potassium |
| `ca_h`, `e_ca_h` | Calcium |
| `ti_h`, `e_ti_h` | Titanium |
| `v_h`, `e_v_h` | Vanadium |
| `cr_h`, `e_cr_h` | Chromium |
| `mn_h`, `e_mn_h` | Manganese |
| `fe_h`, `e_fe_h` | Iron |
| `co_h`, `e_co_h` | Cobalt |
| `ni_h`, `e_ni_h` | Nickel |
| `cu_h`, `e_cu_h` | Copper |
| `ge_h`, `e_ge_h` | Germanium |
| `c12_c13`, `e_c12_c13` | Carbon isotope ratio (12C/13C) |

All labels also have `raw_e_*` counterparts storing the formal uncertainties before noise model correction.

### Fit quality and metadata

| Field | Type | Description |
|-------|------|-------------|
| `chi2` | float | Chi-squared of the best fit |
| `reduced_chi2` | float | Reduced chi-squared |
| `result_flags` | bitmask | Bitfield encoding quality flags |

### Correlation coefficients

Pairwise correlation coefficients between all labels are stored as `rho_<label1>_<label2>` fields (e.g., `rho_teff_logg`, `rho_teff_fe_h`). These are derived from the covariance matrix of the fit.

### Spectral data

| Field | Type | Description |
|-------|------|-------------|
| `wavelength` | array | Wavelength array (log-lambda spaced, 8575 pixels) |
| `model_flux` | array | Best-fit model flux (continuum x rectified model) |
| `continuum` | array | Fitted continuum |

These spectral data arrays are stored in intermediate pickle files and loaded on demand.

## Flags

| Flag | Bit | Description |
|------|-----|-------------|
| `flag_fitting_failure` | 2^0 | The fitting procedure failed |
| `flag_warn_teff` | 2^1 | Teff < 3,100 K or Teff > 7,900 K |
| `flag_warn_logg` | 2^2 | log g < 0.1 or log g > 5.2 |
| `flag_warn_fe_h` | 2^3 | [Fe/H] > 0.4 or [Fe/H] < -1.4 |
| `flag_low_snr` | 2^4 | S/N < 70 |

### Summary flags

- **`flag_warn`**: Set when any flag bit is non-zero (i.e., `result_flags > 0`).
- **`flag_bad`**: Set only when `flag_fitting_failure` is set.

## Caveats

- The Payne is a forward-modeling approach: it uses a neural network to emulate spectra and optimizes labels to fit the data. This is fundamentally different from pipelines like AstroNN or APOGEENet, which directly predict labels from spectra.
- The effective training range is approximately 3,100 -- 7,900 K in Teff, 0.1 -- 5.2 in log g, and -1.4 to +0.4 in [Fe/H]. Results outside these ranges are flagged.
- The optimization uses bounded parameters (labels are internally constrained to [0, 1] in normalized space). This means the optimizer cannot extrapolate far beyond the training grid.
- Continuum normalization uses a 4th-order Chebyshev polynomial fit with a fixed pixel mask. Poor continuum fits (e.g., for emission-line stars or heavily reddened spectra) can propagate into biased label estimates.
- The model and continuum spectral arrays are stored as intermediate pickle files, not in the database directly. These files are loaded lazily when the `model_flux` or `continuum` attributes are accessed.
- The formal uncertainties from the covariance matrix tend to underestimate true uncertainties, which is why a post-hoc noise model correction is applied.
