# The Cannon

The Cannon is a data-driven spectral modeling pipeline that estimates stellar labels from APOGEE spectra using a second-order polynomial model trained on a reference set of labeled spectra.

## What it does

The Cannon estimates stellar parameters and chemical abundances from APOGEE spectra:

- **Stellar parameters**: Teff, log g, microturbulence (v_micro), macroturbulence (v_macro)
- **Metallicity**: [Fe/H]
- **Chemical abundances** (as [X/Fe]): [C/Fe], [N/Fe], [O/Fe], [Na/Fe], [Mg/Fe], [Al/Fe], [Si/Fe], [S/Fe], [K/Fe], [Ca/Fe], [Ti/Fe], [V/Fe], [Cr/Fe], [Mn/Fe], [Ni/Fe]

It operates on APOGEE coadded spectra (`ApogeeCoaddedSpectrumInApStar`), visit spectra (`ApogeeVisitSpectrumInApStar`), and combined spectra (`ApogeeCombinedSpectrum`).

## How it works

### The generative model

The Cannon is a data-driven model that learns a mapping from stellar labels to spectra using a training set of spectra with known labels. At each pixel, the flux is modeled as a second-order polynomial function of the labels:

```
f(labels) = theta_0 + theta_1 * L1 + theta_2 * L1^2 + theta_3 * L2 + theta_4 * L1*L2 + theta_5 * L2^2 + ...
```

This includes a bias term, linear terms, quadratic terms, and cross-terms for all label pairs.

### Training

The training step fits the model coefficients (theta) at each pixel independently:

1. Labels are normalized to zero mean and unit variance.
2. A design matrix is constructed from the normalized labels (including all second-order terms).
3. The coefficients are fit by (optionally regularized) linear regression using scikit-learn's `LinearRegression` or `Lasso`.
4. A model variance (s^2) is computed at each pixel to account for model inadequacy (the difference between the model predictions and the training data beyond what is explained by observational noise).

### Inference (test step)

For each test spectrum:

1. The spectrum is continuum-normalized. Two methods are used depending on the spectrum type:
   - For `ApogeeCombinedSpectrum`: the continuum is pre-computed and stored with the spectrum.
   - For `ApogeeCoaddedSpectrumInApStar` and visit spectra: NMF (non-negative matrix factorization) continuum normalization is used via the `NMFRectify` pipeline's stored continuum parameters.

2. The labels are optimized using `scipy.optimize.curve_fit`, minimizing the chi-squared between the observed and model spectra. The total inverse variance used for weighting includes both the observational noise and the model variance:
   ```
   adjusted_ivar = ivar / (1 + ivar * s2)
   ```

3. Multiple initial guesses are tried (zeros, +1, -1, and a linear algebra estimate), and the one with the lowest chi-squared is used.

4. Uncertainties are estimated from the covariance matrix of the fit.

### Noise model

A post-hoc noise model correction is applied to the formal uncertainties using empirical calibration from `TheCannon_corrections.pkl`:

```
e_label = scale * raw_e_label + offset
```

## Output fields

### Stellar parameters

| Field | Type | Description |
|-------|------|-------------|
| `teff` | float | Effective temperature (K) |
| `e_teff` | float | Uncertainty in Teff |
| `logg` | float | Surface gravity (log10(cm/s^2)) |
| `e_logg` | float | Uncertainty in log g |
| `fe_h` | float | Metallicity [Fe/H] (dex) |
| `e_fe_h` | float | Uncertainty in [Fe/H] |
| `v_micro` | float | Microturbulent velocity (km/s) |
| `e_v_micro` | float | Uncertainty in v_micro |
| `v_macro` | float | Macroturbulent velocity (km/s) |
| `e_v_macro` | float | Uncertainty in v_macro |

### Chemical abundances

Chemical abundances are reported as [X/Fe] (relative to iron):

| Fields | Element |
|--------|---------|
| `c_fe`, `e_c_fe` | Carbon |
| `n_fe`, `e_n_fe` | Nitrogen |
| `o_fe`, `e_o_fe` | Oxygen |
| `na_fe`, `e_na_fe` | Sodium |
| `mg_fe`, `e_mg_fe` | Magnesium |
| `al_fe`, `e_al_fe` | Aluminum |
| `si_fe`, `e_si_fe` | Silicon |
| `s_fe`, `e_s_fe` | Sulfur |
| `k_fe`, `e_k_fe` | Potassium |
| `ca_fe`, `e_ca_fe` | Calcium |
| `ti_fe`, `e_ti_fe` | Titanium |
| `v_fe`, `e_v_fe` | Vanadium |
| `cr_fe`, `e_cr_fe` | Chromium |
| `mn_fe`, `e_mn_fe` | Manganese |
| `ni_fe`, `e_ni_fe` | Nickel |

All labels also have `raw_e_*` counterparts storing the formal uncertainties before noise model correction.

### Fit quality and metadata

| Field | Type | Description |
|-------|------|-------------|
| `chi2` | float | Chi-squared of the best fit |
| `rchi2` | float | Reduced chi-squared |
| `ier` | int | Integer flag from `scipy.optimize.curve_fit` indicating solver status |
| `nfev` | int | Number of function evaluations during optimization |
| `x0_index` | int | Index of the initial guess trial that produced the best chi-squared |
| `result_flags` | bitmask | Bitfield encoding quality flags |

### Spectral data

| Field | Type | Description |
|-------|------|-------------|
| `wavelength` | array | Wavelength array (log-lambda spaced, 8575 pixels) |
| `model_flux` | array | Best-fit model flux (continuum x rectified model) |
| `continuum` | array | Fitted continuum used for normalization |

These spectral data arrays are stored in intermediate pickle files and loaded on demand.

## Flags

| Flag | Bit | Description |
|------|-----|-------------|
| `flag_fitting_failure` | 2^0 | The fitting procedure failed |

### Summary flags

- **`flag_bad`**: Equivalent to `flag_fitting_failure`.

## Caveats

- The Cannon is a data-driven method. Its accuracy is fundamentally limited by the quality and coverage of its training set. Results for stars outside the training set's label space (the convex hull of the training labels) should be treated with extra caution.
- Chemical abundances are reported as [X/Fe] (relative to iron), not [X/H] (relative to hydrogen), unlike The Payne and AstroNN.
- The model assumes a second-order polynomial relationship between labels and spectra at each pixel. This can limit accuracy for labels that have more complex spectral signatures.
- Continuum normalization is a critical step. For coadded and visit spectra, the pipeline requires pre-computed NMF continuum parameters from the `NMFRectify` pipeline. Spectra without successful NMF rectification are excluded from processing.
- The model variance (s^2) term accounts for model inadequacy but can downweight informative pixels if the training set has large scatter at those pixels.
- The `x0_index` field indicates which initial guess trial was used (0 = zeros, 1 = +1, 2 = -1, 3 = linear algebra estimate). If the best fit frequently comes from the +1 or -1 trials rather than the linear algebra estimate, it may indicate the model is struggling with certain types of spectra.
- The model and continuum spectral arrays are stored as intermediate pickle files organized by source primary key, not in the database directly.
- The formal uncertainties from `curve_fit` tend to underestimate true uncertainties. The post-hoc noise model correction addresses this.
