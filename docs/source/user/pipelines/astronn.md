# AstroNN

AstroNN is a Bayesian convolutional neural network pipeline that estimates stellar parameters and detailed chemical abundances from APOGEE spectra.

## What it does

AstroNN predicts 22 stellar labels from continuum-normalized APOGEE spectra:

- **Stellar parameters**: Teff, log g
- **Chemical abundances**: [C/H], [C I/H], [N/H], [O/H], [Na/H], [Mg/H], [Al/H], [Si/H], [P/H], [S/H], [K/H], [Ca/H], [Ti/H], [Ti II/H], [V/H], [Cr/H], [Mn/H], [Fe/H], [Co/H], [Ni/H]

It operates on both coadded and visit-level APOGEE spectra (`ApogeeCoaddedSpectrumInApStar` or `ApogeeVisitSpectrumInApStar`).

## How it works

### Network architecture

AstroNN uses a Bayesian Convolutional Neural Network with Censoring (ApogeeBCNNCensored), implemented in PyTorch. The architecture has two parallel branches:

1. **Full-spectrum CNN branch**:
   - Two 1D convolutional layers (filter size 8, channels: 1 -> 2 -> 4)
   - Max pooling (pool length 4)
   - Two dense layers (7512 -> 192 -> 96)
   - Outputs Teff, log g, and [Fe/H] along with auxiliary features

2. **Element-specific censored branch**:
   - For each of 19 chemical elements, a separate sub-network processes only the ASPCAP-masked pixels relevant to that element.
   - Each sub-network has two dense layers with ReLU activation.
   - The output of each element sub-network is concatenated with the full-spectrum branch outputs (Teff, log g, [Fe/H] + 2 auxiliary features) before a final dense layer predicts that element's abundance.

This censored design ensures each element is predicted primarily from spectral regions that contain its absorption features, improving robustness.

### Uncertainty estimation

AstroNN uses MC Dropout for uncertainty estimation:

- Dropout (rate 0.3) is applied at inference time.
- The network is evaluated `mc_num` times (default: 100) per spectrum.
- The total uncertainty combines the predictive variance (from the network's variance output head) with the MC dropout variance.

### Preprocessing

Spectra are continuum-normalized using the `astroNN.apogee.apogee_continuum` function (DR17 mode), which normalizes the 8575-pixel APOGEE spectra to 7514 pixels.

### Noise model

After initial predictions, a post-hoc noise model is applied using correction factors loaded from a pickle file (`AstroNN_corrections.pkl`). For each label, the corrected uncertainty is:

```
e_label = scale * raw_e_label + offset
```

where `scale` and `offset` are empirically calibrated.

## Output fields

### Stellar parameters

| Field | Type | Description |
|-------|------|-------------|
| `teff` | float | Effective temperature (K) |
| `e_teff` | float | Uncertainty in Teff (K), after noise model |
| `logg` | float | Surface gravity (log10(cm/s^2)) |
| `e_logg` | float | Uncertainty in log g, after noise model |

### Chemical abundances

Each abundance is reported as [X/H] in dex, with an associated uncertainty:

| Fields | Element |
|--------|---------|
| `c_h`, `e_c_h` | Carbon |
| `c_1_h`, `e_c_1_h` | Carbon (C I) |
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
| `ti_2_h`, `e_ti_2_h` | Titanium (Ti II) |
| `v_h`, `e_v_h` | Vanadium |
| `cr_h`, `e_cr_h` | Chromium |
| `mn_h`, `e_mn_h` | Manganese |
| `fe_h`, `e_fe_h` | Iron |
| `co_h`, `e_co_h` | Cobalt |
| `ni_h`, `e_ni_h` | Nickel |

All abundances also have `raw_*` counterparts (e.g., `raw_teff`, `raw_e_teff`) that store the values before the noise model correction is applied.

### Other fields

| Field | Type | Description |
|-------|------|-------------|
| `result_flags` | bitmask | Bitfield encoding quality flags |

## Flags

| Flag | Bit | Description |
|------|-----|-------------|
| `flag_uncertain_logg` | 2^0 | Surface gravity is uncertain (`e_logg` > 0.2 and `abs(e_logg/logg)` > 0.075) |
| `flag_uncertain_teff` | 2^1 | Effective temperature is uncertain (`e_teff` > 300) |
| `flag_uncertain_fe_h` | 2^2 | Iron abundance is uncertain (`abs(e_fe_h/fe_h)` > 0.12) |
| `flag_no_result` | 2^11 | Exception raised when loading spectra |

### Summary flags

- **`flag_warn`**: Set when any flag bit is non-zero (i.e., `result_flags > 0`).
- **`flag_bad`**: Set when all three of `flag_uncertain_logg`, `flag_uncertain_teff`, and `flag_uncertain_fe_h` are set simultaneously.

## Caveats

- AstroNN was originally trained on APOGEE DR17 labels. The model used in Astra is a retrained PyTorch port of the original TensorFlow model.
- The continuum normalization uses the `astroNN` package's built-in APOGEE continuum routine with DR17 settings.
- Processing can run in parallel mode (using multiprocessing queues) or serial mode. Parallel mode is the default and is recommended for large batches.
- The `flag_bad` flag requires all three uncertainty flags to be set simultaneously, meaning a result is only marked "bad" if it is uncertain across all primary parameters.
- The `raw_*` fields store the network's direct predictions and formal uncertainties before post-hoc calibration.
