# AstroNN Distances

The AstroNN distance pipeline estimates spectrophotometric distances to stars by predicting a "fake" absolute Ks-band magnitude from APOGEE spectra, then combining it with photometry and extinction information to compute a distance.

## What it does

Given an APOGEE coadded spectrum and associated photometric metadata, this pipeline produces:

- A predicted Ks-band absolute luminosity ("fakemag")
- A spectrophotometric distance (in parsecs)
- Extinction-related quantities

It operates on coadded APOGEE spectra (`ApogeeCoaddedSpectrumInApStar`).

## How it works

### Network architecture

The distance model uses a Bayesian Convolutional Neural Network (ApogeeDistBCNN) implemented in PyTorch. The architecture is simpler than the full AstroNN abundance network:

1. Two 1D convolutional layers (filter size 8, channels: 1 -> 2 -> 4)
2. Max pooling (pool length 4)
3. Two dense layers (7512 -> 192 -> 64)
4. Two output heads:
   - **Prediction head**: a dense layer with softplus activation that outputs the predicted fakemag
   - **Variance head**: a dense layer that outputs the log-variance of the prediction

The single output target is `fakemag`, a neural network-predicted Ks-band absolute magnitude.

### Uncertainty estimation

Like the main AstroNN pipeline, this uses MC Dropout:

- Dropout (rate 0.3) is applied at inference time.
- The network is evaluated 100 times per spectrum.
- Total uncertainty combines the predictive variance with MC dropout variance.

### Distance calculation

The distance is computed by combining:

1. **fakemag**: the predicted absolute Ks-band magnitude from the spectrum
2. **k_mag**: the apparent Ks-band magnitude from 2MASS photometry (from the source catalog)
3. **E(B-V)**: reddening from the source catalog
4. **A_K**: Ks-band extinction, computed as `A_K = E(B-V) * 0.3517`

The extinction-corrected apparent magnitude is calculated, and the distance modulus is used to convert to a distance in parsecs via the `astroNN.gaia.fakemag_to_pc` utility.

### Preprocessing

Spectra are continuum-normalized using `astroNN.apogee.apogee_continuum` (DR17 mode), the same normalization used in the main AstroNN pipeline.

## Output fields

| Field | Type | Description |
|-------|------|-------------|
| `k_mag` | float | 2MASS Ks-band apparent magnitude used |
| `ebv` | float | E(B-V) reddening value used |
| `A_k_mag` | float | Ks-band extinction (A_K) |
| `L_fakemag` | float | Predicted Ks-band absolute luminosity (fakemag) |
| `e_L_fakemag` | float | Uncertainty in fakemag |
| `dist` | float | Spectrophotometric distance (parsecs) |
| `e_dist` | float | Uncertainty in distance (parsecs) |
| `result_flags` | bitmask | Bitfield encoding quality flags |

## Flags

| Flag | Bit | Description |
|------|-----|-------------|
| `flag_fakemag_unreliable` | 2^0 | Predicted fakemag is unreliable (`fakemag_err / fakemag >= 0.2`) |
| `flag_missing_photometry` | 2^1 | Missing Ks-band apparent magnitude from source catalog |
| `flag_missing_extinction` | 2^2 | Missing extinction (E(B-V)) from source catalog |
| `flag_no_result` | 2^11 | Exception raised when loading spectra |

### Summary flags

- **`flag_warn`**: Set when `flag_missing_extinction` is set. Missing extinction alone is a warning because the distance can still be computed (with lower accuracy).
- **`flag_bad`**: Set when any of `flag_fakemag_unreliable`, `flag_missing_photometry`, or `flag_no_result` is set.

## Caveats

- This pipeline requires ancillary data beyond the spectrum: Ks-band photometry from 2MASS and E(B-V) reddening values must be present in the source catalog. Missing photometry will cause the `flag_missing_photometry` flag to be set and the result to be marked as bad.
- Missing extinction is treated as a warning rather than a fatal error. The pipeline will substitute zero values for missing photometry or extinction, but the resulting distances should be treated with caution.
- Very large distance values (> 10^10 pc) are replaced with NaN.
- The extinction law used is: `A_K = 0.3517 * E(B-V)`, derived from the relation `A_K = 0.918 * E(B-V) / 2.61`.
- The fakemag prediction uses a softplus activation, ensuring the predicted absolute magnitude is always positive.
- This pipeline only operates on coadded spectra, not individual visit spectra.
