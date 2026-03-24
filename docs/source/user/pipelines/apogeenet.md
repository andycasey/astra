# APOGEENet

APOGEENet (also referred to as ANet or APOGEENet III) is a convolutional neural network pipeline that estimates fundamental stellar parameters from APOGEE spectra.

## What it does

APOGEENet predicts three stellar parameters from an input APOGEE spectrum:

- **Effective temperature** (Teff)
- **Surface gravity** (log g)
- **Metallicity** ([Fe/H])

It operates on both coadded and visit-level APOGEE spectra stored as `ApogeeCoaddedSpectrumInApStar` or `ApogeeVisitSpectrumInApStar` objects.

## How it works

### Network architecture

APOGEENet uses a deep residual convolutional neural network (ResNet) built in PyTorch. The architecture consists of:

1. **Positional encoding** -- a 1D positional encoding is prepended to the flux, giving the network awareness of pixel position.
2. **Initial convolution block** -- a 1D convolutional block (kernel size 30) that maps from 2 input channels (flux + positional encoding) to 4 output channels.
3. **Four residual blocks** -- each block doubles the number of channels (4 -> 8 -> 16 -> 32 -> 64), using kernel size 30 with padding 15.
4. **Adaptive average pooling** -- reduces the spatial dimension to a fixed length of 1024.
5. **Fully connected layers** -- six linear blocks of dimension 1024, followed by a final linear layer that outputs 3 values (log g, log Teff, [Fe/H]).

### Preprocessing

Before inference, the input flux is:

1. Converted from inverse variance to error (with handling for infinite/NaN values).
2. Log-scaled: `flux = log(clip(flux, min=1e-6))`, then clipped at the 95th percentile + 1 to remove outliers.

### Uncertainty estimation

Uncertainties are estimated via a Monte Carlo noise injection approach. The pipeline:

1. Draws `num_uncertainty_draws` (default: 20) noise realizations by adding Gaussian noise to the flux based on the error spectrum.
2. Runs each noised spectrum through the network.
3. Takes the standard deviation of the resulting predictions as the uncertainty estimate.

A post-hoc noise model is also applied to adjust the formal uncertainties:

- `e_teff = 1.25 * raw_e_teff + 10`
- `e_logg = 1.25 * raw_e_logg + 0.01`
- `e_fe_h = raw_e_fe_h + 0.01`

### Output conversion

The network internally predicts log(Teff), log(g), and [Fe/H] in a normalized space. These are unnormalized using pre-computed statistics (mean and standard deviation) and then converted:

- `teff = 10^(log_Teff)`
- `e_teff = 10^(log_Teff) * log_Teff_std * ln(10)` (propagated from log-space)

## Output fields

| Field | Type | Description |
|-------|------|-------------|
| `teff` | float | Effective temperature (K) |
| `e_teff` | float | Uncertainty in Teff (K), after noise model correction |
| `logg` | float | Surface gravity (log10(cm/s^2)) |
| `e_logg` | float | Uncertainty in log g, after noise model correction |
| `fe_h` | float | Metallicity [Fe/H] (dex) |
| `e_fe_h` | float | Uncertainty in [Fe/H] (dex), after noise model correction |
| `raw_e_teff` | float | Formal (raw) uncertainty in Teff before noise model |
| `raw_e_logg` | float | Formal (raw) uncertainty in log g before noise model |
| `raw_e_fe_h` | float | Formal (raw) uncertainty in [Fe/H] before noise model |
| `result_flags` | bitmask | Bitfield encoding quality flags |

## Flags

| Flag | Bit | Description |
|------|-----|-------------|
| `flag_runtime_exception` | 2^0 | An exception was raised during runtime |
| `flag_unreliable_teff` | 2^1 | Teff is outside the range 1,700 -- 100,000 K |
| `flag_unreliable_logg` | 2^2 | log g is outside the range -1 to 6 |
| `flag_unreliable_fe_h` | 2^3 | Teff < 3,200 K, or log g > 5, or [Fe/H] is outside -4 to +2 |

### Summary flags

- **`flag_warn`**: Set when `flag_unreliable_fe_h` is set.
- **`flag_bad`**: Set when any of `flag_unreliable_teff`, `flag_unreliable_logg`, `flag_unreliable_fe_h`, or `flag_runtime_exception` is set.

## Caveats

- The network was originally designed for BOSS optical spectra ("BossNet") and adapted for APOGEE near-infrared spectra. The same ResNet architecture is used but trained on APOGEE data.
- The [Fe/H] estimates are flagged as unreliable for cool stars (Teff < 3,200 K) and high-gravity stars (log g > 5), where the training data coverage is limited.
- The reported uncertainties (`e_teff`, `e_logg`, `e_fe_h`) include a post-hoc noise model correction. The raw (formal) uncertainties from Monte Carlo noise injection are stored separately as `raw_e_*` fields.
- GPU acceleration is used automatically when available.
