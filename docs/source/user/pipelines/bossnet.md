# BossNet

BossNet is a convolutional neural network pipeline that estimates stellar parameters from BOSS optical spectra. It shares the same ResNet architecture as APOGEENet but is trained on optical spectral data and additionally predicts radial velocity.

## What it does

BossNet predicts four quantities from an input BOSS spectrum:

- **Effective temperature** (Teff)
- **Surface gravity** (log g)
- **Metallicity** ([Fe/H])
- **Radial velocity** (v_r)

It operates on both visit-level and combined BOSS spectra (`BossVisitSpectrum` or `BossCombinedSpectrum`).

## How it works

### Network architecture

BossNet uses the same deep residual convolutional neural network as APOGEENet, with an additional output for radial velocity. The architecture consists of:

1. **Positional encoding** -- a 1D positional encoding is appended to the flux channel.
2. **Initial convolution block** -- a 1D convolutional block (kernel size 30) mapping 2 input channels to 4 output channels.
3. **Four residual blocks** -- each doubling the channel count (4 -> 8 -> 16 -> 32 -> 64), using kernel size 30.
4. **Adaptive average pooling** -- reduces the spatial dimension to 1024.
5. **Fully connected layers** -- six linear blocks of dimension 1024, followed by a final linear layer outputting 4 values (log g, log Teff, [Fe/H], radial velocity).

### Preprocessing

BOSS spectra undergo several preprocessing steps:

1. **Wavelength interpolation**: The variable-resolution BOSS spectra are interpolated onto a uniform linear wavelength grid spanning 3,800 -- 8,900 Angstroms with 3,900 pixels.
2. **Error conversion**: Inverse variance is converted to error, with handling for infinite/NaN values. Errors are clipped at 5 times the median non-infinite error.
3. **Log scaling**: The flux is log-transformed (`log(clip(flux, min=1e-6))`) and clipped at the 95th percentile + 1 to remove outliers.

### Uncertainty estimation

Uncertainties are estimated via Monte Carlo noise injection:

1. `num_uncertainty_draws` (default: 20) noise realizations are generated.
2. Each noised spectrum is interpolated and log-scaled.
3. The standard deviation of the resulting predictions gives the uncertainty.

### Output conversion

The network predicts log(Teff) and log(g) in a normalized space. These are unnormalized and converted:

- `teff = 10^(log_Teff)`
- `e_teff = 10^(log_Teff) * log_Teff_std * ln(10)`

Radial velocity and its uncertainty are returned directly after unnormalization.

## Output fields

| Field | Type | Description |
|-------|------|-------------|
| `teff` | float | Effective temperature (K) |
| `e_teff` | float | Uncertainty in Teff (K) |
| `logg` | float | Surface gravity (log10(cm/s^2)) |
| `e_logg` | float | Uncertainty in log g |
| `fe_h` | float | Metallicity [Fe/H] (dex) |
| `e_fe_h` | float | Uncertainty in [Fe/H] (dex) |
| `bn_v_r` | float | Radial velocity (km/s) as estimated by BossNet |
| `e_bn_v_r` | float | Uncertainty in radial velocity (km/s) |
| `result_flags` | bitmask | Bitfield encoding quality flags |

## Flags

| Flag | Bit | Description |
|------|-----|-------------|
| `flag_runtime_exception` | 2^0 | Exception occurred at runtime |
| `flag_unreliable_teff` | 2^1 | Teff is outside the range 1,700 -- 100,000 K |
| `flag_unreliable_logg` | 2^2 | log g is outside the range -1 to 10 |
| `flag_unreliable_fe_h` | 2^3 | Teff < 3,200 K, or log g > 5, or [Fe/H] is outside -4 to +2 |
| `flag_suspicious_fe_h` | 2^4 | [Fe/H] may be suspicious for stars with Teff < 3,900 K and 3 < log g < 6 |

### Summary flags

- **`flag_warn`**: Set when `flag_unreliable_fe_h` or `flag_suspicious_fe_h` is set.
- **`flag_bad`**: Set when any of `flag_unreliable_teff`, `flag_unreliable_logg`, `flag_unreliable_fe_h`, or `flag_runtime_exception` is set.

## Caveats

- BossNet uses a wider log g validity range (-1 to 10) than APOGEENet (-1 to 6), reflecting the different stellar populations observed in BOSS optical spectra.
- The radial velocity field is named `bn_v_r` (rather than `v_r`) to distinguish it from radial velocities measured by other methods.
- The `flag_suspicious_fe_h` flag is specific to BossNet and warns about potentially unreliable [Fe/H] values for cool dwarf/subgiant stars (Teff < 3,900 K, 3 < log g < 6), where optical metallicity estimates may be less reliable.
- BOSS spectra are interpolated from their native (variable) wavelength grid onto a uniform linear grid before processing. This interpolation step may introduce small systematic effects.
- GPU acceleration is used automatically when available.
- If no valid Teff can be predicted (e.g., due to a failed inference), the `flag_runtime_exception` flag is automatically set.
