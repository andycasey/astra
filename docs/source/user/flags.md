# Bitflags Reference

This page documents the bitflag system used across Astra pipeline models to communicate quality information about analysis results.

## How bitflags work in Astra

Astra uses Peewee's `BitField` to store multiple boolean flags in a single integer database column. Most pipeline models have a field called `result_flags` (and some have additional flag fields like `ferre_flags`, `initial_flags`, or per-element abundance flags).

Each individual flag occupies a single bit position in the integer. The bit values are powers of 2 (1, 2, 4, 8, 16, ...), so multiple flags can be combined by adding their values together. For example, if `flag_a` has bit value 1 and `flag_b` has bit value 4, an integer value of 5 means both flags are set.

### Checking flags in Python

You can check whether a flag is set on a result object by accessing it as a boolean property:

```python
result = ApogeeNet.get_by_id(some_pk)

# Check a single flag
if result.flag_unreliable_teff:
    print("Teff is unreliable")

# Check the summary properties
if result.flag_bad:
    print("This result should not be used")
elif result.flag_warn:
    print("This result may have quality issues")
```

### Setting flags

When creating or updating results, flags can be set as boolean values:

```python
result.flag_unreliable_teff = True
result.save()
```

## Common flags: `flag_warn` and `flag_bad`

Most pipeline models define two computed (hybrid) properties that summarize result quality:

- **`flag_warn`** -- Indicates that the result may have quality issues. The result might still be usable, but downstream users should exercise caution. The specific conditions that trigger a warning vary by pipeline.

- **`flag_bad`** -- Indicates that the result is considered unreliable and should generally not be used for science. This is a stricter condition than `flag_warn`. Any result with `flag_bad` set should be treated with extreme skepticism.

These are not stored in the database directly. They are computed from the individual flags using bitwise OR operations. The exact flags that contribute to `flag_warn` and `flag_bad` differ by pipeline and are documented in the per-pipeline sections below.

## Per-pipeline flag reference

### ApogeeNet

**Model:** `ApogeeNet` (`src/astra/models/apogeenet.py`)

APOGEENet (version 3) estimates effective temperature, surface gravity, and metallicity from APOGEE spectra using a neural network.

**BitField:** `result_flags`

| Flag name | Bit value | Description | `flag_warn` | `flag_bad` |
|---|---|---|---|---|
| `flag_runtime_exception` | 2^0 = 1 | Exception raised during runtime | | Yes |
| `flag_unreliable_teff` | 2^1 = 2 | `teff` is outside the range of 1700 K to 100,000 K | | Yes |
| `flag_unreliable_logg` | 2^2 = 4 | `logg` is outside the range of -1 to 6 | | Yes |
| `flag_unreliable_fe_h` | 2^3 = 8 | `teff` < 3200 K, or `logg` > 5, or `fe_h` is outside the range of -4 to 2 | Yes | Yes |

### AstroNN

**Model:** `AstroNN` (`src/astra/models/astronn.py`)

AstroNN estimates stellar labels and chemical abundances from APOGEE spectra.

**BitField:** `result_flags`

| Flag name | Bit value | Description | `flag_warn` | `flag_bad` |
|---|---|---|---|---|
| `flag_uncertain_logg` | 2^0 = 1 | Surface gravity is uncertain (`e_logg` > 0.2 and `abs(e_logg/logg)` > 0.075) | Yes | Yes* |
| `flag_uncertain_teff` | 2^1 = 2 | Effective temperature is uncertain (`e_teff` > 300) | Yes | Yes* |
| `flag_uncertain_fe_h` | 2^2 = 4 | Iron abundance is uncertain (`abs(e_fe_h/fe_h)` > 0.12) | Yes | Yes* |
| `flag_no_result` | 2^11 = 2048 | Exception raised when loading spectra | Yes | |

\*`flag_bad` is set only when **all three** of `flag_uncertain_logg`, `flag_uncertain_teff`, and `flag_uncertain_fe_h` are set simultaneously.

`flag_warn` is set when `result_flags > 0` (any flag is set).

### AstroNNdist

**Model:** `AstroNNdist` (`src/astra/models/astronn_dist.py`)

AstroNN distance pipeline estimates spectrophotometric distances.

**BitField:** `result_flags`

| Flag name | Bit value | Description | `flag_warn` | `flag_bad` |
|---|---|---|---|---|
| `flag_fakemag_unreliable` | 2^0 = 1 | Predicted Ks-band absolute luminosity is unreliable (fakemag_err / fakemag >= 0.2) | | Yes |
| `flag_missing_photometry` | 2^1 = 2 | Missing Ks-band apparent magnitude | | Yes |
| `flag_missing_extinction` | 2^2 = 4 | Missing extinction | Yes | |
| `flag_no_result` | 2^11 = 2048 | Exception raised when loading spectra | | Yes |

### BossNet

**Model:** `BossNet` (`src/astra/models/bossnet.py`)

BOSSNet estimates stellar parameters from BOSS optical spectra using a neural network.

**BitField:** `result_flags`

| Flag name | Bit value | Description | `flag_warn` | `flag_bad` |
|---|---|---|---|---|
| `flag_runtime_exception` | 2^0 = 1 | Exception occurred at runtime | | Yes |
| `flag_unreliable_teff` | 2^1 = 2 | `teff` is outside the range of 1700 K to 100,000 K | | Yes |
| `flag_unreliable_logg` | 2^2 = 4 | `logg` is outside the range of -1 to 10 | | Yes |
| `flag_unreliable_fe_h` | 2^3 = 8 | `teff` < 3200 K, or `logg` > 5, or `fe_h` is outside the range of -4 to 2 | Yes | Yes |
| `flag_suspicious_fe_h` | 2^4 = 16 | [Fe/H] may be suspicious for `teff` < 3900 K with 3 < `logg` < 6 | Yes | |

### ThePayne

**Model:** `ThePayne` (`src/astra/models/the_payne.py`)

The Payne estimates detailed stellar labels by fitting a spectral model to APOGEE spectra.

**BitField:** `result_flags`

| Flag name | Bit value | Description | `flag_warn` | `flag_bad` |
|---|---|---|---|---|
| `flag_fitting_failure` | 2^0 = 1 | Fitting failure | Yes | Yes |
| `flag_warn_teff` | 2^1 = 2 | Teff < 3100 K or Teff > 7900 K | Yes | |
| `flag_warn_logg` | 2^2 = 4 | logg < 0.1 or logg > 5.2 | Yes | |
| `flag_warn_fe_h` | 2^3 = 8 | [Fe/H] > 0.4 or [Fe/H] < -1.4 | Yes | |
| `flag_low_snr` | 2^4 = 16 | S/N < 70 | Yes | |

`flag_warn` is set when `result_flags > 0` (any flag is set).

### TheCannon

**Model:** `TheCannon` (`src/astra/models/the_cannon.py`)

The Cannon estimates stellar labels using a data-driven model trained on a reference set.

**BitField:** `result_flags`

| Flag name | Bit value | Description | `flag_warn` | `flag_bad` |
|---|---|---|---|---|
| `flag_fitting_failure` | 2^0 = 1 | Fitting failure | | |

The Cannon model does not define `flag_warn` or `flag_bad` computed properties. Users should check `flag_fitting_failure` directly.

### Slam

**Model:** `Slam` (`src/astra/models/slam.py`)

The Stellar Labels Machine (SLAM) estimates stellar parameters for M dwarfs from BOSS optical spectra.

**BitField:** `result_flags`

| Flag name | Bit value | Description | `flag_warn` | `flag_bad` |
|---|---|---|---|---|
| `flag_bad_optimizer_status` | 2^0 = 1 | Optimizer returned a bad status | Yes | Yes |
| `flag_teff_outside_bounds` | 2^1 = 2 | Teff < 2800 K or Teff > 4500 K | | Yes |
| `flag_fe_h_outside_bounds` | 2^2 = 4 | [Fe/H] is outside expected bounds | | Yes |
| `flag_outside_photometry_range` | 2^3 = 8 | Star is outside the expected photometry range (BP-RP color or absolute magnitude) | Yes | Yes |
| `flag_not_magnitude_cut` | 2^4 = 16 | Star does not pass the magnitude cut | | |
| `flag_not_carton_match` | 2^5 = 32 | Star does not match the expected carton | | |

### ASPCAP

**Model:** `ASPCAP` (`src/astra/models/aspcap.py`)

The APOGEE Stellar Parameter and Chemical Abundances Pipeline (ASPCAP) uses FERRE to fit synthetic spectra to observed APOGEE spectra.

ASPCAP has multiple BitField columns: `result_flags` for overall quality, `initial_flags` for initial guess provenance, `calibrated_flags` for calibration metadata, `irfm_teff_flags` for IRFM temperature flags, and per-element abundance flags (e.g., `al_h_flags`, `c_h_flags`, etc.).

#### `result_flags`

| Flag name | Bit value | Description | `flag_warn` | `flag_bad` |
|---|---|---|---|---|
| `flag_ferre_fail` | 2^0 = 1 | FERRE failed | Yes | Yes |
| `flag_missing_model_flux` | 2^1 = 2 | Missing model fluxes from FERRE | Yes | Yes |
| `flag_potential_ferre_timeout` | 2^2 = 4 | Potentially impacted by FERRE timeout | Yes | Yes |
| `flag_no_suitable_initial_guess` | 2^3 = 8 | FERRE not executed because there is no suitable initial guess | Yes | Yes |
| `flag_spectrum_io_error` | 2^4 = 16 | Error accessing spectrum pixel data | Yes | Yes |
| `flag_teff_grid_edge_warn` | 2^5 = 32 | Teff is within one step from the grid edge | Yes | |
| `flag_teff_grid_edge_bad` | 2^6 = 64 | Teff is within 1/8th of a step from the grid edge | Yes | Yes |
| `flag_logg_grid_edge_warn` | 2^7 = 128 | logg is within one step from the grid edge | Yes | |
| `flag_logg_grid_edge_bad` | 2^8 = 256 | logg is within 1/8th of a step from the grid edge | Yes | Yes |
| `flag_v_micro_grid_edge_warn` | 2^9 = 512 | v_micro is within one step from the grid edge | Yes | |
| `flag_v_micro_grid_edge_bad` | 2^10 = 1024 | v_micro is within 1/8th of a step from the grid edge | Yes | |
| `flag_v_sini_grid_edge_warn` | 2^11 = 2048 | v_sini is within one step from the highest grid edge | Yes | |
| `flag_v_sini_grid_edge_bad` | 2^12 = 4096 | v_sini is within 1/8th of a step from the highest grid edge | Yes | |
| `flag_m_h_atm_grid_edge_warn` | 2^13 = 8192 | [M/H] is within one step from the grid edge | Yes | |
| `flag_m_h_atm_grid_edge_bad` | 2^14 = 16384 | [M/H] is within 1/8th of a step from the grid edge | Yes | |
| `flag_alpha_m_grid_edge_warn` | 2^15 = 32768 | [alpha/M] is within one step from the grid edge | Yes | |
| `flag_alpha_m_grid_edge_bad` | 2^16 = 65536 | [alpha/M] is within 1/8th of a step from the grid edge | Yes | |
| `flag_c_m_atm_grid_edge_warn` | 2^17 = 131072 | [C/M] is within one step from the grid edge | Yes | |
| `flag_c_m_atm_grid_edge_bad` | 2^18 = 262144 | [C/M] is within 1/8th of a step from the grid edge | Yes | |
| `flag_n_m_atm_grid_edge_warn` | 2^19 = 524288 | [N/M] is within one step from the grid edge | Yes | |
| `flag_n_m_atm_grid_edge_bad` | 2^20 = 1048576 | [N/M] is within 1/8th of a step from the grid edge | Yes | |
| `flag_suspicious_parameters` | 2^21 = 2097152 | Stellar parameters are in a suspicious and low-density region | Yes | Yes |
| `flag_high_v_sini` | 2^22 = 4194304 | High rotational velocity | Yes | Yes |
| `flag_high_v_micro` | 2^23 = 8388608 | v_micro exceeds 3 km/s | Yes | Yes |
| `flag_unphysical_parameters` | 2^24 = 16777216 | FERRE returned unphysical stellar parameters | Yes | Yes |
| `flag_high_rchi2` | 2^25 = 33554432 | Reduced chi-squared is greater than 1000 | Yes | Yes |
| `flag_low_snr` | 2^26 = 67108864 | S/N is less than 20 | Yes | Yes |
| `flag_high_std_v_rad` | 2^27 = 134217728 | Standard deviation of v_rad is greater than 1 km/s | Yes | Yes |

`flag_warn` is set when `result_flags > 0` (any flag is set).

#### `initial_flags`

These flags record the provenance of the initial guess used for the FERRE fit:

| Flag name | Bit value | Description |
|---|---|---|
| `flag_initial_guess_from_apogeenet` | 2^0 = 1 | Initial guess from APOGEENet |
| `flag_initial_guess_from_doppler` | 2^1 = 2 | Initial guess from Doppler (SDSS-V) |
| `flag_initial_guess_from_doppler_sdss4` | 2^1 = 2 | Initial guess from Doppler (SDSS-IV) |
| `flag_initial_guess_from_user` | 2^2 = 4 | Initial guess specified by user |
| `flag_initial_guess_from_gaia_xp_andrae23` | 2^3 = 8 | Initial guess from Andrae et al. (2023) |

#### `irfm_teff_flags`

These flags apply to the IRFM effective temperature from V-Ks (Gonzalez Hernandez and Bonifacio 2009):

| Flag name | Bit value | Description |
|---|---|---|
| `flag_out_of_v_k_bounds` | 2^0 = 1 | Out of V-Ks bounds |
| `flag_out_of_fe_h_bounds` | 2^1 = 2 | Out of [Fe/H] bounds |
| `flag_extrapolated_v_mag` | 2^2 = 4 | Synthetic V magnitude is extrapolated |
| `flag_poor_quality_k_mag` | 2^3 = 8 | Poor quality Ks magnitude |
| `flag_ebv_used_is_upper_limit` | 2^4 = 16 | E(B-V) used is an upper limit |
| `flag_as_dwarf_for_irfm_teff` | 2^5 = 32 | Flagged as dwarf for IRFM temperature |
| `flag_as_giant_for_irfm_teff` | 2^6 = 64 | Flagged as giant for IRFM temperature |

#### `calibrated_flags`

These flags record calibration decisions:

| Flag name | Bit value | Description |
|---|---|---|
| `flag_as_dwarf_for_calibration` | 2^0 = 1 | Classified as main-sequence star for logg calibration |
| `flag_as_giant_for_calibration` | 2^1 = 2 | Classified as red giant branch star for logg calibration |
| `flag_as_red_clump_for_calibration` | 2^2 = 4 | Classified as red clump star for logg calibration |
| `flag_as_m_dwarf_for_calibration` | 2^3 = 8 | Classified as M-dwarf for teff and logg calibration |
| `flag_censored_logg_for_metal_poor_m_dwarf` | 2^4 = 16 | Censored logg for metal-poor ([M/H] < -0.6) M-dwarf |

#### Per-element abundance flags

ASPCAP provides individual chemical abundance flags for each element measured. These are stored in separate BitField columns named `<element>_flags` (e.g., `al_h_flags`, `c_h_flags`, `fe_h_flags`). Elements with abundance flags include: Al, C12/C13, Ca, Ce, C, Co, Cr, Cu, Fe, K, Mg, Mn, Na, Nd, Ni, N, O, P, Si, S, Ti, and V.

All per-element abundance flag fields share a common pattern of bit definitions:

**Upper limit flags (for elements with weak lines):** Some elements (Ce, C, Cu, Na, Nd, N, O, P, S, V) include upper limit flags at bits 0--4:

| Bit value | Description |
|---|---|
| 2^0 = 1 | At least one line is an upper limit by the 1% threshold (Hayes et al. 2022) |
| 2^1 = 2 | At least one line is an upper limit by the 2% threshold |
| 2^2 = 4 | At least one line is an upper limit by the 3% threshold |
| 2^3 = 8 | At least one line is an upper limit by the 4% threshold |
| 2^4 = 16 | At least one line is an upper limit by the 5% threshold |

**Standard abundance quality flags (all elements):**

| Bit value | Description |
|---|---|
| 2^5 = 32 | Censored value because abundances known to be wrong for this Teff |
| 2^6 = 64 | Censored value because of low Teff and v_micro |
| 2^7 = 128 | Censored value because FERRE returned unphysical value |
| 2^8 = 256 | Grid edge bad |
| 2^9 = 512 | Grid edge warning |
| 2^10 = 1024 | Abundances known to be unreliable for this Teff |
| 2^11 = 2048 | Abundances known to be unreliable for this [M/H] |

### FERRE (intermediate models)

**Models:** `FerreCoarse`, `FerreStellarParameters`, `FerreChemicalAbundances` (`src/astra/models/ferre.py`)

These are intermediate FERRE models used internally by ASPCAP. They share similar flag structures.

#### `initial_flags`

Records how the initial guess was determined (same pattern as ASPCAP `initial_flags`, with additional flags for Gaia XP sources):

| Flag name | Bit value | Description |
|---|---|---|
| `flag_initial_guess_from_apogeenet` | 2^0 = 1 | Initial guess from APOGEENet |
| `flag_initial_guess_from_doppler` | 2^1 = 2 | Initial guess from Doppler (SDSS-V) |
| `flag_initial_guess_from_doppler_sdss4` | 2^2 = 4 | Initial guess from Doppler (SDSS-IV) |
| `flag_initial_guess_from_gaia_xp_andrae_2023` | 2^3 = 8 | Initial guess from Andrae et al. (2023) |
| `flag_initial_guess_from_gaia_xp_zhang_2023` | 2^4 = 16 | Initial guess from Zhang, Green & Rix (2023) |
| `flag_initial_guess_from_user` | 2^5 = 32 | Initial guess specified by user |
| `flag_initial_guess_at_grid_center` | 2^6 = 64 | Initial guess from grid center |

#### `frozen_flags`

Records which FERRE dimensions were held fixed during fitting:

| Flag name | Bit value | Description |
|---|---|---|
| `flag_teff_frozen` | 2^0 = 1 | Effective temperature is frozen |
| `flag_logg_frozen` | 2^1 = 2 | Surface gravity is frozen |
| `flag_m_h_frozen` | 2^2 = 4 | [M/H] is frozen |
| `flag_log10_v_sini_frozen` | 2^3 = 8 | Rotational broadening is frozen |
| `flag_log10_v_micro_frozen` | 2^4 = 16 | Microturbulence is frozen |
| `flag_alpha_m_frozen` | 2^5 = 32 | [alpha/M] is frozen |
| `flag_c_m_frozen` | 2^6 = 64 | [C/M] is frozen |
| `flag_n_m_frozen` | 2^7 = 128 | [N/M] is frozen |

#### `ferre_flags`

Records FERRE execution status and grid edge conditions:

| Flag name | Bit value | Description |
|---|---|---|
| `flag_ferre_fail` | 2^0 = 1 | FERRE failed |
| `flag_missing_model_flux` | 2^1 = 2 | Missing model fluxes from FERRE |
| `flag_potential_ferre_timeout` | 2^2 = 4 | Potentially impacted by FERRE timeout |
| `flag_no_suitable_initial_guess` | 2^3 = 8 | No suitable initial guess available |
| `flag_spectrum_io_error` | 2^4 = 16 | Error accessing spectrum pixel data |
| `flag_teff_grid_edge_warn` | 2^5 = 32 | Teff near grid edge (warning) |
| `flag_teff_grid_edge_bad` | 2^6 = 64 | Teff at grid edge (bad) |
| `flag_logg_grid_edge_warn` | 2^7 = 128 | logg near grid edge (warning) |
| `flag_logg_grid_edge_bad` | 2^8 = 256 | logg at grid edge (bad) |
| `flag_v_micro_grid_edge_warn` | 2^9 = 512 | v_micro near grid edge (warning) |
| `flag_v_micro_grid_edge_bad` | 2^10 = 1024 | v_micro at grid edge (bad) |
| `flag_v_sini_grid_edge_warn` | 2^11 = 2048 | v_sini near grid edge (warning) |
| `flag_v_sini_grid_edge_bad` | 2^12 = 4096 | v_sini at grid edge (bad) |
| `flag_m_h_atm_grid_edge_warn` | 2^13 = 8192 | [M/H] near grid edge (warning) |
| `flag_m_h_atm_grid_edge_bad` | 2^14 = 16384 | [M/H] at grid edge (bad) |
| `flag_alpha_m_grid_edge_warn` | 2^15 = 32768 | [alpha/M] near grid edge (warning) |
| `flag_alpha_m_grid_edge_bad` | 2^16 = 65536 | [alpha/M] at grid edge (bad) |
| `flag_c_m_atm_grid_edge_warn` | 2^17 = 131072 | [C/M] near grid edge (warning) |
| `flag_c_m_atm_grid_edge_bad` | 2^18 = 262144 | [C/M] at grid edge (bad) |
| `flag_n_m_atm_grid_edge_warn` | 2^19 = 524288 | [N/M] near grid edge (warning) |
| `flag_n_m_atm_grid_edge_bad` | 2^20 = 1048576 | [N/M] at grid edge (bad) |
| `flag_caused_timeout` | 2^21 = 2097152 | Caused timeout in downstream tasks |
| `flag_affected_by_timeout` | 2^22 = 4194304 | Affected by timeout |
| `flag_multiple_equally_good_coarse_results` | 2^23 = 8388608 | Multiple equally good coarse results |
| `flag_no_good_coarse_result` | 2^24 = 16777216 | No good result from coarse grid |

### SnowWhite

**Model:** `SnowWhite` (`src/astra/models/snow_white.py`)

The white dwarf pipeline (affectionately known as Snow White) classifies white dwarf spectra and estimates Teff and logg.

**BitField:** `result_flags`

| Flag name | Bit value | Description |
|---|---|---|
| `flag_low_snr` | 2^0 = 1 | Results are suspect because S/N <= 8 |
| `flag_unconverged` | 2^1 = 2 | Fit did not converge |
| `flag_teff_grid_edge_bad` | 2^2 = 4 | Teff is at edge of grid |
| `flag_logg_grid_edge_bad` | 2^3 = 8 | logg is at edge of grid |
| `flag_no_flux` | 2^4 = 16 | Spectrum has no flux |
| `flag_not_mwm_wd` | 2^5 = 32 | Object is not in the `mwm_wd` program |
| `flag_missing_bp_rp_mag` | 2^6 = 64 | Missing Gaia BP-RP color for prior on Teff |

SnowWhite does not define `flag_warn` or `flag_bad` computed properties. Users should inspect individual flags.

### MDwarfType

**Model:** `MDwarfType` (`src/astra/models/mdwarftype.py`)

M-dwarf spectral type classifier.

**BitField:** `result_flags`

| Flag name | Bit value | Description | `flag_warn` | `flag_bad` |
|---|---|---|---|---|
| `flag_suspicious` | 2^0 = 1 | Spectral type is K5.0, suspicious for an M dwarf | | Yes |
| `flag_exception` | 2^1 = 2 | Runtime exception during processing | | Yes |

`flag_bad` is set when `result_flags > 0` (any flag is set). No `flag_warn` is defined.

### Corv

**Model:** `Corv` (`src/astra/models/corv.py`)

The `corv` pipeline estimates radial velocities and stellar parameters for DA-type white dwarfs.

**BitField:** `result_flags`

| Flag name | Bit value | Description |
|---|---|---|
| `flag_not_mwm_wd` | 2^5 = 32 | Object is not in the `mwm_wd` program |
| `flag_no_wd_classification` | 2^6 = 64 | No SnowWhite classification available |
| `flag_not_da_type` | 2^7 = 128 | Object is not classified as DA-type by SnowWhite |

Corv does not define `flag_warn` or `flag_bad` computed properties. Users should inspect individual flags.

### LineForest

**Model:** `LineForest` (`src/astra/models/line_forest.py`)

LineForest measures equivalent widths and absorption depths for a large set of spectral lines. LineForest does not define any BitField flags or `result_flags` column. Quality information is communicated through the `detection_stat_*` and `detection_raw_*` fields for each line.

### Clam

**Model:** `Clam` (`src/astra/models/clam.py`)

The Clam pipeline estimates stellar labels.

**BitField:** `result_flags`

| Flag name | Bit value | Description |
|---|---|---|
| `flag_spectrum_io_error` | 2^0 = 1 | Spectrum I/O error |
| `flag_runtime_error` | 2^1 = 2 | Runtime error |

Clam does not define `flag_warn` or `flag_bad` computed properties.

### NMFRectify

**Model:** `NMFRectify` (`src/astra/models/nmf_rectify.py`)

NMF-based continuum rectification.

**BitField:** `nmf_flags`

| Flag name | Bit value | Description |
|---|---|---|
| `flag_initialised_from_small_w` | 2^0 = 1 | Initialised from small W |
| `flag_could_not_read_spectrum` | 2^3 = 8 | Could not read spectrum |
| `flag_runtime_exception` | 2^4 = 16 | Runtime exception |

NMFRectify does not define `flag_warn` or `flag_bad` computed properties.

## Working with flags

### Checking if a flag is set

```python
from astra.models.apogeenet import ApogeeNet

# Get a single result
result = ApogeeNet.get_by_id(12345)

# Check individual flags
print(result.flag_unreliable_teff)   # True or False
print(result.flag_unreliable_logg)   # True or False

# Check summary flags
print(result.flag_warn)              # True or False
print(result.flag_bad)               # True or False

# Check the raw integer value
print(result.result_flags)           # e.g. 6 means bits 1 and 2 are set
```

### Filtering results by flags

The `flag_warn` and `flag_bad` properties work in database queries because they are hybrid properties:

```python
from astra.models.apogeenet import ApogeeNet

# Get all results that are not flagged as bad
good_results = (
    ApogeeNet
    .select()
    .where(~ApogeeNet.flag_bad)
)

# Get all results with warnings
warned_results = (
    ApogeeNet
    .select()
    .where(ApogeeNet.flag_warn)
)

# Filter on a specific flag
unreliable_teff = (
    ApogeeNet
    .select()
    .where(ApogeeNet.flag_unreliable_teff)
)

# Combine with other conditions
quality_results = (
    ApogeeNet
    .select()
    .where(
        (~ApogeeNet.flag_bad)
    &   (ApogeeNet.teff > 4000)
    &   (ApogeeNet.teff < 6000)
    )
)
```

### Decoding the integer value into individual flags

You can decode a `result_flags` integer value by checking each bit:

```python
from astra.models.apogeenet import ApogeeNet

result = ApogeeNet.get_by_id(12345)
flags_int = result.result_flags

# Check each bit manually
flag_definitions = {
    0: "flag_runtime_exception",
    1: "flag_unreliable_teff",
    2: "flag_unreliable_logg",
    3: "flag_unreliable_fe_h",
}

active_flags = []
for bit, name in flag_definitions.items():
    if flags_int & (2 ** bit):
        active_flags.append(name)

print(f"result_flags = {flags_int}")
print(f"Active flags: {active_flags}")
# e.g. result_flags = 6
# Active flags: ['flag_unreliable_teff', 'flag_unreliable_logg']
```

You can also check flags directly through the model attributes, which is usually simpler:

```python
result = ApogeeNet.get_by_id(12345)
for attr in ("flag_runtime_exception", "flag_unreliable_teff",
             "flag_unreliable_logg", "flag_unreliable_fe_h"):
    if getattr(result, attr):
        print(f"{attr} is set")
```
