# FERRE

FERRE (Fitting Evolutionary and Resolved Radial-velocity Estimates) is a grid-based
spectral fitting code that serves as the backend for the [ASPCAP](aspcap) pipeline. In
Astra, FERRE is used in a multi-stage process to determine stellar parameters and
individual chemical abundances from APOGEE spectra.

## What it does

FERRE fits observed APOGEE spectra against pre-computed grids of synthetic spectra to
determine:

- Effective temperature (`teff`)
- Surface gravity (`logg`)
- Overall metallicity (`m_h`)
- Log10 microturbulence (`log10_v_micro`)
- Log10 projected rotational velocity (`log10_v_sini`)
- Alpha-element abundance (`alpha_m`)
- Carbon abundance (`c_m`)
- Nitrogen abundance (`n_m`)

Chemical abundances for individual elements are determined in a separate abundance stage.

## How it works

FERRE operates in three stages within Astra:

### 1. Coarse grid search (`FerreCoarse`)

An initial coarse search identifies the best-matching spectral grid and approximate
stellar parameters. Initial guesses can come from several sources (flagged in
`initial_flags`):

- APOGEENet neural network predictions
- Doppler pipeline (SDSS-V or SDSS-IV)
- Gaia XP-based estimates (Andrae 2023 or Zhang, Green & Rix 2023)
- User-specified values
- Grid center (fallback)

### 2. Stellar parameter refinement (`FerreStellarParameters`)

Starting from the coarse result, FERRE refines all stellar parameters simultaneously.
The upstream coarse result is tracked via the `upstream` foreign key. Parameters can
be selectively frozen during the fit (tracked in `frozen_flags`).

### 3. Chemical abundances (`FerreChemicalAbundances`)

With stellar parameters fixed from the previous stage, individual element abundances
are determined one at a time by fitting restricted wavelength windows. Each element
has its own FERRE run with most parameters frozen.

### FERRE execution

FERRE is executed as an external Fortran binary. Astra manages:
- Preparing input files (spectra, uncertainties, initial guesses, control parameters)
- Submitting FERRE jobs (optionally via Slurm on HPC clusters)
- Parsing output files (best-fit parameters, model spectra, chi-squared values)
- Ingesting results back into the database

### Continuum handling

FERRE can perform its own continuum normalization, controlled by `continuum_order`,
`continuum_flag`, and `continuum_observations_flag`. The polynomial continuum order,
rejection threshold, and whether to normalize observations, model, or both are all
configurable.

## Output fields

All three stages share the same set of core output fields:

### Stellar parameters

| Field | Description |
| --- | --- |
| `teff` | Effective temperature (K) |
| `e_teff` | Uncertainty on Teff |
| `logg` | Surface gravity (log cm/s^2) |
| `e_logg` | Uncertainty on log(g) |
| `m_h` | Overall metallicity [M/H] (dex) |
| `e_m_h` | Uncertainty on [M/H] |
| `log10_v_sini` | Log10 projected rotational velocity |
| `e_log10_v_sini` | Uncertainty on log10(v sin i) |
| `log10_v_micro` | Log10 microturbulent velocity |
| `e_log10_v_micro` | Uncertainty on log10(v_micro) |
| `alpha_m` | Alpha-element abundance [alpha/M] (dex) |
| `e_alpha_m` | Uncertainty on [alpha/M] |
| `c_m` | Carbon abundance [C/M] (dex) |
| `e_c_m` | Uncertainty on [C/M] |
| `n_m` | Nitrogen abundance [N/M] (dex) |
| `e_n_m` | Uncertainty on [N/M] |

### Initial values and settings

| Field | Description |
| --- | --- |
| `initial_*` | Initial guess for each parameter |
| `short_grid_name` | Name of the FERRE grid used |
| `header_path` | Path to the grid header file |
| `pwd` | Working directory for the FERRE run |
| `continuum_order` | Polynomial order for continuum normalization |
| `interpolation_order` | Interpolation order used in the grid |

### Summary statistics

| Field | Description |
| --- | --- |
| `snr` | Signal-to-noise ratio of the input spectrum |
| `rchi2` | Reduced chi-squared of the fit |
| `penalized_rchi2` | Penalized reduced chi-squared |
| `ferre_log_snr_sq` | FERRE's internal log(SNR^2) |
| `ferre_time_load_grid` | Time to load the spectral grid (seconds) |
| `ferre_time_elapsed` | Total FERRE execution time (seconds) |

### FERRE access

| Field | Description |
| --- | --- |
| `ferre_name` | Internal FERRE spectrum identifier |
| `ferre_index` | Index of this spectrum in the FERRE output files |
| `ferre_n_obj` | Total number of objects in the FERRE run |

## Flags

### `initial_flags` (source of initial guess)

| Flag | Bit | Meaning |
| --- | --- | --- |
| `flag_initial_guess_from_apogeenet` | 2^0 | Initial guess from APOGEENet |
| `flag_initial_guess_from_doppler` | 2^1 | Initial guess from Doppler (SDSS-V) |
| `flag_initial_guess_from_doppler_sdss4` | 2^2 | Initial guess from Doppler (SDSS-IV) |
| `flag_initial_guess_from_gaia_xp_andrae_2023` | 2^3 | Initial guess from Andrae et al. (2023) |
| `flag_initial_guess_from_gaia_xp_zhang_2023` | 2^4 | Initial guess from Zhang, Green & Rix (2023) |
| `flag_initial_guess_from_user` | 2^5 | Initial guess specified by user |
| `flag_initial_guess_at_grid_center` | 2^6 | Initial guess from grid center |

### `frozen_flags` (frozen parameters)

| Flag | Bit | Meaning |
| --- | --- | --- |
| `flag_teff_frozen` | 2^0 | Teff was held fixed |
| `flag_logg_frozen` | 2^1 | log(g) was held fixed |
| `flag_m_h_frozen` | 2^2 | [M/H] was held fixed |
| `flag_log10_v_sini_frozen` | 2^3 | v sin i was held fixed |
| `flag_log10_v_micro_frozen` | 2^4 | v_micro was held fixed |
| `flag_alpha_m_frozen` | 2^5 | [alpha/M] was held fixed |
| `flag_c_m_frozen` | 2^6 | [C/M] was held fixed |
| `flag_n_m_frozen` | 2^7 | [N/M] was held fixed |

### `ferre_flags` (processing and quality flags)

| Flag | Bit | Meaning |
| --- | --- | --- |
| `flag_ferre_fail` | 2^0 | FERRE failed to produce a result |
| `flag_missing_model_flux` | 2^1 | Model flux output is missing |
| `flag_potential_ferre_timeout` | 2^2 | Result may be affected by a FERRE timeout |
| `flag_no_suitable_initial_guess` | 2^3 | No suitable initial guess was available |
| `flag_spectrum_io_error` | 2^4 | Error reading the input spectrum |
| `flag_teff_grid_edge_warn` | 2^5 | Teff near grid edge (warning) |
| `flag_teff_grid_edge_bad` | 2^6 | Teff at grid edge (bad) |
| `flag_logg_grid_edge_warn` | 2^7 | log(g) near grid edge (warning) |
| `flag_logg_grid_edge_bad` | 2^8 | log(g) at grid edge (bad) |
| `flag_v_micro_grid_edge_warn` | 2^9 | v_micro near grid edge (warning) |
| `flag_v_micro_grid_edge_bad` | 2^10 | v_micro at grid edge (bad) |
| `flag_v_sini_grid_edge_warn` | 2^11 | v_sini near grid edge (warning) |
| `flag_v_sini_grid_edge_bad` | 2^12 | v_sini at grid edge (bad) |
| `flag_m_h_atm_grid_edge_warn` | 2^13 | [M/H] near grid edge (warning) |
| `flag_m_h_atm_grid_edge_bad` | 2^14 | [M/H] at grid edge (bad) |
| `flag_alpha_m_grid_edge_warn` | 2^15 | [alpha/M] near grid edge (warning) |
| `flag_alpha_m_grid_edge_bad` | 2^16 | [alpha/M] at grid edge (bad) |
| `flag_c_m_atm_grid_edge_warn` | 2^17 | [C/M] near grid edge (warning) |
| `flag_c_m_atm_grid_edge_bad` | 2^18 | [C/M] at grid edge (bad) |
| `flag_n_m_atm_grid_edge_warn` | 2^19 | [N/M] near grid edge (warning) |
| `flag_n_m_atm_grid_edge_bad` | 2^20 | [N/M] at grid edge (bad) |
| `flag_caused_timeout` | 2^21 | This spectrum caused a timeout in downstream tasks |
| `flag_affected_by_timeout` | 2^22 | This spectrum was affected by a timeout |
| `flag_multiple_equally_good_coarse_results` | 2^23 | Multiple coarse grid results had similar chi-squared |
| `flag_no_good_coarse_result` | 2^24 | No good result found in the coarse grid search |

Grid edge flags come in `_warn` and `_bad` pairs. The `_warn` flag indicates the
parameter is near the grid boundary; `_bad` indicates it is at or beyond the boundary.

## Pixel-level data

FERRE results include pixel-level data that can be accessed through the result object:

- `ferre_flux`: Input flux array
- `ferre_e_flux`: Input flux uncertainty array
- `model_flux`: Best-fit model flux
- `rectified_model_flux`: Continuum-normalized model flux
- `rectified_flux`: Continuum-normalized observed flux

These arrays use the APOGEE pixel mask (7514 pixels) and can be unmasked to the full
8575-pixel grid using the `unmask()` method.

## Key caveats

- FERRE is an external Fortran code. It must be installed and accessible on the system.
- Grid edge flags should be checked carefully. Parameters at grid boundaries are
  constrained by the grid limits and may be biased.
- The three-stage process (coarse, stellar parameters, abundances) means that errors
  can propagate: a poor coarse result can lead to a poor stellar parameter fit, which
  in turn affects abundance measurements.
- Timeout flags indicate that FERRE hit a time limit. Results for affected spectra may
  be incomplete or unreliable.
- The `penalized_rchi2` includes penalties for parameters near grid edges, providing a
  more conservative quality metric than `rchi2` alone.
