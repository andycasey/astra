# ASPCAP

The APOGEE Stellar Parameter and Chemical Abundances Pipeline (ASPCAP) determines
stellar parameters and chemical abundances from APOGEE spectra. It uses
[FERRE](https://github.com/callendeprieto/ferre) to perform chi-squared
minimization against pre-computed grids of synthetic spectra, following a
multi-stage fitting strategy: coarse parameter determination, refined stellar
parameter determination, and individual chemical abundance measurement.

## How it works

ASPCAP runs in three sequential stages, each using FERRE to fit observed spectra
against grids of synthetic spectra computed with the Synspec radiative transfer
code and MARCS model atmospheres.

### Stage 1: Coarse stellar parameter determination

Each input spectrum is dispatched to one or more synthetic spectral grids based on
an initial guess of the stellar parameters. Initial guesses come from upstream
pipelines, tried in this order:

1. **APOGEENet** -- a neural network that provides Teff, log(g), and [Fe/H]
   estimates from APOGEE spectra.
2. **Gaia XP (Zhang et al. 2023)** -- photometric parameters from Gaia XP spectra,
   used when APOGEENet results are not available and Gaia XP quality flags are
   clean.

If neither source provides a usable initial guess, the spectrum is sent to all
grids starting from the grid centers.

The grid selection logic considers:

- **Spectral type**: GK (cool giants/dwarfs), F (intermediate), or BA (hot stars).
- **Luminosity class**: giant (`g`) or dwarf (`d`).
- **LSF model**: the line spread function varies across the detector, so grids are
  grouped by fiber number into four bins (a, b, c, d) corresponding to fiber
  ranges 245--300, 145--245, 50--145, and 1--50, respectively.
- **Telescope**: APO 2.5m, LCO 2.5m, or APO 1m (treated as APO 2.5m for grid
  selection).

During the coarse stage, some parameters are frozen depending on the grid:

- For non-BA grids: [C/M] and [N/M] are frozen.
- For F-dwarf grids: [alpha/M] is additionally frozen.

A global pixel mask (`global.mask`) is applied as the FERRE weight file.

After FERRE runs on all grids, the best coarse result per spectrum is selected by
**penalized reduced chi-squared**. Penalties are applied when:

- The result is near a grid edge in Teff or log(g) (warn: 5x, bad: 10x).
- FERRE returned a failure code for Teff or log(g) (20x).
- A cool star (Teff < 3900 K) was fit on the GK grid (10x).

### Stage 2: Refined stellar parameter determination

Using the best coarse result to select a single grid and provide initial guesses,
FERRE re-fits all stellar parameters simultaneously with better continuum
normalization.

Before this stage, a **median filter continuum** correction is applied. This
compares the FERRE-rectified observed and model fluxes from the coarse stage,
computes their ratio, interpolates over bad pixels, and applies a 151-pixel
median filter to derive a smooth continuum correction. The three APOGEE detector
regions (15152--15800, 15867--16424, and 16484--16944 Angstroms) are treated
independently.

All parameters (Teff, log(g), [M/H], [alpha/M], [C/M], [N/M], log10(v_micro),
log10(v_sini)) are free to vary in this stage.

### Stage 3: Chemical abundance determination

With the stellar parameters fixed to the values from Stage 2, ASPCAP measures
individual elemental abundances one species at a time. Each species has its own
pixel weight mask that isolates the relevant spectral features, and a specific
configuration of which FERRE label dimensions are free to vary.

The continuum is held fixed to the solution from Stage 2 (continuum order = -1,
no re-normalization).

Elements are measured by varying either the overall metallicity [M/H], the alpha
abundance [alpha/M], [C/M], or [N/M] dimension -- depending on which grid
dimension best represents the element. For elements that map onto the [M/H]
dimension (e.g., Fe, Na, Al, Cr, Mn, Ni, Co, Cu, K, V, Ce, Nd, P), the other
abundance dimensions ([C/M], [N/M], [alpha/M]) are tied back to the [M/H]
dimension during the fit.

Some abundances are reported relative to hydrogen [X/H], while others are
measured as [X/M] and converted to [X/H] by adding [M/H]:

- **Measured relative to H**: Na, Al, P, K, V, Cr, Mn, Fe, Co, Ni, Cu, Ce, Nd
- **Measured relative to M (converted to [X/H])**: C, N, O, Mg, Si, S, Ca, Ti, C12/13

## Configuration

The main entry point is the `aspcap` task function. Key parameters:

| Parameter | Default | Description |
| --- | --- | --- |
| `header_paths` | `$MWM_ASTRA/pipelines/aspcap/synspec_dr17_marcs_header_paths.list` | File listing paths to FERRE grid header files |
| `weight_path` | `$MWM_ASTRA/pipelines/aspcap/masks/global.mask` | Pixel weight mask for coarse and stellar parameter stages |
| `element_weight_paths` | `$MWM_ASTRA/pipelines/aspcap/masks/elements.list` | File listing per-element pixel masks for the abundance stage |
| `n_threads` | 32 | Number of threads per FERRE process |
| `max_processes` | 16 | Maximum concurrent FERRE processes |
| `max_threads` | 128 | Soft limit on total threads across all processes |
| `max_concurrent_loading` | 4 | Maximum number of grids loading simultaneously (prevents disk I/O bottleneck) |
| `use_ferre_list_mode` | True | Use FERRE list mode (`-l`) for the abundance stage |
| `continuum_order` | 4 | Polynomial order for FERRE continuum normalization (coarse and params stages) |
| `continuum_reject` | 0.3 | Sigma-clipping tolerance for FERRE continuum fitting |

## Output fields

### Stellar parameters

| Field | Description |
| --- | --- |
| `teff`, `e_teff` | Effective temperature and uncertainty [K] |
| `logg`, `e_logg` | Surface gravity and uncertainty [dex] |
| `v_micro`, `e_v_micro` | Microturbulent velocity and uncertainty [km/s] |
| `v_sini`, `e_v_sini` | Projected rotational velocity and uncertainty [km/s] |
| `m_h_atm`, `e_m_h_atm` | Overall metallicity [M/H] and uncertainty [dex] |
| `alpha_m_atm`, `e_alpha_m_atm` | Alpha-element abundance [alpha/M] and uncertainty [dex] |
| `c_m_atm`, `e_c_m_atm` | Carbon abundance [C/M] and uncertainty [dex] |
| `n_m_atm`, `e_n_m_atm` | Nitrogen abundance [N/M] and uncertainty [dex] |

Note that `v_micro` and `v_sini` are fit internally as log10(v_micro) and
log10(v_sini), then converted to linear units in the output. The reported
uncertainties account for this transformation.

### Chemical abundances

ASPCAP reports abundances for the following species, all as [X/H] in dex. Each
abundance has a corresponding uncertainty (`e_*`), reduced chi-squared (`*_rchi2`),
and flag bit field (`*_flags`):

| Field | Element |
| --- | --- |
| `al_h` | Aluminium |
| `c_h` | Carbon |
| `c_12_13` | Carbon isotope ratio (12C/13C) |
| `ca_h` | Calcium |
| `ce_h` | Cerium |
| `co_h` | Cobalt |
| `cr_h` | Chromium |
| `cu_h` | Copper |
| `fe_h` | Iron |
| `k_h` | Potassium |
| `mg_h` | Magnesium |
| `mn_h` | Manganese |
| `na_h` | Sodium |
| `nd_h` | Neodymium |
| `ni_h` | Nickel |
| `n_h` | Nitrogen |
| `o_h` | Oxygen |
| `p_h` | Phosphorus |
| `si_h` | Silicon |
| `s_h` | Sulphur |
| `ti_h` | Titanium |
| `v_h` | Vanadium |

### IRFM effective temperature

An independent effective temperature estimate is provided from the infrared flux
method (IRFM) using V-Ks photometry, following Gonzalez Hernandez and Bonifacio
(2009):

| Field | Description |
| --- | --- |
| `irfm_teff` | IRFM effective temperature [K] |
| `irfm_teff_flags` | Bit field flagging issues with the IRFM estimate |

### Raw (uncalibrated) quantities

All calibrated output fields have corresponding `raw_*` fields containing the
uncalibrated FERRE outputs (e.g., `raw_teff`, `raw_logg`, `raw_fe_h`). These
are preserved so that the effect of post-processing calibrations can be assessed.

### Coarse stage results

The coarse stage results are preserved for diagnostic purposes:

| Field | Description |
| --- | --- |
| `coarse_teff`, `coarse_logg`, etc. | Best-fit parameters from the coarse grid |
| `coarse_rchi2` | Reduced chi-squared from the coarse fit |
| `coarse_penalized_rchi2` | Penalized reduced chi-squared used for grid selection |
| `short_grid_name` | Name of the selected FERRE grid |

### Other fields

| Field | Description |
| --- | --- |
| `rchi2` | Reduced chi-squared of the stellar parameter fit |
| `ferre_log_snr_sq` | FERRE-reported log10(SNR^2) |
| `ferre_time_coarse` | Compute time for the coarse stage [s] |
| `ferre_time_params` | Compute time for the stellar parameter stage [s] |
| `ferre_time_abundances` | Compute time for the abundance stage [s] |
| `continuum_order` | Polynomial continuum order used by FERRE |
| `interpolation_order` | Interpolation order used by FERRE |
| `mass` | Mass inferred from isochrones [M_sun] |
| `radius` | Radius inferred from isochrones [R_sun] |

## Flags

### Result flags (`result_flags`)

These flags indicate issues with the stellar parameter fit. The convenience
properties `flag_warn` (any flag set) and `flag_bad` (serious issues) can be used
to filter results.

| Flag | Bit | Description | Severity |
| --- | --- | --- | --- |
| `flag_ferre_fail` | 0 | FERRE optimization failed | BAD |
| `flag_missing_model_flux` | 1 | Model flux array not returned by FERRE | BAD |
| `flag_potential_ferre_timeout` | 2 | Result may be affected by a FERRE timeout | BAD |
| `flag_no_suitable_initial_guess` | 3 | No initial guess available; FERRE was not executed | BAD |
| `flag_spectrum_io_error` | 4 | Error reading spectrum pixel data | BAD |
| `flag_teff_grid_edge_warn` | 5 | Teff within one grid step of the edge | WARN |
| `flag_teff_grid_edge_bad` | 6 | Teff within 1/8 of a grid step from the edge | BAD |
| `flag_logg_grid_edge_warn` | 7 | log(g) within one grid step of the edge | WARN |
| `flag_logg_grid_edge_bad` | 8 | log(g) within 1/8 of a grid step from the edge | BAD |
| `flag_v_micro_grid_edge_warn` | 9 | v_micro within one step of the grid edge | WARN |
| `flag_v_micro_grid_edge_bad` | 10 | v_micro within 1/8 step of the grid edge | WARN |
| `flag_v_sini_grid_edge_warn` | 11 | v_sini within one step of the highest grid edge | WARN |
| `flag_v_sini_grid_edge_bad` | 12 | v_sini within 1/8 step of the highest grid edge | WARN |
| `flag_m_h_atm_grid_edge_warn` | 13 | [M/H] within one step of the grid edge | WARN |
| `flag_m_h_atm_grid_edge_bad` | 14 | [M/H] within 1/8 step of the grid edge | WARN |
| `flag_alpha_m_grid_edge_warn` | 15 | [alpha/M] within one step of the grid edge | WARN |
| `flag_alpha_m_grid_edge_bad` | 16 | [alpha/M] within 1/8 step of the grid edge | WARN |
| `flag_c_m_atm_grid_edge_warn` | 17 | [C/M] within one step of the grid edge | WARN |
| `flag_c_m_atm_grid_edge_bad` | 18 | [C/M] within 1/8 step of the grid edge | WARN |
| `flag_n_m_atm_grid_edge_warn` | 19 | [N/M] within one step of the grid edge | WARN |
| `flag_n_m_atm_grid_edge_bad` | 20 | [N/M] within 1/8 step of the grid edge | WARN |
| `flag_suspicious_parameters` | 21 | Parameters in a suspicious, low-density region of parameter space | BAD |
| `flag_high_v_sini` | 22 | High rotational velocity | BAD |
| `flag_high_v_micro` | 23 | Microturbulence exceeds 3 km/s | BAD |
| `flag_unphysical_parameters` | 24 | FERRE returned unphysical parameters (e.g., log(g) < -0.5, Teff < 0) | BAD |
| `flag_high_rchi2` | 25 | Reduced chi-squared greater than 1000 | BAD |
| `flag_low_snr` | 26 | Signal-to-noise ratio less than 20 | BAD |
| `flag_high_std_v_rad` | 27 | Standard deviation of radial velocity greater than 1 km/s | BAD |

### Initial guess flags (`initial_flags`)

These track the origin of the initial parameter guess:

| Flag | Description |
| --- | --- |
| `flag_initial_guess_from_apogeenet` | Initial guess from the APOGEENet pipeline |
| `flag_initial_guess_from_gaia_xp_andrae23` | Initial guess from Gaia XP (Andrae et al. 2023) |
| `flag_initial_guess_from_doppler` | Initial guess from the Doppler pipeline (SDSS-V) |
| `flag_initial_guess_from_user` | Initial guess specified by the user |

### Calibration flags (`calibrated_flags`)

Post-processing calibration flags:

| Flag | Description |
| --- | --- |
| `flag_as_dwarf_for_calibration` | Classified as a main-sequence star for log(g) calibration |
| `flag_as_giant_for_calibration` | Classified as a red giant branch star for log(g) calibration |
| `flag_as_red_clump_for_calibration` | Classified as a red clump star for log(g) calibration |
| `flag_as_m_dwarf_for_calibration` | Classified as an M dwarf for Teff and log(g) calibration |
| `flag_censored_logg_for_metal_poor_m_dwarf` | log(g) censored for metal-poor ([M/H] < -0.6) M dwarfs |

### Abundance flags (`*_flags`)

Each element has its own flag bit field. The flags follow a consistent pattern:

| Bit | Flag | Description |
| --- | --- | --- |
| 0--4 | `flag_*_upper_limit_t1` through `t5` | Upper limit detection at thresholds from Hayes et al. (2022). Only set for elements with weak lines (Ce, C, Cu, Na, Nd, N, O, P, S, V). |
| 5 | `flag_*_censored_high_teff` | Abundance censored because it is known to be unreliable at this Teff |
| 6 | `flag_*_censored_low_teff_vmicro` | Abundance censored because the star has low Teff and v_micro (cool giant regime where abundances are unreliable) |
| 7 | `flag_*_censored_unphysical` | Abundance censored because FERRE returned an unphysical value |
| 8 | `flag_*_bad_grid_edge` | Abundance near the grid edge (bad) |
| 9 | `flag_*_warn_grid_edge` | Abundance near the grid edge (warning) |
| 10 | `flag_*_warn_teff` | Abundance known to be unreliable at this Teff |
| 11 | `flag_*_warn_m_h` | Abundance known to be unreliable at this [M/H] |

### IRFM temperature flags (`irfm_teff_flags`)

| Flag | Description |
| --- | --- |
| `flag_out_of_v_k_bounds` | V-Ks color is outside the calibration range |
| `flag_out_of_fe_h_bounds` | [Fe/H] is outside the calibration range |
| `flag_extrapolated_v_mag` | Synthetic V magnitude was extrapolated |
| `flag_poor_quality_k_mag` | Poor quality Ks magnitude from 2MASS |
| `flag_ebv_used_is_upper_limit` | E(B-V) reddening used is an upper limit |
| `flag_as_dwarf_for_irfm_teff` | Classified as a dwarf for IRFM calculation |
| `flag_as_giant_for_irfm_teff` | Classified as a giant for IRFM calculation |

## Caveats and things to know

### Abundance censoring in cool giants

Abundances for giants (log(g) <= 3.8) are censored (set to NaN) in regions of
parameter space where they are known to be unreliable:

- **All species except Mg**: censored when Teff <= 3250 K at any v_micro, or when
  Teff <= 4300 K and v_micro <= 1.25 km/s, or in a triangular region between
  3250--4300 K with low v_micro.

This primarily affects cool, low-gravity giants where molecular blending makes
individual abundance measurements unreliable.

### BA stars

Abundances are not measured for BA-type star grids (`combo5_BA`). These hot stars
have few metallic lines in the APOGEE wavelength range.

### Grid edge effects

When a parameter falls near the boundary of the synthetic spectral grid, the
result becomes less reliable because FERRE cannot interpolate freely in all
directions. Grid edge warnings and bad flags are set based on proximity to the
edge: a warning is triggered within one grid step, and a bad flag within 1/8 of a
step. Stars with `flag_teff_grid_edge_bad` or `flag_logg_grid_edge_bad` are
included in the `flag_bad` composite flag.

### Microturbulence relation

The initial guess for microturbulent velocity is derived from log(g) using a
polynomial relation:

```
log10(v_micro) = 0.372 - 0.091*logg - 0.001*logg^2 + 0.001*logg^3
```

This is used only as a starting point; v_micro is free to vary in the stellar
parameter stage.

### Continuum normalization

The continuum treatment differs between stages:

- **Coarse stage**: FERRE performs its own polynomial continuum normalization
  (default order 4).
- **Stellar parameter stage**: a median filter pre-correction is applied before
  FERRE runs, then FERRE performs polynomial continuum normalization.
- **Abundance stage**: the continuum from the stellar parameter stage is held
  fixed (no re-normalization).

### Error estimates

FERRE uncertainty estimates are formal errors from the chi-squared surface. A
post-processing noise model can be applied (via `apply_noise_model`) that rescales
and offsets the raw uncertainties based on empirical calibrations stored in
`$MWM_ASTRA/{version}/aux/ASPCAP_corrections.pkl`.

### Multiple equally good coarse results

If two or more coarse grid fits produce identical penalized reduced chi-squared
values, the `flag_multiple_equally_good_coarse_results` flag is set on the coarse
result. The first result encountered is used.

## Source code

The ASPCAP pipeline implementation is in:

- `src/astra/pipelines/aspcap/__init__.py` -- main `aspcap()` task and stage
  orchestration
- `src/astra/pipelines/aspcap/coarse.py` -- coarse parameter stage planning and
  grid penalization
- `src/astra/pipelines/aspcap/stellar_parameters.py` -- stellar parameter stage
  planning
- `src/astra/pipelines/aspcap/abundances.py` -- abundance stage planning
- `src/astra/pipelines/aspcap/continuum.py` -- median filter continuum correction
- `src/astra/pipelines/aspcap/corrections.py` -- post-processing calibrations and
  flagging
- `src/astra/pipelines/aspcap/initial.py` -- initial guess providers
- `src/astra/pipelines/aspcap/utils.py` -- grid matching, abundance controls, and
  helper functions
- `src/astra/models/aspcap.py` -- database model and output field definitions
