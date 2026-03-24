# Best

The Best pipeline (internally `MWMBest`) selects the single best set of stellar parameters
and auxiliary measurements for each source observed by the Milky Way Mapper (MWM), drawing
from across all available analysis pipelines.

## What it does

Best produces one row per source that consolidates:

- Stellar parameters (Teff, log g, metallicity, abundances)
- Radial velocities from multiple methods
- Chemical abundances (up to 20+ elements)
- White dwarf classifications (from Snow White)
- M dwarf spectral types (from MDwarfType)
- Spectrum metadata (SNR, MJD range, number of visits)

## How it works

The Best pipeline queries results from multiple analysis pipelines in a priority-ordered
sequence. For each source, it takes the result from the first pipeline in the sequence
that has a valid measurement. Once a source has been assigned results, it is excluded
from subsequent queries.

### Pipeline priority order

1. **Snow White** (white dwarf classifications) -- for sources in the `mwm_wd` program
2. **BOSSNet** (hot stars) -- for sources in the `mwm_ob` program, with no result flags
3. **ASPCAP** (hot stars) -- for sources in the `mwm_ob` program
4. **APOGEENet** (YSOs) -- for sources in the `mwm_yso` program, with no result flags
5. **BOSSNet** (YSOs) -- for sources in the `mwm_yso` program, with no result flags
6. **SLAM** (M dwarfs) -- for sources with no result flags
7. **ASPCAP** (general) -- for sources not flagged as bad
8. **AstroNN** (general) -- for sources with no result flags
9. **APOGEENet** (general) -- for sources with no result flags
10. **BOSSNet** (general) -- for sources with no result flags
11. **APOGEENet** (any remaining) -- no quality cuts
12. **BOSSNet** (any remaining) -- no quality cuts

This priority order reflects the science strategy: specialized pipelines are preferred
for their target populations (e.g., Snow White for white dwarfs, SLAM for M dwarfs),
and higher-quality results are preferred over lower-quality ones.

### Spectrum metadata

Each Best result also carries metadata from the underlying spectrum:

- **APOGEE spectra**: release, apred, apstar, telescope, field, fiber information,
  Doppler RV parameters, cross-correlation RV, visit statistics
- **BOSS spectra**: release, run2d, telescope, XCSAO parameters, zwarning flags,
  visit statistics

## Output fields

### Radial velocity

| Field | Description |
| --- | --- |
| `v_rad` | Best radial velocity (km/s) |
| `e_v_rad` | Uncertainty on radial velocity |
| `std_v_rad` | Standard deviation of visit radial velocities |
| `median_e_v_rad` | Median per-visit RV uncertainty |

### Radial velocity (method-specific)

| Field | Description |
| --- | --- |
| `xcsao_teff`, `xcsao_logg`, `xcsao_fe_h` | XCSAO template parameters |
| `xcsao_meanrxc` | XCSAO cross-correlation R-value |
| `doppler_teff`, `doppler_logg`, `doppler_fe_h` | Doppler template parameters |
| `doppler_rchi2` | Doppler fit reduced chi-squared |
| `xcorr_v_rad`, `xcorr_v_rel`, `ccfwhm` | Cross-correlation RV results |
| `boss_net_v_rad`, `boss_net_e_v_rad` | BOSSNet radial velocity |

### Stellar parameters

| Field | Description |
| --- | --- |
| `teff`, `e_teff` | Effective temperature (K) |
| `logg`, `e_logg` | Surface gravity (log cm/s^2) |
| `v_micro`, `e_v_micro` | Microturbulent velocity (km/s) |
| `v_sini`, `e_v_sini` | Projected rotational velocity (km/s) |
| `m_h_atm`, `e_m_h_atm` | Overall metallicity [M/H] (dex) |
| `alpha_m_atm`, `e_alpha_m_atm` | Alpha-element abundance [alpha/M] (dex) |
| `c_m_atm`, `e_c_m_atm` | Atmospheric carbon abundance [C/M] (dex) |
| `n_m_atm`, `e_n_m_atm` | Atmospheric nitrogen abundance [N/M] (dex) |

### Chemical abundances

Individual element abundances are provided for: Al, C, C_1, Ca, Ce, Co, Cr, Cu, Fe,
K, Mg, Mn, N, Na, Nd, Ni, O, P, S, Si, Ti, Ti_2, V, and C_12_13 (carbon isotope ratio).

Each element `X` has four fields:
- `X_h`: Abundance [X/H] (dex)
- `e_X_h`: Uncertainty
- `X_h_flags`: Quality flags
- `X_h_rchi2`: Reduced chi-squared for the abundance window fit

### White dwarf classifications

| Field | Description |
| --- | --- |
| `classification` | White dwarf classification string |
| `p_da`, `p_db`, `p_dc`, ... | Probabilities for each white dwarf subtype |

### M dwarf classifications

| Field | Description |
| --- | --- |
| `spectral_type` | M dwarf spectral type |
| `sub_type` | Numerical sub-type |

### Observing metadata

| Field | Description |
| --- | --- |
| `snr` | Signal-to-noise ratio |
| `min_mjd`, `max_mjd` | MJD range of visits |
| `n_visits` | Number of visits |
| `n_good_visits` | Number of good visits |
| `n_good_rvs` | Number of good radial velocities |
| `telescope` | Telescope used |
| `release`, `filetype` | Data release and file type |

## Key caveats

- The Best result for a given source depends on which pipelines ran successfully and the
  priority order. The "best" designation reflects the operational strategy, not necessarily
  the most accurate measurement for a particular science case.
- A single source has at most one Best entry (unique on `source_pk` and `v_astra`). If a
  source was observed by both APOGEE and BOSS, the Best result comes from whichever
  pipeline ranks higher in the priority list.
- Fields that are not provided by the selected pipeline will be null. For example, chemical
  abundances are only populated when ASPCAP or AstroNN is the selected pipeline.
- The `pipeline_flags` field is an amalgamated bitfield that may combine flags from the
  selected source pipeline.
- Users interested in results from a specific pipeline should query that pipeline's table
  directly rather than relying on Best.
