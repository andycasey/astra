# Corv

Corv measures radial velocities and atmospheric parameters for DA-type (hydrogen-atmosphere)
white dwarfs observed with BOSS. It was developed by Vedant Chandra and uses the Montreal
DA white dwarf model atmospheres.

## What it does

Corv fits DA white dwarf models to BOSS optical spectra to determine:

- Radial velocity (`v_rad`)
- Effective temperature (`teff`)
- Surface gravity (`logg`)

The pipeline targets objects in the `mwm_wd` program that have been classified as DA-type
white dwarfs by the [Snow White](snow_white) pipeline.

## How it works

1. **Pre-filtering**: Before fitting, each spectrum is checked against three criteria:
   - The source must belong to the `mwm_wd` program.
   - A Snow White classification must be available.
   - The Snow White classification must indicate a DA-type white dwarf.

   If any of these checks fail, a result is still created but the corresponding flag is set
   and no fit is performed.

2. **Model construction**: A DA white dwarf model is built using the Montreal model grid,
   which provides synthetic spectra parameterized by effective temperature and surface gravity.

3. **Radial velocity estimation**: An initial radial velocity is determined via
   cross-correlation on a grid from -1500 to +1500 km/s, followed by quadratic refinement
   around the peak.

4. **Full fit**: The pipeline uses `lmfit` to perform a least-squares fit of the model to
   continuum-normalized Balmer line profiles in the observed spectrum. The fit iterates
   over effective temperature to improve convergence.

5. **Parallelism**: Fitting is run in parallel using a process pool (up to 32 workers by
   default).

## Output fields

| Field | Description |
| --- | --- |
| `v_rad` | Radial velocity (km/s) |
| `e_v_rad` | Uncertainty on radial velocity (km/s) |
| `teff` | Effective temperature (K) |
| `e_teff` | Uncertainty on effective temperature (K) |
| `logg` | Surface gravity (log cm/s^2) |
| `e_logg` | Uncertainty on surface gravity |
| `initial_teff` | Initial guess for effective temperature |
| `initial_logg` | Initial guess for surface gravity |
| `initial_v_rad` | Initial guess for radial velocity |
| `rchi2` | Reduced chi-squared of the fit |
| `result_flags` | Bitfield summarizing result quality |

## Flags

| Flag | Bit | Meaning |
| --- | --- | --- |
| `flag_not_mwm_wd` | 2^5 | Object is not in the `mwm_wd` program |
| `flag_no_wd_classification` | 2^6 | No Snow White classification is available |
| `flag_not_da_type` | 2^7 | Object is not classified as DA-type by Snow White |

When any of these flags are set, no fit was performed and the stellar parameter and
radial velocity fields will be null.

## Key caveats

- Corv only fits DA-type white dwarfs. Other white dwarf subtypes (DB, DC, DQ, DZ, etc.)
  are not supported and will be flagged.
- Results depend on the Snow White classification being correct. Misclassified objects
  will either be skipped or produce unreliable fits.
- The Montreal DA model grid has finite coverage in temperature and gravity. Results near
  the grid edges should be treated with caution.
- The pipeline operates on BOSS visit spectra, not coadded spectra.
