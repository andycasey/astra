# Data models

This page describes the data models used by Astra. These models define how astronomical sources, spectra, pipeline results, and summary products are stored in the database and written to FITS files.

## Sources

**Model:** `astra.models.source.Source`

The `Source` model represents a single astronomical object. Every spectrum and pipeline result in Astra is linked back to a `Source`. Each source has a unique primary key (`pk`) and carries identifiers, astrometry, photometry, targeting information, and reddening estimates.

### Identifiers

- `sdss_id` -- the unique SDSS-V identifier for this source.
- `sdss4_apogee_id` -- the APOGEE identifier from SDSS-IV (a string like `2M00000+000000`).
- `gaia_dr2_source_id`, `gaia_dr3_source_id` -- Gaia source identifiers.
- `tic_v8_id` -- TESS Input Catalog (v8) identifier.
- `healpix` -- HEALPix index for spatial indexing.
- `catalogid`, `catalogid21`, `catalogid25`, `catalogid31` -- SDSS-V catalog identifiers from different cross-match versions.

### Astrometry

- `ra`, `dec` -- right ascension and declination (degrees, J2000).
- `l`, `b` -- Galactic longitude and latitude (degrees).
- `plx`, `e_plx` -- parallax and its uncertainty (mas), from Gaia.
- `pmra`, `e_pmra`, `pmde`, `e_pmde` -- proper motions and uncertainties (mas/yr).
- `gaia_v_rad`, `gaia_e_v_rad` -- radial velocity from Gaia (km/s).

### Photometry

The `Source` model stores photometry from several surveys:

- **Gaia:** `g_mag`, `bp_mag`, `rp_mag`
- **2MASS:** `j_mag`, `h_mag`, `k_mag` (with uncertainties `e_j_mag`, etc.)
- **unWISE:** `w1_mag`, `w2_mag` (with uncertainties), plus raw fluxes `w1_flux`, `w2_flux`
- **GLIMPSE (Spitzer 4.5 micron):** `mag4_5`
- **Synthetic photometry from Gaia XP spectra:** Johnson-Kron-Cousins (`u_jkc_mag`, `b_jkc_mag`, etc.), SDSS (`u_sdss_mag`, `g_sdss_mag`, etc.), and Pan-STARRS (`y_ps1_mag`)

### Reddening

- `ebv`, `e_ebv` -- the adopted E(B-V) reddening and its uncertainty.
- `ebv_flags` -- bit field indicating the provenance of the adopted E(B-V), which may come from Zhang (2023), Edenhofer (2023), SFD, RJCE (GLIMPSE or AllWISE), or Bayestar (2019).
- Individual reddening estimates are also stored (e.g., `ebv_zhang_2023`, `ebv_sfd`, `ebv_bayestar_2019`).

### External stellar parameter estimates

- `zgr_teff`, `zgr_logg`, `zgr_fe_h` -- effective temperature, surface gravity, and metallicity from the Gaia XP analysis of Zhang, Green & Rix (2023).
- `r_med_geo`, `r_med_photogeo` -- geometric and photogeometric distance estimates from Bailer-Jones (EDR3, 2021).

### Observations summary

- `n_boss_visits`, `n_apogee_visits` -- number of BOSS and APOGEE visits.
- `boss_min_mjd`, `boss_max_mjd`, `apogee_min_mjd`, `apogee_max_mjd` -- MJD range of observations.

### Targeting flags

The `Source` model carries extensive targeting flags from both SDSS-IV (`sdss4_apogee_target1_flags`, `sdss4_apogee2_target1_flags`, etc.) and SDSS-V (`sdss5_target_flags`). These can be queried with helper methods such as `assigned_to_carton_label()` and `assigned_to_program()`.


## Spectra

### APOGEE spectra

**Module:** `astra.models.apogee`

APOGEE spectra are infrared spectra (H-band, ~1.5--1.7 micron) with 8575 pixels on a log-lambda wavelength grid. There are three APOGEE spectrum models in Astra, each corresponding to a different stage of processing.

#### ApogeeVisitSpectrum

An individual visit spectrum from the APOGEE data reduction pipeline, stored in an `apVisit` file. A "visit" is a single observation of a source (possibly combining multiple dithered exposures within the same night).

Key fields:

- `spectrum_pk` -- unique spectrum identifier used to link to pipeline results.
- `source` -- foreign key to the `Source` this spectrum belongs to.
- `release` -- data release (e.g., `"sdss5"` or `"dr17"`).
- `apred` -- APOGEE reduction pipeline version.
- `telescope` -- telescope used (e.g., `"apo25m"`, `"lco25m"`, `"apo1m"`).
- `plate`, `field`, `fiber`, `mjd` -- observation identifiers.
- `snr` -- signal-to-noise ratio.
- `v_rad` -- absolute radial velocity (km/s).
- `v_rel`, `e_v_rel` -- relative radial velocity and its uncertainty from Doppler fitting.
- `doppler_teff`, `doppler_logg`, `doppler_fe_h` -- initial stellar parameter estimates from the Doppler RV code.
- `spectrum_flags` -- bit field encoding quality warnings (bad pixels, bright neighbors, persistence, RV failures, etc.).
- `wavelength`, `flux`, `ivar`, `pixel_flags` -- spectral data arrays. Wavelengths are in vacuum, in the observed frame (not rest frame).

#### ApogeeVisitSpectrumInApStar

A visit spectrum as stored within an `apStar` file. Unlike the raw `apVisit`, these spectra have been resampled onto a common log-lambda wavelength grid and shifted to the source rest frame.

Key fields are similar to `ApogeeVisitSpectrum`, with the addition of `drp_spectrum_pk` which links back to the original `ApogeeVisitSpectrum`.

#### ApogeeCoaddedSpectrumInApStar

A co-added (stacked) APOGEE spectrum from an `apStar` file, created by combining all good visit spectra for a source. This represents the highest signal-to-noise APOGEE spectrum available for a given source.

Key fields:

- `star_pk` -- APOGEE DRP star primary key.
- `n_entries`, `n_visits`, `n_good_visits`, `n_good_rvs` -- number of visits and quality counts.
- `min_mjd`, `max_mjd` -- MJD range of the observations that went into the stack.
- `snr` -- signal-to-noise ratio of the co-added spectrum.
- `v_rad`, `e_v_rad`, `std_v_rad` -- mean radial velocity, its uncertainty, and the scatter across visits.
- `doppler_teff`, `doppler_logg`, `doppler_fe_h` -- stellar parameters from the Doppler RV code.
- `wavelength`, `flux`, `ivar`, `pixel_flags` -- spectral data on the common APOGEE log-lambda grid.

### BOSS spectra

**Module:** `astra.models.boss`

#### BossVisitSpectrum

An optical BOSS spectrum from a `specFull` file. A BOSS "visit" is defined as all exposures of a source taken on the same MJD. The wavelengths are in vacuum and shifted to the Solar system barycentric rest frame (not the source rest frame).

Key fields:

- `spectrum_pk` -- unique spectrum identifier.
- `source` -- foreign key to the `Source`.
- `release`, `run2d` -- data release and BOSS reduction pipeline version.
- `fieldid`, `mjd`, `catalogid` -- observation identifiers.
- `telescope` -- telescope used (e.g., `"apo25m"`, `"lco25m"`).
- `snr` -- signal-to-noise ratio.
- `n_exp`, `exptime` -- number of exposures and total exposure time.
- `seeing`, `airmass` -- observing conditions.
- `xcsao_v_rad`, `xcsao_e_v_rad` -- radial velocity from the XCSAO cross-correlation method.
- `xcsao_teff`, `xcsao_logg`, `xcsao_fe_h` -- initial stellar parameter estimates from XCSAO.
- `zwarning_flags` -- BOSS DRP warning flags (sky fiber, low coverage, unplugged, etc.).
- `wavelength`, `flux`, `ivar`, `pixel_flags` -- spectral data arrays.

There is no BOSS equivalent of `apStar` in the upstream BOSS pipeline -- the BOSS DRP does not stack spectra across multiple nights. Astra fills this gap with the `BossCombinedSpectrum` model (see below).

### MWM spectra

**Module:** `astra.models.mwm`

The MWM (Milky Way Mapper) spectrum models unify APOGEE and BOSS spectra into a common framework. They resample all spectra onto standardized wavelength grids in the source rest frame, making them ready for scientific analysis.

#### BossRestFrameVisitSpectrum

A BOSS visit spectrum that has been resampled onto a common log-lambda wavelength grid (4648 pixels, starting at log10(lambda) = 3.5523 with step 1e-4) and shifted to the source rest frame. Stored in `mwmVisit` files.

Key fields:

- `drp_spectrum_pk` -- link to the original `BossVisitSpectrum`.
- `sdss_id` -- SDSS-V unique identifier.
- `in_stack` -- boolean indicating whether this visit was used to create the combined spectrum.
- `snr` -- signal-to-noise ratio.
- `xcsao_v_rad`, `xcsao_teff`, `xcsao_logg`, `xcsao_fe_h` -- cross-correlation RV and stellar parameters.
- `continuum`, `nmf_rchi2`, `nmf_flags` -- NMF continuum model and quality indicators.
- `wavelength`, `flux`, `ivar`, `pixel_flags` -- spectral data in the source rest frame.

#### BossCombinedSpectrum

A co-added BOSS spectrum created by stacking all good rest-frame visit spectra for a source. This fills the gap left by the BOSS DRP, which does not produce stacked spectra. Stored in `mwmStar` files.

Key fields:

- `sdss_id` -- SDSS-V unique identifier.
- `telescope` -- telescope used.
- `n_visits`, `n_good_visits`, `n_good_rvs` -- visit counts.
- `min_mjd`, `max_mjd` -- MJD range of contributing visits.
- `v_rad`, `e_v_rad`, `std_v_rad` -- mean radial velocity and scatter.
- `snr` -- signal-to-noise ratio of the co-added spectrum.
- `continuum`, `nmf_rectified_model_flux`, `nmf_rchi2` -- NMF continuum model products.
- `wavelength`, `flux`, `ivar`, `pixel_flags` -- co-added spectral data.

#### ApogeeCombinedSpectrum

A co-added APOGEE spectrum in the MWM framework. Similar in structure to `BossCombinedSpectrum` but for APOGEE data, using the APOGEE log-lambda grid (8575 pixels, starting at log10(lambda) = 4.179 with step 6e-6). Stored in `mwmStar` files.

Key fields:

- `sdss_id`, `apred`, `obj`, `telescope` -- identifiers.
- `n_entries`, `n_visits`, `n_good_visits`, `n_good_rvs` -- visit counts.
- `v_rad`, `e_v_rad`, `std_v_rad` -- mean radial velocity and scatter.
- `doppler_teff`, `doppler_logg`, `doppler_fe_h` -- Doppler-derived stellar parameters.
- `snr`, `mean_fiber`, `std_fiber` -- summary statistics.
- `continuum`, `nmf_rectified_model_flux`, `nmf_rchi2` -- NMF continuum model products.
- `wavelength`, `flux`, `ivar`, `pixel_flags` -- co-added spectral data.

#### ApogeeRestFrameVisitSpectrum

An APOGEE visit spectrum resampled onto the common APOGEE log-lambda grid and shifted to the source rest frame. The APOGEE analogue of `BossRestFrameVisitSpectrum`. Stored in `mwmVisit` files.


## Pipeline results

**Module:** `astra.models.pipeline`

All analysis pipeline output models inherit from `PipelineOutputMixin`. This mixin provides a standardized set of metadata fields that every pipeline result carries:

- `task_pk` -- auto-incrementing primary key for the pipeline task.
- `source_pk` -- foreign key linking to the `Source`.
- `spectrum_pk` -- foreign key linking to the `Spectrum` that was analyzed.
- `v_astra` -- the Astra version (as an integer) that produced the result.
- `created`, `modified` -- timestamps.
- `t_elapsed`, `t_overhead` -- execution time and overhead (seconds).
- `tag` -- an optional text tag for organizing results.

A uniqueness constraint ensures that each spectrum is analyzed at most once per major.minor Astra version: `UNIQUE (spectrum_pk, v_astra_major_minor)`.

Individual pipeline models (e.g., ASPCAP, The Cannon, Snow White) extend `PipelineOutputMixin` with their own result fields (stellar parameters, abundances, flags, etc.). The `from_spectrum()` class method provides a convenience for creating a result record from a spectrum object. See the individual pipeline documentation pages for details on specific pipeline output models.


## Summary products

**Module:** `astra.products.pipeline_summary`

Summary products are FITS files that collect pipeline results across all sources into convenient catalog tables. There are two main types:

### astraAllStar files

Created by `create_all_star_product()`, these files contain results from a given pipeline run on co-added (star-level) spectra. The FITS structure is:

| HDU | Contents |
|-----|----------|
| 0 | Primary HDU with metadata header (pipeline name, Astra version, HDU descriptions) |
| 1 | BOSS results -- one row per source with a co-added BOSS spectrum analyzed by the pipeline, joined with `Source` and `BossCombinedSpectrum` fields |
| 2 | APOGEE results -- one row per source with a co-added APOGEE spectrum analyzed by the pipeline, joined with `Source` and `ApogeeCoaddedSpectrumInApStar` fields |

Each row contains the source-level information (identifiers, astrometry, photometry), the spectrum-level metadata (SNR, radial velocities), and all pipeline output fields (stellar parameters, abundances, flags). If a pipeline defines `flag_warn` and `flag_bad` properties, these are included as boolean columns.

### astraAllVisit files

Created by `create_all_visit_product()`, these files have the same HDU structure as `astraAllStar` but contain per-visit results instead of per-star results:

| HDU | Contents |
|-----|----------|
| 0 | Primary HDU with metadata header |
| 1 | BOSS visit-level results (using `BossVisitSpectrum`) |
| 2 | APOGEE visit-level results (using `ApogeeVisitSpectrumInApStar`) |

### astraBest files

Created by `create_astra_best_product()`, these files contain the "best" result per source across all pipelines. The HDU structure is simpler: a primary HDU and a single binary table extension with one row per source.

File naming follows the pattern `astraAllStar<Pipeline>-<version>.fits` (or `astraAllVisit<Pipeline>-<version>.fits`, or `astraFrankenstein-<version>.fits` for the best product), and files are optionally gzip-compressed.


## MWM products

**Module:** `astra.models.mwm`

MWM (Milky Way Mapper) data products combine all observations of a source into unified FITS files with a standardized structure. All spectra are resampled onto common wavelength grids and shifted to the source rest frame.

### mwmVisit

An `mwmVisit` file contains all visit spectra for a source, organized by telescope and instrument. The FITS structure has 5 HDUs:

| HDU | Contents |
|-----|----------|
| 0 | Primary HDU with source information (identifiers, photometry, astrometry) |
| 1 | All BOSS spectra from Apache Point Observatory |
| 2 | All BOSS spectra from Las Campanas Observatory |
| 3 | All APOGEE spectra from Apache Point Observatory |
| 4 | All APOGEE spectra from Las Campanas Observatory |

Key properties of `mwmVisit` spectra:

- All wavelengths are in **vacuum** and in the **source rest frame** (unlike `apVisit` which is in the observed frame, and `specFull` which is in the barycentric frame).
- All spectra are **resampled onto common wavelength grids** (BOSS: 4648 pixels; APOGEE: 8575 pixels).
- Spectra deemed unreliable are still included (unlike `apStar`), with an `in_stack` boolean column indicating whether each spectrum was used for the co-added product.

The file path follows the pattern: `$MWM_ASTRA/<v_astra>/spectra/visit/<sdss_id_groups>/mwmVisit-<v_astra>-<sdss_id>.fits`

The models that populate these HDUs are `BossRestFrameVisitSpectrum` (HDUs 1--2) and `ApogeeRestFrameVisitSpectrum` (HDUs 3--4).

### mwmStar

An `mwmStar` file contains the co-added spectra for a source, with one stacked spectrum per telescope/instrument combination. The HDU layout mirrors `mwmVisit`.

These represent the **best available spectrum** for a source and are the primary input to most Astra analysis pipelines.

The file path follows the pattern: `$MWM_ASTRA/<v_astra>/spectra/star/<sdss_id_groups>/mwmStar-<v_astra>-<sdss_id>.fits`

The models that populate these HDUs are `BossCombinedSpectrum` (HDUs 1--2) and `ApogeeCombinedSpectrum` (HDUs 3--4).

### MWMSpectrumProductStatus

This model tracks whether MWM spectrum products have been created for a given source. Its `flags` bit field records the processing status:

- `flag_skipped_because_no_sdss_id` -- source was skipped because it has no SDSS ID.
- `flag_skipped_because_not_stellar_like` -- source was skipped because it is not stellar-like.
- `flag_attempted_but_exception` -- an exception occurred during processing.
- `flag_created_mwm_visit` -- an `mwmVisit` file was successfully created.
- `flag_created_mwm_star` -- an `mwmStar` file was successfully created.
