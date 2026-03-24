# Glossary

Astra uses a centralized glossary system (defined in `astra.glossary`) to provide consistent help text for field names across all pipeline models. When a database field is created in any model, the glossary is consulted automatically to populate the field's `help_text` attribute. This means that a field called `teff` will always carry the description "Stellar effective temperature [K]", regardless of which pipeline produced it.

The glossary also understands naming conventions. Prefixes and suffixes like `e_`, `raw_`, and `_flags` are resolved automatically, so `e_teff` is understood as "Error on stellar effective temperature [K]" without needing a separate entry.

---

## Naming conventions

Astra field names follow systematic conventions that encode meaning through prefixes and suffixes.

| Prefix / Suffix | Meaning | Example |
|---|---|---|
| `e_` | Error (uncertainty) on a quantity | `e_teff` = error on effective temperature |
| `raw_` | Raw (uncalibrated) measurement | `raw_teff` = uncalibrated effective temperature |
| `initial_` | Initial guess value used to seed a fit | `initial_teff` = initial guess for effective temperature |
| `_flags` | Bitfield flags for a quantity | `result_flags` = flags describing the results |
| `_rchi2` | Reduced chi-square value for a fit | `nmf_rchi2` = reduced chi-square of NMF continuum fit |
| `rho_` | Correlation coefficient between two quantities | `rho_teff_logg` = correlation between TEFF and LOGG |

These conventions are handled by the `SPECIAL_CONTEXTS` mechanism in the glossary module, which means that any combination (e.g., `e_fe_h`, `raw_logg`, `initial_v_rad`) is resolved automatically without requiring an explicit glossary entry.

---

## Common pipeline output fields

These fields appear on all (or most) pipeline result tables via the `PipelineOutputMixin` base class.

### Identifiers and metadata

| Field | Description |
|---|---|
| `task_pk` | Task model primary key (auto-assigned unique identifier for each pipeline result) |
| `source_pk` | Foreign key linking to the unique source (astronomical object) |
| `spectrum_pk` | Foreign key linking to the unique spectrum that was analyzed |
| `v_astra` | Integer-encoded version of Astra that produced this result |
| `created` | Datetime when the task record was created |
| `modified` | Datetime when the task record was last modified |
| `tag` | Experiment tag for this result (useful for distinguishing test runs) |
| `t_elapsed` | Core-time elapsed on this analysis [s] |
| `t_overhead` | Estimated core-time spent in overheads [s] |

### Stellar parameters

These fields are produced by most stellar parameter pipelines (e.g., ASPCAP, The Cannon, The Payne, AstroNN).

| Field | Description |
|---|---|
| `teff` | Stellar effective temperature [K] |
| `e_teff` | Error on effective temperature [K] |
| `logg` | Surface gravity [log10(cm/s^2)] |
| `e_logg` | Error on surface gravity [log10(cm/s^2)] |
| `fe_h` | [Fe/H] iron abundance [dex] |
| `e_fe_h` | Error on [Fe/H] [dex] |
| `m_h` | Metallicity [dex] |
| `v_sini` | Projected rotational velocity [km/s] |
| `v_micro` | Microturbulence [km/s] |
| `v_macro` | Macroscopic broadening [km/s] |
| `alpha_fe` | [alpha/Fe] abundance ratio [dex] |
| `c_m_atm` | Atmospheric carbon abundance [dex] |
| `n_m_atm` | Atmospheric nitrogen abundance [dex] |
| `alpha_m_atm` | [alpha/M] abundance ratio [dex] |

### Radial velocities

| Field | Description |
|---|---|
| `v_rad` | Barycentric rest-frame radial velocity [km/s] |
| `e_v_rad` | Error on radial velocity [km/s] |
| `v_rel` | Relative velocity [km/s] |
| `bc` | Barycentric velocity correction applied [km/s] |
| `median_e_v_rad` | Median error in radial velocity [km/s] |
| `ccfwhm` | Cross-correlation function FWHM |
| `autofwhm` | Auto-correlation function FWHM |
| `n_components` | Number of components in the cross-correlation function |
| `v_helio` | Heliocentric velocity correction [km/s] |
| `v_shift` | Relative velocity shift used in stack [km/s] |

### Quality and fit indicators

| Field | Description |
|---|---|
| `snr` | Signal-to-noise ratio |
| `result_flags` | Bitfield flags describing the results |
| `flag_warn` | Composite warning flag (True if any warning condition is set) |
| `flag_bad` | Composite bad flag (True if any critical failure condition is set) |
| `chi2` | Chi-square value of the fit |
| `rchi2` | Reduced chi-square value of the fit |
| `initial_flags` | Flags indicating the source of the initial guess |
| `calibrated` | Whether any calibration has been applied to raw measurements |

### Elemental abundances

Many pipelines (especially ASPCAP) report individual elemental abundances. These all follow the `[X/H]` convention in dex.

| Field | Description |
|---|---|
| `al_h` | [Al/H] [dex] |
| `c_h` | [C/H] [dex] |
| `c_1_h` | [C/H] from neutral C lines [dex] |
| `c_12_13` | C12/C13 ratio |
| `ca_h` | [Ca/H] [dex] |
| `ce_h` | [Ce/H] [dex] |
| `co_h` | [Co/H] [dex] |
| `cr_h` | [Cr/H] [dex] |
| `cu_h` | [Cu/H] [dex] |
| `he_h` | [He/H] [dex] |
| `k_h` | [K/H] [dex] |
| `mg_h` | [Mg/H] [dex] |
| `mn_h` | [Mn/H] [dex] |
| `na_h` | [Na/H] [dex] |
| `nd_h` | [Nd/H] [dex] |
| `ni_h` | [Ni/H] [dex] |
| `n_h` | [N/H] [dex] |
| `o_h` | [O/H] [dex] |
| `p_h` | [P/H] [dex] |
| `si_h` | [Si/H] [dex] |
| `s_h` | [S/H] [dex] |
| `ti_h` | [Ti/H] [dex] |
| `ti_2_h` | [Ti/H] from singly ionized Ti lines [dex] |
| `v_h` | [V/H] [dex] |

All abundance fields support the `e_` prefix for uncertainties (e.g., `e_al_h` is the error on [Al/H]).

---

## Spectral fields

These fields describe the spectral data itself, available on spectrum models (APOGEE visits/stars, BOSS spectra, etc.).

### Pixel arrays

| Field | Description |
|---|---|
| `wavelength` | Wavelength (vacuum) [Angstrom] |
| `flux` | Flux [10^-17 erg/s/cm^2/Angstrom] |
| `ivar` | Inverse variance of flux values |
| `e_flux` | Error on flux (computed as ivar^-0.5) |
| `pixel_flags` | Pixel-level quality flags (see data reduction documentation) |
| `model_flux` | Best-fit model flux |
| `continuum` | Best-fit continuum flux |
| `nmf_rectified_model_flux` | Rectified NMF model flux |

### Wavelength solution

| Field | Description |
|---|---|
| `crval` | Reference vacuum wavelength [Angstrom] |
| `cdelt` | Vacuum wavelength step [Angstrom] |
| `crpix` | Reference pixel (1-indexed) |
| `npixels` | Number of pixels in the spectrum |
| `ctype` | Wavelength axis type |
| `cunit` | Wavelength axis unit |
| `dc_flag` | Linear (0) or logarithmic wavelength axis |
| `wresl` | Spectral resolution [Angstrom] |

### Observation metadata

| Field | Description |
|---|---|
| `mjd` | Modified Julian Date of observation |
| `fiber` | Fiber number |
| `field` | Field identifier |
| `plate` | Plate number of observation |
| `telescope` | Telescope used to observe the source |
| `exptime` | Total exposure time [s] |
| `n_exp` | Number of co-added exposures |
| `nvisits` | Number of visits included in the stack |
| `date_obs` | Observation date (UTC) |
| `jd` | Julian date at mid-point of visit |

### BOSS-specific fields

| Field | Description |
|---|---|
| `run2d` | BOSS data reduction pipeline version |
| `fieldid` | Field identifier |
| `plateid` | Plate identifier |
| `cartid` | Cartridge used for plugging |
| `plug_ra` | Right ascension of plug position [deg] |
| `plug_dec` | Declination of plug position [deg] |
| `zwarning` | BOSS redshift warning bitmask |
| `in_stack` | Whether this spectrum was used in the stack |

### APOGEE-specific fields

| Field | Description |
|---|---|
| `apred` | APOGEE data reduction pipeline version |
| `prefix` | Short prefix used for DR17 apVisit files |
| `obj` | Object name |
| `reduction` | An `obj`-like keyword used for apo1m spectra |
| `n_pairs` | Number of dither pairs combined |
| `dithered` | Fraction of visits that were dithered |
| `nvisits_apstar` | Number of visits included in the apStar file |
| `fluxflam` | ADU to flux conversion factor [ergs/s/cm^2/A] |
| `n_frames` | Number of frames combined |

### Observing conditions

| Field | Description |
|---|---|
| `airmass` | Mean airmass |
| `alt` | Telescope altitude [deg] |
| `az` | Telescope azimuth [deg] |
| `seeing` | Median seeing conditions [arcsecond] |
| `airtemp` | Air temperature [C] |
| `dewpoint` | Dew point temperature [C] |
| `humidity` | Humidity [%] |
| `pressure` | Air pressure [millibar] |
| `wind_speed` | Wind speed [km/s] |
| `wind_direction` | Wind direction [deg] |
| `gust_speed` | Wind gust speed [km/s] |
| `gust_direction` | Wind gust direction [deg] |
| `moon_phase_mean` | Mean phase of the moon |
| `moon_dist_mean` | Mean sky distance to the moon [deg] |

### FPS (Focal Plane System) fields

| Field | Description |
|---|---|
| `fps` | Whether a fibre positioner was used to acquire this data |
| `on_target` | FPS fiber on target |
| `assigned` | FPS target assigned |
| `valid` | Valid FPS target |
| `fiber_offset` | Position offset applied during observations |
| `delta_ra` | Offset in right ascension [arcsecond] |
| `delta_dec` | Offset in declination [arcsecond] |

---

## Source fields

These fields are on the `Source` model, which represents a unique astronomical object.

### Identifiers

| Field | Description |
|---|---|
| `sdss_id` | SDSS-5 unique source identifier |
| `gaia_dr3_source_id` | Gaia DR3 source identifier |
| `gaia_dr2_source_id` | Gaia DR2 source identifier |
| `tic_v8_id` | TESS Input Catalog (v8) identifier |
| `catalogid` | Catalog identifier used to target the source |
| `catalogid21` | Catalog identifier (v21; v0.0) |
| `catalogid25` | Catalog identifier (v25; v0.5) |
| `catalogid31` | Catalog identifier (v31; v1.0) |
| `healpix` | HEALPix location (128 sides) |
| `n_associated` | Number of SDSS_IDs associated with this CATALOGID |
| `n_neighborhood` | Sources within 3" and G_MAG < G_MAG_source + 5 |

### Astrometry

| Field | Description |
|---|---|
| `ra` | Right ascension (J2000) [deg] |
| `dec` | Declination (J2000) [deg] |
| `l` | Galactic longitude [deg] |
| `b` | Galactic latitude [deg] |
| `plx` | Parallax [mas] (Gaia DR3) |
| `e_plx` | Error on parallax [mas] (Gaia DR3) |
| `pmra` | Proper motion in RA [mas/yr] (Gaia DR3) |
| `e_pmra` | Error on proper motion in RA [mas/yr] (Gaia DR3) |
| `pmde` | Proper motion in DEC [mas/yr] (Gaia DR3) |
| `e_pmde` | Error on proper motion in DEC [mas/yr] (Gaia DR3) |
| `gaia_v_rad` | Radial velocity from Gaia DR3 [km/s] |
| `gaia_e_v_rad` | Error on Gaia radial velocity [km/s] |

### Photometry

| Field | Description |
|---|---|
| `g_mag` | Gaia DR3 mean G-band magnitude [mag] |
| `bp_mag` | Gaia DR3 mean BP-band magnitude [mag] |
| `rp_mag` | Gaia DR3 mean RP-band magnitude [mag] |
| `j_mag` | 2MASS J-band magnitude [mag] |
| `e_j_mag` | Error on 2MASS J-band magnitude [mag] |
| `h_mag` | 2MASS H-band magnitude [mag] |
| `e_h_mag` | Error on 2MASS H-band magnitude [mag] |
| `k_mag` | 2MASS K-band magnitude [mag] |
| `e_k_mag` | Error on 2MASS K-band magnitude [mag] |
| `ph_qual` | 2MASS photometric quality flag |
| `bl_flg` | Number of components fit per band (JHK) |
| `cc_flg` | Contamination and confusion flag |

### unWISE photometry

| Field | Description |
|---|---|
| `w1_flux` | unWISE W1-band flux [Vega nMgy] |
| `w1_dflux` | Statistical uncertainty in W1-band flux [Vega nMgy] |
| `w1_frac` | Fraction of W1 flux from this object |
| `w2_flux` | unWISE W2-band flux [Vega nMgy] |
| `w2_dflux` | Statistical uncertainty in W2-band flux [Vega nMgy] |
| `w2_frac` | Fraction of W2 flux from this object |

### Extinction

| Field | Description |
|---|---|
| `ebv` | E(B-V) reddening [mag] |
| `e_ebv` | Error on E(B-V) [mag] |
| `ebv_sfd` | E(B-V) from SFD [mag] |
| `ebv_zhang_2023` | E(B-V) from Zhang et al. (2023) [mag] |
| `ebv_bayestar_2019` | E(B-V) from Bayestar 2019 [mag] |
| `ebv_edenhofer_2023` | E(B-V) from Edenhofer et al. (2023) [mag] |
| `ebv_rjce_glimpse` | E(B-V) from RJCE GLIMPSE [mag] |
| `ebv_rjce_allwise` | E(B-V) from RJCE AllWISE [mag] |

### Distances (Bailer-Jones)

| Field | Description |
|---|---|
| `r_med_geo` | Median geometric distance [pc] |
| `r_lo_geo` | 16th percentile of geometric distance [pc] |
| `r_hi_geo` | 84th percentile of geometric distance [pc] |
| `r_med_photogeo` | 50th percentile of photogeometric distance [pc] |
| `r_lo_photogeo` | 16th percentile of photogeometric distance [pc] |
| `r_hi_photogeo` | 84th percentile of photogeometric distance [pc] |

### Targeting

| Field | Description |
|---|---|
| `lead` | Lead catalog used for cross-match |
| `version_id` | SDSS catalog version for targeting |
| `carton_0` | First carton for the source |
| `carton_flags` | Carton bit field |
| `sdss5_target_flags` | SDSS-5 targeting flags |

---

## Data reduction pipeline version fields

Various version fields track the software used in data processing.

| Field | Description |
|---|---|
| `v_astra` | Astra version (integer-encoded) |
| `v_apred` | APOGEE data reduction pipeline version |
| `v_boss` | Version of the BOSS ICC |
| `v_jaeger` | Version of Jaeger |
| `v_kaiju` | Version of Kaiju |
| `v_coord` | Version of coordIO |
| `v_calibs` | Version of FPS calibrations |
| `v_read` | Version of idlspec2d for processing raw data |
| `v_2d` | Version of idlspec2d for 2D reduction |
| `v_comb` | Version of idlspec2d for combining exposures |
| `release` | The SDSS release name |
