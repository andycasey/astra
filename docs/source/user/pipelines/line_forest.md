# Line Forest

Line Forest measures equivalent widths and absolute line strengths for a comprehensive
set of spectral lines in BOSS spectra. It uses trained neural network models to predict
line properties from windowed spectral regions.

## What it does

Line Forest provides measurements for individual spectral lines including:

- Equivalent width
- Absolute line strength
- Detection significance
- Uncertainty estimates (as percentiles from Monte Carlo resampling)

## How it works

1. **Spectral preparation**: The observed spectrum is converted to log10(flux) space.
   Bad pixels (non-finite, negative, or high-error) are cleaned: flux values are set to 1
   and uncertainties are capped at 5 times the median error.

2. **Line measurement**: For each spectral line in the target list:
   - A window centered on the line (converted from air to vacuum wavelength) is extracted
     and resampled onto a uniform grid of 128 steps using spline interpolation.
   - The windowed spectrum is passed through a pre-trained TensorFlow neural network model
     that predicts equivalent width, absolute strength, and a detection statistic.

3. **Monte Carlo uncertainties**: The measurement is repeated 100 times with Gaussian noise
   added to the spectrum (scaled by the flux uncertainty). The distribution of measurements
   across these realizations provides percentile-based uncertainties (16th, 50th, 84th
   percentiles).

4. **Detection filtering**: A line is only reported if:
   - The detection statistic exceeds 0.5 in absolute value (initial detection).
   - The detection rate across Monte Carlo realizations exceeds 30% (`detection_raw > 0.3`).

5. **Two model types**: Lines use one of two neural network models depending on window size:
   - `hlines.model`: Used for broader lines (200 A windows), including Balmer and Paschen
     series hydrogen lines, Ca H&K.
   - `zlines.model`: Used for narrower lines (50 A windows), including metal lines and
     helium lines.

## Lines measured

Line Forest measures lines from the following species and series:

### Hydrogen Balmer series
H-alpha (6562.8 A), H-beta (4861.3 A), H-gamma (4340.5 A), H-delta (4101.7 A),
H-epsilon (3970.1 A), H-8 through H-17 (3889--3697 A)

### Hydrogen Paschen series
Pa-7 (10049.5 A), Pa-8 (9546.1 A), Pa-9 (9229.1 A), Pa-10 through Pa-17 (9015--8467 A)

### Calcium
Ca II triplet (8498.0, 8542.1, 8662.1 A), Ca K (3933.7 A), Ca H (3968.5 A)

### Helium
He I (4471.5, 5015.7, 5875.6, 6678.2 A), He II (4685.7 A)

### Other species
N II (6548.1, 6583.5 A), S II (6716.4, 6730.8 A), Fe II (5018.4, 5169.0, 5197.6, 6432.7 A),
O I (5577.3, 6300.3, 6363.8 A), O II (3727.4 A), O III (4363.9, 4958.9, 5006.8 A),
Li I (6707.8 A)

## Output fields

For each line `X` (e.g., `h_alpha`, `ca_ii_8662`, `li_i`):

| Field | Description |
| --- | --- |
| `eqw_X` | Equivalent width (Angstroms; negative = emission, positive = absorption) |
| `abs_X` | Absolute line strength |
| `detection_stat_X` | Detection statistic from the neural network (values > 0.5 indicate detection) |
| `detection_raw_X` | Fraction of Monte Carlo realizations where the line was detected |
| `eqw_percentiles_X` | 16th, 50th, 84th percentile of equivalent width from Monte Carlo |
| `abs_percentiles_X` | 16th, 50th, 84th percentile of absolute strength from Monte Carlo |

Fields are null when the line is not detected or falls outside the spectral coverage.

## Key caveats

- Line Forest requires TensorFlow and pre-trained neural network models. The models were
  trained on BOSS spectra and may not generalize to spectra from other instruments.
- The detection threshold (|detection_stat| > 0.5 and detection_raw > 0.3) is a heuristic.
  Marginal detections should be treated with care.
- Wavelengths in the line list are given in air; they are converted to vacuum internally
  using the Ciddor (1996) formula.
- Some lines (particularly high-order Balmer and Paschen lines) may be blended. The neural
  network approach captures the blended profile, but the reported equivalent widths may not
  correspond to isolated single-line measurements.
- Lines near the edges of the BOSS wavelength coverage (especially the blue end below
  3700 A and the red end beyond 10000 A) may have degraded S/N and less reliable measurements.
