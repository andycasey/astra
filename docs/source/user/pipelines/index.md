# Pipelines

Astra includes a suite of analysis pipelines for processing SDSS-V Milky Way Mapper
spectra. Each pipeline targets specific science cases -- from stellar parameter
estimation and chemical abundance measurement to radial velocity determination and
spectral classification.

## Available pipelines

| Pipeline | Description |
| --- | --- |
| [APOGEENet](apogeenet) | Neural network stellar parameters for APOGEE spectra |
| [ASPCAP](aspcap) | ASPCAP pipeline using FERRE grid fitting (stellar parameters and chemical abundances) |
| [AstroNN](astronn) | AstroNN neural network for stellar parameters from APOGEE |
| [AstroNN Dist](astronn_dist) | AstroNN with distance estimation |
| [Best](best) | Selects best results from multiple pipelines per source |
| [BOSSNet](bossnet) | Neural network stellar parameters for BOSS spectra |
| [CLAM](clam) | Spectral fitting using grid interpolation |
| [Corv](corv) | Radial velocity and parameters for DA white dwarfs |
| [FERRE](ferre) | FERRE grid-based fitting backend (used by ASPCAP) |
| [Grok](grok) | Stellar parameters and vsini from high-resolution grid fitting |
| [Line Forest](line_forest) | Spectral line strength measurements |
| [MDwarfType](mdwarftype) | M dwarf spectral classification via template matching |
| [NMF Rectify](nmf_rectify) | NMF continuum rectification |
| [SLAM](slam) | M dwarf stellar parameters via machine learning |
| [Snow White](snow_white) | White dwarf classification and DA-type fitting |
| [The Cannon](the_cannon) | Label-transfer spectroscopy (The Cannon) |
| [The Payne](the_payne) | Neural network spectral fitting (The Payne) |

```{toctree}
:hidden:

apogeenet
aspcap
astronn
astronn_dist
best
bossnet
clam
corv
ferre
grok
line_forest
mdwarftype
nmf_rectify
slam
snow_white
the_cannon
the_payne
```
