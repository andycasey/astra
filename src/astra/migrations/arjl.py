
from astropy.table import Table
from glob import glob
from peewee import chunked
import numpy as np
from tqdm import tqdm
import h5py as h5


from astra.utils import dict_to_iterable

from astra.models import Source
from astra.models.arjl import (
    ARJLTHVisitSpectrum, ARJLDDVisitSpectrum,
    ARJLTHRestFrameVisitSpectrum, ARJLDDRestFrameVisitSpectrum
)
from astra.migrations.utils import enumerate_new_spectrum_pks




def shift(array, pixels, fill_value=0):
    pixels = np.asarray(pixels).astype(int)

    if pixels.ndim == 0:
        pixels = int(pixels)
        pad = fill_value * np.ones(abs(pixels))
        if pixels >= 0:
            return np.hstack([array[pixels:], pad])
        else:
            return np.hstack([pad, array[:pixels]])

    N, P = array.shape
    col_idx = np.arange(P)
    src_idx = col_idx[np.newaxis, :] + pixels[:, np.newaxis]  # (N, P)

    valid = (src_idx >= 0) & (src_idx < P)
    row_idx = np.arange(N)[:, np.newaxis] * np.ones((1, P), dtype=int)

    out = np.where(valid, array[row_idx, np.clip(src_idx, 0, P - 1)], fill_value)
    return out



def ingest_arjl_dr17_spectra(
    base_dir="/uufs/chpc.utah.edu/common/home/sdss50/dr19/vac/mwm/apMADGICS/v2024_03_16",
    batch_size=1000,
    limit=None
):
    from astra.models.base import database

    # Lookup all sdss_id vs source_pk first
    q = (
        Source
        .select(Source.pk, Source.sdss_id)
        .tuples()
    )
    source_pk_to_sdss_id = { sdss_id: pk for pk, sdss_id in q }

    # Ingest theory sepctra
    kinds = [
        ("outdir_wu_th/", (ARJLTHVisitSpectrum, ARJLTHRestFrameVisitSpectrum)),
        ("outdir_wu_dd/", (ARJLDDVisitSpectrum, ARJLDDRestFrameVisitSpectrum)),
    ]

    flatten = lambda x: x.flatten()[0]

    for subdir, models in kinds:

        with database.atomic():
            database.create_tables(models)

        visits = Table.read(glob(f"{base_dir}/{subdir}/allVisit_*.fits")[0])
        if limit is not None:
            visits = visits[:limit]

        n_unmatched, n_visits = (0, len(visits))

        rows_as_dict = dict(
            sdss_id=visits["SDSS_ID"].flatten(),
            release=["dr19"] * n_visits,
            v_arjl=["v2024_03_16"] * n_visits,
            source_pk=[ None ] * n_visits,
            spectrum_pk=[ None ] * n_visits,
            component_dir=[f"{base_dir}/{subdir}"] * n_visits,
            #row_index=np.arange(n_visits),
            row_index=visits["map2madgics"].flatten() - 1,  # 1-indexed in the file, convert to 0-indexed
            mjd=visits["MJD"],
            plate=visits["PLATE"],
            field=visits["FIELD"],
            obj=visits["APOGEE_ID"],
            fiber=visits["FIBERID"],
            telescope=visits["TELESCOPE"],
            adjfiberindx=visits["adjfiberindx"],
            v_rad=visits["RV_bary"],
            v_rad_flags=visits["RV_flag"],
            v_rad_minchi2_final=visits["RV_minchi2_final"],
            v_rad_pix_var=visits["RV_pix_var"],
            v_rad_pixoff_disc_final=visits["RV_pixoff_disc_final"],
            v_rad_pixoff_final=visits["RV_pixoff_final"],
            v_rel=visits["RV_vel"],
            v_rad_chi2_residuals=visits["RVchi2_residuals"],
            drp_snr=visits["DRP_SNR"],
            drp_starflag=visits["DRP_STARFLAG"],
            drp_vhelio=visits["DRP_VHELIO"],
            drp_vrel=visits["DRP_VREL"],
            drp_vrelerr=visits["DRP_VRELERR"],
            dr17_teff=visits["DR17_TEFF"],
            dr17_logg=visits["DR17_LOGG"],
            dr17_x_h=visits["DR17_X_H"],
            dr17_vsini=visits["DR17_VSINI"],
        )
        for i, (spectrum_pk, sdss_id) in enumerate(enumerate_new_spectrum_pks(tqdm(rows_as_dict["sdss_id"]))):
            source_pk = source_pk_to_sdss_id.get(sdss_id, None)
            rows_as_dict["spectrum_pk"][i] = spectrum_pk
            rows_as_dict["source_pk"][i] = source_pk
            if source_pk is None:
                n_unmatched += 1

        for key in ("plate", "field", "obj"):
            rows_as_dict[key] = list(map(str.strip, rows_as_dict[key]))

        print(f"Found {n_unmatched} unmatched sources in {subdir}")

        for model in models:
            n = 0
            with database.atomic():
                with tqdm(total=n_visits, desc=f"Ingesting {model.__name__}") as pb:
                    for chunk in chunked(dict_to_iterable(rows_as_dict), batch_size):
                        (
                            model
                            .insert_many(chunk)
                            .execute()
                        )
                        n += len(chunk)
                        pb.update(len(chunk))

            # For now, delete the sources with source_pk as None (unmatched) since they will cause issues downstream
            # and I don't want to write the mgiration code yet.
            n_deleted = (
                model
                .delete()
                .where(model.source_pk.is_null())
                .execute()
            )



        '''

        # --- Pre-load all h5 data ---
        # Load full dataset into numpy first, then index — much faster than h5 fancy indexing
        keys = ("x_starContinuum_v0", "x_starLines_v0", "x_residuals_v0", "fluxerr2")

        print("Pre-loading h5 components...")
        cache = {}
        for key in keys:
            print(f"  loading {key}...")
            with h5.File(f"{subdir}/apMADGICS_out_{key}.h5", "r") as fp:
                cache[key] = fp[key][:][indices]   # load all, index in numpy

        RV = np.array(allVisits["RV_pixoff_final"])[indices].squeeze(-1)  # (N,)

        # --- Vectorized flux/ivar over all visits ---
        print("Computing flux and ivar...")
        all_flux = shift(
            1
            + cache["x_starLines_v0"]
            + cache["x_residuals_v0"] / cache["x_starContinuum_v0"],
            RV
        )[:, 125:]  # (N, 8575)

        all_ivar_raw = shift(
            (1 / cache["fluxerr2"]) * cache["x_starContinuum_v0"] ** 2,
            RV
        )[:, 125:]
        all_ivar = np.where(np.isfinite(all_ivar_raw) & (all_ivar_raw > 0), 1.0 / all_ivar_raw, 0.0)

        bad = ~np.isfinite(all_flux)
        all_ivar[bad] = 0.0
        all_flux[bad] = 0.0

        del cache  # free memory

        # --- Combine visits per (SDSS_ID, TELESCOPE) group ---
        λ = 10 ** (4.179 + 6e-6 * np.arange(8575))

        groups = allVisits_matched_include.group_by(["SDSS_ID", "TELESCOPE"]).groups
        group_boundaries = groups.indices  # start index of each group

        n_groups = len(groups)
        n_pixels = 8575
        star_flux = np.zeros((n_groups, n_pixels))
        star_ivar = np.zeros((n_groups, n_pixels))
        star_meta = []

        print("Combining visits...")
        for i, group in enumerate(tqdm(groups)):
            s = group_boundaries[i]
            e = group_boundaries[i + 1]

            v_flux = all_flux[s:e]   # (n_visits, 8575)
            v_ivar = all_ivar[s:e]

            s_ivar = np.sum(v_ivar, axis=0)
            s_flux = np.sum(v_flux * v_ivar, axis=0)

            star_ivar[i] = s_ivar
            star_flux[i] = np.where(s_ivar > 0, s_flux / s_ivar, 0.0)

            sdss_id   = group["SDSS_ID"][0][0]
            telescope = group["TELESCOPE"][0]

            star_meta.append(dict(
                row_index=i,
                sdss_id=sdss_id,
                telescope=telescope,
                n_visits=e - s,
                drp_snr=np.sqrt(np.sum(allVisits_matched_include["DRP_SNR"][s:e] ** 2)),
                dr17_teff=allVisits_matched_include["DR17_TEFF"][s],
                dr17_logg=allVisits_matched_include["DR17_LOGG"][s],
                dr17_m_h=allVisits_matched_include["DR17_M_H"][s],
            ))

        print(f"Done. {n_groups} groups.")

        # Save the star-level combined spectra somewhere in a hdf5 file
        '''
