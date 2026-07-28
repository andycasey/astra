import numpy as np
from peewee import JOIN, fn, chunked
from tqdm import tqdm

from astra.models import ApogeeCoaddedSpectrumInApStar
from astra.models.source import Source
from astra.models.aspcap import ASPCAP
from sklearn.metrics import confusion_matrix
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LinearRegression
from astropy.table import Table
from astra.utils import log, expand_path
from astra import __version__

import astropy.units as u
import astropy.constants as c

def clear_corrections(batch_size: int = 500):
    """
    Clear any existing corrections.
    """
    # TODO: Have some reference columns of what fields we should use / update with raw_ prefixes?
    N_updated = 0
    q = (
        ASPCAP
        .select()
        .where(ASPCAP.v_astra_major_minor >= 7)
    )

    for chunk in chunked(ASPCAP.select(), batch_size):
        for result in chunk:
            result.teff = result.raw_teff
            result.e_teff = result.raw_e_teff
            result.logg = result.raw_logg
            result.e_logg = result.raw_e_logg

        N_updated += (
            ASPCAP
            .bulk_update(
                chunk,
                fields=[
                    ASPCAP.teff,
                    ASPCAP.e_teff,
                    ASPCAP.logg,
                    ASPCAP.e_logg,
                ]
            )
        )

    return N_updated

import numpy as np
import matplotlib.pyplot as plt



def flag_boundaries_and_abundances():
    (
        ASPCAP
        .update(
            teff=np.nan,
            e_teff=np.nan,
            logg=np.nan,
            e_logg=np.nan,
            m_h_atm=np.nan,
            e_m_h_atm=np.nan,
            c_m_atm=np.nan,
            e_c_m_atm=np.nan,
            n_m_atm=np.nan,
            e_n_m_atm=np.nan,
            v_micro=np.nan,
            e_v_micro=np.nan,
            alpha_m_atm=np.nan,
            e_alpha_m_atm=np.nan,
            v_sini=np.nan,
            e_v_sini=np.nan,
            result_flags=ASPCAP.flag_unphysical_parameters.set()
        )
        .where(
            (ASPCAP.v_astra_major_minor >= 7)
        &   (
                (ASPCAP.raw_logg < -0.5)
            |   (ASPCAP.raw_teff < 0)
            |   (ASPCAP.raw_m_h_atm < -10)
            |   (ASPCAP.raw_c_m_atm < -10)
            |   (ASPCAP.raw_n_m_atm < -10)
            |   (ASPCAP.raw_alpha_m_atm < -10)
            )
        )
        .execute()
    )

    # Flag boundaries
    bounds = (
        (ASPCAP.raw_m_h_atm, ASPCAP.flag_m_h_atm_grid_edge_bad, ASPCAP.flag_m_h_atm_grid_edge_warn, -2.5, 1, 0.25),
        (ASPCAP.raw_alpha_m_atm, ASPCAP.flag_alpha_m_grid_edge_bad, ASPCAP.flag_alpha_m_grid_edge_warn, -0.75, 1, 0.25),
        (ASPCAP.raw_c_m_atm, ASPCAP.flag_c_m_atm_grid_edge_bad, ASPCAP.flag_c_m_atm_grid_edge_warn, -1.5, 1, 0.25),
        (ASPCAP.raw_n_m_atm, ASPCAP.flag_n_m_atm_grid_edge_bad, ASPCAP.flag_n_m_atm_grid_edge_warn, -0.5, 2, 0.5),
        (ASPCAP.raw_v_micro, ASPCAP.flag_v_micro_grid_edge_bad, ASPCAP.flag_v_micro_grid_edge_warn, 0.30, 4.8, 0.3),
        (ASPCAP.raw_v_sini, ASPCAP.flag_v_sini_grid_edge_bad, ASPCAP.flag_v_sini_grid_edge_warn, None, 96, 1),
    )
    for field, bad_flag, warn_flag, lower, upper, step in bounds:

        # Clear existing flags.
        (
            ASPCAP
            .update(result_flags=bad_flag.clear())
            .where(
                ASPCAP.v_astra_major_minor >= 7
            )
            .execute()
        )
        (
            ASPCAP
            .update(result_flags=warn_flag.clear())
            .where(ASPCAP.v_astra_major_minor >= 7)
            .execute()
        )

        if lower is None:
            warn_where = (field > (upper - step))
            bad_where = (field > (upper - 1/8 * step))
        elif upper is None:
            warn_where = (field < (lower + step))
            bad_where = (field < (lower + 1/8 * step))
        else:
            warn_where = (
                (field < (lower + step))
            |   (field > (upper - step))
            )
            bad_where = (
                (field < (lower + 1/8 * step))
            |   (field > (upper - 1/8 * step))
            )


        (
            ASPCAP
            .update(result_flags=warn_flag.set())
            .where(
                (ASPCAP.v_astra_major_minor >= 7)
            &   (field.is_null(False))
            &   warn_where
            )
            .execute()
        )
        (
            ASPCAP
            .update(result_flags=bad_flag.set())
            .where(
                (ASPCAP.v_astra_major_minor >= 7)
            &   (field.is_null(False))
            &   bad_where
            )
            .execute()
        )

    # Now let's do the abundances.
    abundance_bounds = [
        ("al_h", ASPCAP.raw_al_h, -2.5, 1, 0.25),
        ("c_12_13", ASPCAP.raw_c_12_13 - ASPCAP.raw_m_h_atm, -1.5, 1.0, 0.25),
        ("ca_h", ASPCAP.raw_ca_h - ASPCAP.raw_m_h_atm, -0.75, 1, 0.25),
        ("ce_h", ASPCAP.raw_ce_h, -2.5, 1, 0.25),
        ("c_h", ASPCAP.raw_c_h - ASPCAP.raw_m_h_atm, -1.5, 1, 0.25),
        ("co_h", ASPCAP.raw_co_h, -2.5, 1, 0.25),
        ("cr_h", ASPCAP.raw_cr_h, -2.5, 1, 0.25),
        ("cu_h", ASPCAP.raw_cu_h, -2.5, 1, 0.25),
        ("fe_h", ASPCAP.raw_fe_h, -2.5, 1, 0.25),
        ("k_h", ASPCAP.raw_k_h, -2.5, 1, 0.25),
        ("mg_h", ASPCAP.raw_mg_h - ASPCAP.raw_m_h_atm, -0.75, 1, 0.25),
        ("mn_h", ASPCAP.raw_mn_h, -2.5, 1, 0.25),
        ("na_h", ASPCAP.raw_na_h, -2.5, 1, 0.25),
        ("nd_h", ASPCAP.raw_nd_h, -2.5, 1, 0.25),
        ("ni_h", ASPCAP.raw_ni_h, -2.5, 1, 0.25),
        ("n_h", ASPCAP.raw_n_h - ASPCAP.raw_m_h_atm, -0.5, 2, 0.5),
        ("o_h", ASPCAP.raw_o_h - ASPCAP.raw_m_h_atm, -0.75, 1, 0.25),
        ("p_h", ASPCAP.raw_p_h - ASPCAP.raw_m_h_atm, -2.5, 1, 0.25),
        ("si_h", ASPCAP.raw_si_h - ASPCAP.raw_m_h_atm, -0.75, 1, 0.25),
        ("s_h", ASPCAP.raw_s_h - ASPCAP.raw_m_h_atm, -0.75, 1, 0.25),
        ("ti_h", ASPCAP.raw_ti_h - ASPCAP.raw_m_h_atm, -0.75, 1, 0.25),
        ("v_h", ASPCAP.raw_v_h, -2.5, 1, 0.25),
    ]
    for field_name, lhs, lower, upper, step in abundance_bounds:
        print("Doing", field_name)

        # Clear the flags first.
        (
            ASPCAP
            .update(**{f"{field_name}_flags": 0 })
            .where(ASPCAP.v_astra_major_minor >= 7)
            .execute()
        )

        # Set bad edge flags.
        flag_bad = getattr(ASPCAP, f"flag_{field_name}_bad_grid_edge")
        (
            ASPCAP
            .update(**{f"{field_name}_flags": flag_bad.set()})
            .where(
                (ASPCAP.v_astra_major_minor >= 7)
            &   (
                    (lhs < (lower + 1/8 * step))
                |   (lhs > (upper - 1/8 * step))
                )
            )
            .execute()
        )

        # Set warn edge flags.
        flag_warn = getattr(ASPCAP, f"flag_{field_name}_warn_grid_edge")
        (
            ASPCAP
            .update(**{f"{field_name}_flags": flag_warn.set()})
            .where(
                (ASPCAP.v_astra_major_minor >= 7)
            &   (
                    (lhs < (lower + step))
                |   (lhs > (upper - step))
                )
            )
            .execute()
        )

        # Set censored flags.
        flag_censored = getattr(ASPCAP, f"flag_{field_name}_censored_unphysical")
        (
            ASPCAP
            .update(**{
                f"{field_name}_flags": flag_censored.set(),
                field_name: np.nan,
                f"e_{field_name}": np.nan,
            })
            .where(
                (ASPCAP.v_astra_major_minor >= 7)
            &   (lhs < (lower - 10))
            )
            .execute()
        )

    # Let's NaN-ify giant abundances if they are in particularly bad parts of (teff, v_micro) space.
    # Note: not censoring Mg
    (
        ASPCAP
        .update(
            c_m_atm=np.nan,
            e_c_m_atm=np.nan,
            n_m_atm=np.nan,
            e_n_m_atm=np.nan,
            c_h=np.nan,
            e_c_h=np.nan,
            c_h_flags=ASPCAP.flag_c_h_censored_low_teff_vmicro.set(),
            n_h=np.nan,
            e_n_h=np.nan,
            n_h_flags=ASPCAP.flag_n_h_censored_low_teff_vmicro.set(),
            al_h=np.nan,
            e_al_h=np.nan,
            al_h_flags=ASPCAP.flag_al_h_censored_low_teff_vmicro.set(),
            ca_h=np.nan,
            e_ca_h=np.nan,
            ca_h_flags=ASPCAP.flag_ca_h_censored_low_teff_vmicro.set(),
            ce_h=np.nan,
            e_ce_h=np.nan,
            ce_h_flags=ASPCAP.flag_ce_h_censored_low_teff_vmicro.set(),
            co_h=np.nan,
            e_co_h=np.nan,
            co_h_flags=ASPCAP.flag_co_h_censored_low_teff_vmicro.set(),
            cr_h=np.nan,
            e_cr_h=np.nan,
            cr_h_flags=ASPCAP.flag_cr_h_censored_low_teff_vmicro.set(),
            cu_h=np.nan,
            e_cu_h=np.nan,
            cu_h_flags=ASPCAP.flag_cu_h_censored_low_teff_vmicro.set(),
            k_h=np.nan,
            e_k_h=np.nan,
            k_h_flags=ASPCAP.flag_k_h_censored_low_teff_vmicro.set(),
            mn_h=np.nan,
            e_mn_h=np.nan,
            mn_h_flags=ASPCAP.flag_mn_h_censored_low_teff_vmicro.set(),
            na_h=np.nan,
            e_na_h=np.nan,
            na_h_flags=ASPCAP.flag_na_h_censored_low_teff_vmicro.set(),
            nd_h=np.nan,
            e_nd_h=np.nan,
            nd_h_flags=ASPCAP.flag_nd_h_censored_low_teff_vmicro.set(),
            ni_h=np.nan,
            e_ni_h=np.nan,
            ni_h_flags=ASPCAP.flag_ni_h_censored_low_teff_vmicro.set(),
            o_h=np.nan,
            e_o_h=np.nan,
            o_h_flags=ASPCAP.flag_o_h_censored_low_teff_vmicro.set(),
            p_h=np.nan,
            e_p_h=np.nan,
            p_h_flags=ASPCAP.flag_p_h_censored_low_teff_vmicro.set(),
            si_h=np.nan,
            e_si_h=np.nan,
            si_h_flags=ASPCAP.flag_si_h_censored_low_teff_vmicro.set(),
            s_h=np.nan,
            e_s_h=np.nan,
            s_h_flags=ASPCAP.flag_s_h_censored_low_teff_vmicro.set(),
            ti_h=np.nan,
            e_ti_h=np.nan,
            ti_h_flags=ASPCAP.flag_ti_h_censored_low_teff_vmicro.set(),
            v_h=np.nan,
            e_v_h=np.nan,
            v_h_flags=ASPCAP.flag_v_h_censored_low_teff_vmicro.set(),
        )
        .where(
            (ASPCAP.v_astra_major_minor >= 7)
        &   (ASPCAP.raw_logg <= 3.8)
        &   (
                (ASPCAP.teff <= 3250)               # any v_micro
                |   (                               # lower square
                        (ASPCAP.teff <= 4300)
                    &   (ASPCAP.v_micro <= 1.25)
                )
                |   (                               # triangle
                        ASPCAP.teff.between(3250, 4300)
                    &   (ASPCAP.v_micro <= (4.32142857 - 7.14285714e-04 * ASPCAP.teff))
                )
            )
        )
        .execute()
    )

    # Let's nan-ify dwarf abundances
    # n_m_atm < 4250
    # n_h < 4250
    # al_h < 3240
    # ca_h < 3500
    # ce_h < 4250
    # co_h < 4250
    # cr_h < 4250
    # cu_h < 4250
    # k_h < 3500
    # mn_h < 4000
    # na_h < 4250
    # nd_h < 4250
    # ni -> no cut
    # na_h < 4250
    # nd_h < 4250
    # p_h < 4250
    # s_h < 4250
    # ti_h < 4250
    # v_h < 4250





def flag_upper_limits_by_hayes_2022():
    from astropy.table import Table
    coeff = Table.read(expand_path("$MWM_ASTRA/pipelines/aspcap/hayes_2022_upper_limit_coefficients.txt"), format="ascii")

    solar = {
        # Grevesse et al. 2007
        "c": 8.39,
        "n": 7.78,
        "p": 5.36,
        "ce": 1.7,
        "cu": 4.21,
        "na": 6.17,
        "nd": 1.45,
        "o": 8.66,
        "s": 7.14,
        "v": 4.0
    }

    elements = set(coeff["Element"])
    N_elements = len(elements)
    # elements = ("P", "C", "Ce", "Cu", "N", "Na", "Nd", "O", "S", "V")
    ## APJ: the upper limit flagging for C and O are far too aggressive
    elements = ("P", "Ce", "Cu", "N", "Na", "Nd", "S", "V")
    T = 5
    with tqdm(total=T * N_elements) as pb:
        for element in elements:
            print("Doing", element)

            solar_value = solar[element.lower()]
            mask = (coeff["Element"] == element)
            field = getattr(ASPCAP, f"raw_{element.lower()}_h")
            flags_field = getattr(ASPCAP, f"{element.lower()}_h_flags")

            for t in range(1, T + 1):
                row = coeff[mask][0]
                rhs = (float(row[f"A_{t}"]) * (ASPCAP.raw_teff / 10000) + float(row[f"B_{t}"]))
                where = (
                    (solar_value + field) < rhs
                )
                if sum(mask) > 1:
                    for row in coeff[mask][1:]:
                        rhs = (float(row[f"A_{t}"]) * (ASPCAP.raw_teff / 10000) + float(row[f"B_{t}"]))
                        where |= (
                            (solar_value + field) < rhs
                        )

                flag = getattr(ASPCAP, f"flag_{element.lower()}_h_upper_limit_t{t}")
                # Clear flags first.
                (
                    ASPCAP
                    .update(**{f"{element.lower()}_h_flags": flag.clear() })
                    .where((ASPCAP.v_astra_major_minor >= 7))
                    .execute()
                )
                (
                    ASPCAP
                    .update(**{f"{element.lower()}_h_flags": flag.set() })
                    .where(
                        (ASPCAP.v_astra_major_minor >= 7)
                    &   where
                    )
                    .execute()
                )
                pb.update()




def apply_flags():
    # TODO: Move all this to the construction of aspcap rows

    (
        ASPCAP
        .update(
            result_flags=ASPCAP.flag_high_v_sini.set()
        )
        .where(
            (ASPCAP.raw_v_sini.is_null(False))
        &   (ASPCAP.raw_v_sini != 'NaN')
        &   (ASPCAP.v_astra_major_minor >= 7)
        &   (
            (
                ((ASPCAP.raw_teff < 5250) & (ASPCAP.raw_v_sini > 3))
            |   (ASPCAP.raw_v_sini > 20)
            )
        )
        )
        .execute()
    )
    (
        ASPCAP
        .update(result_flags=ASPCAP.flag_high_v_micro.set())
        .where(
            (ASPCAP.raw_logg <= 3)
        &   (ASPCAP.raw_v_micro > 3)
        &   (ASPCAP.v_astra_major_minor >= 7)
        )
        .execute()
    )
    (
        ASPCAP
        .update(
            result_flags=ASPCAP.flag_ferre_fail.set(),
        )
        .where(
            (
                (ASPCAP.raw_teff <= 3000)
            |   (ASPCAP.raw_logg < -1)
            )
        &   (ASPCAP.v_astra_major_minor >= 7)
        )
        .execute()
    )
    (
        ASPCAP
        .update(
            result_flags=ASPCAP.flag_low_snr.set()
        )
        .where(
            (ASPCAP.ferre_log_snr_sq < 20)
        &   (ASPCAP.v_astra_major_minor >= 7)
        )
        .execute()
    )
    (
        ASPCAP
        .update(
            result_flags=ASPCAP.flag_high_rchi2.set()
        )
        .where(
            (ASPCAP.rchi2 > 1000)
        &   (ASPCAP.v_astra_major_minor >= 7)
        )
        .execute()
    )
    q = (
        ASPCAP
        .select(
            ASPCAP,
            ApogeeCoaddedSpectrumInApStar.std_v_rad
        )
        .join(ApogeeCoaddedSpectrumInApStar, on=(ASPCAP.spectrum_pk == ApogeeCoaddedSpectrumInApStar.spectrum_pk))
        .where(
            (ASPCAP.v_astra_major_minor >= 7)
        )
        .objects()
    )
    with tqdm() as pb:
        for chunk in chunked(q, 1000):
            updated = []
            for item in chunk:
                if item.std_v_rad is not None and item.std_v_rad > 1:
                    item.flag_high_std_v_rad = True
                    updated.append(item)

            if len(updated) > 0:
                (
                    ASPCAP
                    .bulk_update(
                        updated,
                        fields=[
                            ASPCAP.result_flags
                        ]
                    )
                )
                pb.update(1000)

    '''
    (
        ASPCAP
        .update(
            result_flags=ASPCAP.flag_high_std_v_rad.set()
        )
        .where(
            (ASPCAP.std_v_rad > 1)
        )
    )
    '''

    import numpy as np
    x_min, x_max, x_step = (2_500, 10_000, 50)
    y_min, y_max, y_step = (-0.5, 5.5, 0.05)

    q = list(
        ASPCAP
        .select()
        .where(
            (ASPCAP.v_astra_major_minor >= 7)
        &   (ASPCAP.raw_teff <= x_max)
        &   (ASPCAP.raw_teff >= x_min)
        &   (ASPCAP.raw_logg >= y_min)
        &   (ASPCAP.raw_logg <= y_max)
        &   ~(
                ASPCAP.flag_high_v_sini
            |   ASPCAP.flag_high_v_micro
            |   ASPCAP.flag_unphysical_parameters
            |   ASPCAP.flag_high_rchi2
            |   ASPCAP.flag_low_snr
            |   ASPCAP.flag_high_std_v_rad
            |   ASPCAP.flag_teff_grid_edge_bad
            |   ASPCAP.flag_logg_grid_edge_bad
            |   ASPCAP.flag_ferre_fail
            |   ASPCAP.flag_missing_model_flux
            |   ASPCAP.flag_potential_ferre_timeout
            |   ASPCAP.flag_no_suitable_initial_guess
            |   ASPCAP.flag_spectrum_io_error
        )
        )
    )
    data = np.array([(r.raw_teff, r.raw_logg) for r in q]).T

    teff, logg = data

    from scipy.stats import binned_statistic_2d


    x_bins = np.linspace(x_min, x_max, 1 + int((x_max - x_min)/x_step))
    y_bins = np.linspace(y_min, y_max, 1 + int((y_max - y_min)/y_step))

    H, x_edges, y_edges, bin_numbers = binned_statistic_2d(
        teff, logg, logg,
        statistic="count",
        bins=(x_bins, y_bins),
        expand_binnumbers=True
    )

    for_updating = []
    for i in tqdm(range(x_bins.size - 1)):
        for j in range(y_bins.size - 1):
            if H[i - 1, j - 1] <= 5:
                in_bin = np.all(bin_numbers.T == np.array([i, j]), axis=1)
                for index in np.where(in_bin)[0]:
                    foo = q[index]
                    foo.flag_suspicious_parameters = True
                    for_updating.append(foo)

    print(f"Setting {len(for_updating)}")
    for chunk in chunked(for_updating, 1000):
        ASPCAP.bulk_update(
            chunk,
            fields=[ASPCAP.result_flags]
        )



def apply_dr19_irfm_corrections():
    # Main-sequence stars
    teff_bounds = [
        (0,     4000, 31.4673),
        (4000,  7000, (1.4065e-4) * ASPCAP.raw_teff * ASPCAP.raw_teff - 1.4283 * ASPCAP.raw_teff + 3592.9544),
        (7000, 100000, 486.8381)
    ]
    m_h_bounds = [
        (-5, -0.75, -95.2568),
        (-0.75, 0.50, 173.3117 * ASPCAP.raw_m_h_atm + 34.7270),
        (0.50, 10, +121.3829)
    ]

    for teff_lower, teff_upper, teff_offset in teff_bounds:
        for m_h_lower, m_h_upper, m_h_offset in m_h_bounds:
            (
                ASPCAP
                .update(
                    teff=ASPCAP.raw_teff - teff_offset - m_h_offset,
                    irfm_teff_flags=ASPCAP.flag_as_dwarf_for_irfm_teff.set()
                )
                .where(
                    (ASPCAP.v_astra_major_minor >= 7)
                &   (ASPCAP.raw_logg >= 3.8)
                &   (ASPCAP.raw_teff >= teff_lower)
                &   (teff_upper >= ASPCAP.raw_teff)
                &   (ASPCAP.raw_m_h_atm >= m_h_lower)
                &   (m_h_upper >= ASPCAP.raw_m_h_atm)
                )
                .execute()
            )

    # Giant stars
    teff_bounds = [
        (0,     3000, 360.6954),
        (3000,  5750, (2.0370e-4) * ASPCAP.raw_teff * ASPCAP.raw_teff - 1.7968 * ASPCAP.raw_teff + 3917.8432),
        (5750, 100000, 321.0579)
    ]
    m_h_bounds = [
        (-5, -2.5, -192.4408),
        (-2.5, 0.50, 85.4827 * ASPCAP.raw_m_h_atm + 21.2660),
        (0.50, 10, +64.0073)
    ]
    for teff_lower, teff_upper, teff_offset in teff_bounds:
        for m_h_lower, m_h_upper, m_h_offset in m_h_bounds:
            (
                ASPCAP
                .update(
                    teff=ASPCAP.raw_teff - teff_offset - m_h_offset,
                    irfm_teff_flags=ASPCAP.flag_as_giant_for_irfm_teff.set()
                )
                .where(
                    (ASPCAP.v_astra_major_minor >= 7)
                &   (ASPCAP.raw_logg < 3.8)
                &   (ASPCAP.raw_teff >= teff_lower)
                &   (teff_upper >= ASPCAP.raw_teff)
                &   (ASPCAP.raw_m_h_atm >= m_h_lower)
                &   (m_h_upper >= ASPCAP.raw_m_h_atm)
                )
                .execute()
            )


def apply_dr19_logg_corrections(batch_size: int = 500, limit: int = None):
    """
    Apply the DR19 logg corrections to the ASPCAP data model.
    """

    # First let's construct a classifier for the RC/RGB stars.
    apokasc = Table.read(expand_path("$MWM_ASTRA/aux/external-catalogs/APOKASC_cat_v7.0.5.fits"))

    q = (
        ASPCAP
        .select(
            ASPCAP,
            Source.sdss4_apogee_id
        )
        .distinct(Source)
        .join(Source)
        .where(Source.sdss4_apogee_id.in_(list(apokasc["2MASS_ID"])))
        .where(ASPCAP.v_astra_major_minor >= 7)
        .objects()
    )

    fields = (
        ASPCAP.raw_teff,
        ASPCAP.raw_logg,
        ASPCAP.raw_m_h_atm,
        ASPCAP.raw_c_m_atm,
        ASPCAP.raw_n_m_atm,
        ASPCAP.raw_v_micro,
    )

    for field in fields:
        apokasc[field.name] = np.nan * np.ones(len(apokasc))

    for row in q:
        mask = (apokasc["2MASS_ID"] == row.sdss4_apogee_id)
        for field in fields:
            apokasc[field.name][mask] = getattr(row, field.name)


    clf = RandomForestClassifier(n_estimators=100, max_depth=10, random_state=0)

    X = np.array([
        apokasc["raw_teff"],
        apokasc["raw_logg"],
        apokasc["raw_m_h_atm"],
        apokasc["raw_c_m_atm"],
    ]).T
    y = np.array(apokasc["APOKASC3_EVSTATES"])

    finite = np.isfinite(X).all(axis=1)
    is_rc_or_rgb = (y == 1) | (y == 2)

    mask_clf = finite * is_rc_or_rgb

    clf.fit(X[mask_clf], y[mask_clf])

    cm = confusion_matrix(
        y[mask_clf],
        clf.predict(X[mask_clf]),
    )
    log.info(f"Confusion matrix:\n{cm}")

    norm_cm = confusion_matrix(
        y[mask_clf],
        clf.predict(X[mask_clf]),
        normalize='true'
    )
    log.info(f"Normalized confusion matrix:\n{norm_cm}")
    assert np.min(np.diag(norm_cm)) > 0.95, "You can do better than that..."

    """
    2024-08-26 14:22:09,344 [INFO]: Confusion matrix:
    [[9095  119]
    [ 159 5386]]
    2024-08-26 14:22:09,475 [INFO]: Normalized confusion matrix:
    [[0.98708487 0.01291513]
    [0.02867448 0.97132552]]
    """

    # For all the RC stars, construct a corrector.
    is_rc_for_fit = (
        (apokasc["APOKASC3_EVSTATES"] == 2)
    &   (apokasc["APOKASC3P_LOGG"] > 2.3)
    &   (apokasc["APOKASC3P_LOGG"] < 2.5) # remove secondary red clump
    &   (apokasc["APOKASC3P_MASS"] >= 0) # remove other secondary red clump things
    &   (np.abs(apokasc["APOKASC3P_LOGG"] - apokasc["raw_logg"] + 0.2) < 0.2) # coarse outlier removal
#    &   (apokasc["KALLINGER_EVSTATES"] != 2) # remove secondary red clump
    )

    X = np.array([
        apokasc["raw_logg"],
    ]).T

    y = apokasc["APOKASC3P_LOGG"]

    mask_lm_rc = np.isfinite(X).all(axis=1) * is_rc_for_fit

    rc_offset = np.median(apokasc["raw_logg"][mask_lm_rc] - y[mask_lm_rc])

    lm_rc = LinearRegression()
    lm_rc.fit(np.atleast_2d(apokasc["raw_m_h_atm"][mask_lm_rc]).T, apokasc["raw_logg"][mask_lm_rc] - y[mask_lm_rc])

    # For all the RGB stars, construct a corrector.
    is_rgb_for_fit = (
        (apokasc["APOKASC3_EVSTATES"] == 1)
    &   (apokasc["APOKASC3P_LOGG"] > -10)
    &   (apokasc["APOKASC3P_LOGG"] < 3.25)
    &   (apokasc["raw_logg"] < 3.25)
    &   (
            (apokasc["APOKASC3P_MASS"] > 0)
        |   (apokasc["APOKASC3P_LOGG"] < 0.85)
        )
    )

    X = np.array([
        apokasc["raw_m_h_atm"],
        apokasc["raw_logg"],
        #apokasc["raw_logg"]**2,
    ]).T
    y = apokasc["APOKASC3P_LOGG"]

    mask_lm_rgb = np.isfinite(X).all(axis=1) * is_rgb_for_fit

    lm_rgb = LinearRegression()
    lm_rgb.fit(X[mask_lm_rgb], y[mask_lm_rgb])

    def logg_correction_dwarf(r):
        return r.raw_logg - (-0.947 + 1.886e-4 * r.raw_teff + 0.410 * r.raw_m_h_atm) # Eq 2.

    print(f"RC offset")
    print(f"    logg = logg_raw - {rc_offset:.3f}")
    print(f"RC function:")
    print(f"    logg = logg_raw - ({lm_rc.coef_[0]:+.3f}[M/H]_raw,atm {lm_rc.intercept_:+.3f})")
    print(f"RGB function:")
    print(f"    logg = {lm_rgb.coef_[0]:+.3f}[M/H]_raw,atm {lm_rgb.coef_[1]:+.3f}logg_raw {lm_rgb.intercept_:+.3f}")

    """
    RC offset
        logg = logg_raw - 0.185
    RC function:
        logg = logg_raw - (+0.144[M/H]_raw,atm +0.198)
    RGB function:
        logg = -0.233[M/H]_raw,atm +1.094logg_raw -0.377
    """

    #def logg_correction_dwarf(r):
    #    return r.raw_logg - (-0.947 + 1.886e-4 * r.raw_teff + 0.410 * r.raw_m_h_atm) # Eq 2.

    # Clear all (logg) calibrations
    (
        ASPCAP
        .update(
            logg=ASPCAP.raw_logg,
            e_logg=ASPCAP.raw_e_logg,
            calibrated_flags=0
        )
        .where(
            (ASPCAP.v_astra_major_minor >= 7)
        )
        .execute()
    )

    # Do MS first
    (
        ASPCAP
        .update(
            logg=ASPCAP.raw_logg - (-0.947 + 1.886e-4 * ASPCAP.raw_teff + 0.410 * ASPCAP.raw_m_h_atm),
            calibrated_flags=ASPCAP.flag_as_dwarf_for_calibration.set()
        )
        .where(
            (ASPCAP.v_astra_major_minor >= 7)
        &   (ASPCAP.raw_logg >= 3.8)
        &   (ASPCAP.raw_teff >= 4250)
        &   (ASPCAP.raw_teff <= 7000)
        )
        .execute()
    )

    # Do M-dwarfs
    #raw_logg +( 1.6930001 + 5.2043072e-07 * pow(raw_teff - 3800,2) - 3.6010967e-04 * raw_teff - 1.7316068e+00 * raw_m_h_atm + 4.0915239e-04 * raw_m_h_atm * raw_teff)
    (
        ASPCAP
        .update(
            logg=ASPCAP.raw_logg + ( 1.6930001 + 5.2043072e-07 * fn.pow(ASPCAP.raw_teff - 3800,2) - 3.6010967e-04 * ASPCAP.raw_teff - 1.7316068e+00 * ASPCAP.raw_m_h_atm + 4.0915239e-04 * ASPCAP.raw_m_h_atm * ASPCAP.raw_teff),
            calibrated_flags=ASPCAP.flag_as_m_dwarf_for_calibration.set()
        )
        .where(
            (ASPCAP.v_astra_major_minor >= 7)
        &   (ASPCAP.raw_logg >= 3.8)
        &   (ASPCAP.raw_teff <= 4250)
        )
        .execute()
    )

    # NaN-ify logg for metal-poor M-dwarfs
    (
        ASPCAP
        .update(
            logg=np.nan,
            e_logg=np.nan,
            calibrated_flags=ASPCAP.flag_censored_logg_for_metal_poor_m_dwarf.set()
        )
        .where(
            (ASPCAP.v_astra_major_minor >= 7)
        &   (ASPCAP.flag_as_m_dwarf_for_calibration)
        &   (ASPCAP.raw_m_h_atm < -0.6)
        )
        .execute()
    )

    '''
    flag_as_m_dwarf_for_calibration
    (
        ASPCAP
        .update(
            calibrated_flags=ASPCAP.flag_as_m_dwarf_for_calibration.set(),
            logg=ASPCAP.raw_logg - (-0.3183278 + -5.9076945e-07 * fn.pow(raw_teff-3800,2) + 4.2788306e-4 * (raw_teff-3800) + 2.1420905e-01 * raw_m_h_atm)
        )
    )
    '''



    # Now do evolved stars
    q = (
        ASPCAP
        .select()
        .where(
            (ASPCAP.v_astra_major_minor >= 7)
        &   (ASPCAP.raw_logg < 3.8)
        &   (ASPCAP.raw_teff <= 7000)
        )
    )

    with tqdm(total=0, desc="Applying corrections") as pb:
        for chunk in chunked(q, batch_size):
            updated = []
            for r in chunk:
                r.calibrated_flags = 0 # remove any previous classifications
                r.logg = r.raw_logg

                # evolved
                X = np.atleast_2d([
                    r.raw_teff,
                    r.raw_logg,
                    r.raw_m_h_atm,
                    r.raw_c_m_atm,
                ]).astype(float)
                if np.isfinite(X).all():
                    predicted_class, = clf.predict(X)
                    if predicted_class == 1: # rgb
                        r.flag_as_giant_for_calibration = True
                        r.logg, = lm_rgb.predict(np.atleast_2d([
                            r.raw_m_h_atm,
                            r.raw_logg,
                        ]))
                    elif predicted_class == 2: # rc
                        r.flag_as_red_clump_for_calibration = True
                        #r.logg = r.raw_logg - rc_offset
                        r.logg = r.raw_logg - lm_rc.predict(np.atleast_2d([r.raw_m_h_atm]))[0]
                    else:
                        raise ValueError("arrrgh")
                else:
                    r.logg = r.raw_logg

                if np.isfinite(r.logg):
                    updated.append(r)

            if updated:
                fields = [ASPCAP.logg, ASPCAP.calibrated_flags]
                (
                    ASPCAP
                    .bulk_update(
                        updated,
                        fields=fields
                    )
                )
            pb.update(batch_size)

    models = [clf, lm_rc, lm_rgb]
    from astra import __version__
    path = expand_path(f"$MWM_ASTRA/{__version__}/aux/aspcap_logg_models.pkl")
    if not os.path.exists(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path))
    import pickle
    with open(path, "wb") as fp:
        pickle.dump(models, fp)

    return None


def apply_ipl3_irfm_corrections():

    (
        ASPCAP
        .update(
            teff=ASPCAP.raw_teff - (1.32607360e-04 * fn.pow(ASPCAP.raw_teff, 2) - 1.34550981e+00 * ASPCAP.raw_teff + 172.58758444 * ASPCAP.m_h_atm + 3362.5502957592807)
        )
        .where(
            (ASPCAP.raw_logg >= 3.8)
        &   (7000 >= ASPCAP.raw_teff) & (ASPCAP.raw_teff >= 4500)
        )
        .execute()
    )
    (
        ASPCAP
        .update(teff=ASPCAP.raw_teff)# #- 133.08899593327305 + 172.58758444 * ASPCAP.raw_m_h_atm + 49.75657881)
        .where(
            (ASPCAP.raw_logg >= 3.8)
        &   (ASPCAP.raw_teff > 7000)
        )
        .execute()
    )
    (
        ASPCAP
        .update(teff=ASPCAP.raw_teff -6.944800552354991 + 172.58758444 * ASPCAP.raw_m_h_atm + 49.75657881)
        .where(
            (ASPCAP.raw_logg >= 3.8)
        &   (ASPCAP.raw_teff < 4500)
        )
        .execute()
    )

    (
        ASPCAP
        .update(
            teff=ASPCAP.raw_teff - (1.50192884e-04 * fn.pow(ASPCAP.raw_teff,2) - 1.34175322e+00 * ASPCAP.raw_teff + 89.91479362 * ASPCAP.m_h_atm + (2962.5628921867774 + 20.904245812524106))
        )
        .where(
            (ASPCAP.raw_logg < 3.8)
        &   (5500 >= ASPCAP.raw_teff) & (ASPCAP.raw_teff >= 3000)
        )
        .execute()
    )
    (
        ASPCAP
        .update(teff=ASPCAP.raw_teff - 126.25491258531883)
        .where(
            (ASPCAP.raw_logg < 3.8)
        &   (ASPCAP.raw_teff > 5500)
        )
        .execute()
    )
    '''
    (
        ASPCAP
        .update(teff=ASPCAP.raw_teff - 106.28944170213481)
        .where(
            (ASPCAP.raw_logg < 3.8)
        &   (ASPCAP.raw_teff < 3500)
        )
        .execute()
    )
    '''

# V - J: 2.840 −1.3453 0.3906 −0.0546 0.002913


def get_m_dwarf_callable():

    bp_rp_mann = np.array([3.245, -2.4309, 1.043, -0.2127, 0.01649])
    #bp_rp_mann_met = np.array([2.835, -1.893, 0.7860, -0.1594, 0.01243, 0.04417])

    from peewee import fn
    from astra.models import Source, ASPCAP


    q = (
        ASPCAP
        .select(
            ASPCAP,
            Source
        )
        .join(Source)
        .where(
            ((Source.bp_mag - Source.rp_mag) > 0.983)
        &   (Source.plx.is_null(False))
        &   (Source.plx > 10)
        &   ((Source.g_mag + 5 * fn.log10(Source.plx/100)) > 5.553)
        &   (ASPCAP.v_sini != 'NaN') & (ASPCAP.v_sini >= 0)
        &   ~ASPCAP.flag_bad
        )
        .dicts()
    )
    #mask = (df.BP_MAG - df.RP_MAG>0.983)&(df.G_MAG+5*np.log10(df.PLX/100)>5.553)&(~df.BADFLAG)&(df.PLX>10)
    from astropy.table import Table


    results = Table(rows=list(q))
    bp_rp = results["bp_mag"] - results["rp_mag"]

    in_fit = (
        (results["raw_teff"] < 4100)
    &   ((results["bp_mag"] - results["rp_mag"]) > 1.5)
    &   ((results["bp_mag"] - results["rp_mag"]) < 3.5)
    ) * np.isfinite(bp_rp)
    from scipy.stats import binned_statistic

    d_teff = results["raw_teff"] - 3500 * np.polyval(bp_rp_mann[::-1], bp_rp)

    meds, edges, binmask = binned_statistic(
        bp_rp[in_fit],
        d_teff[in_fit],
        statistic="median",
        bins=20
    )
    delta_teff = meds[np.searchsorted(edges, bp_rp[in_fit]) - 1]

    def get_teff_logg(bp_rp, k_mag, plx):

        delta_teff = (
            meds[np.clip(np.searchsorted(edges, bp_rp) - 1, 0, edges.size - 2)]
        )

        mann_mass = np.array([-0.642, -0.208, -8.43e-4, 7.87e-3, 1.42e-4, -2.13e-4])
        mann_radius = np.array([1.9515, -0.3520, 0.01680])

        #M_Ks = (df_.K_MAG + 5*np.log10(df_.PLX/100))
        M_Ks = float(k_mag) + 5 * np.log10(float(plx)/100)

        z = np.array([bp_rp]).flatten()
        log10mass = 0
        for i in range(len(mann_mass)):
            log10mass += mann_mass[i]*(M_Ks-7.5)**i

        radius = 0
        for i in range(len(mann_radius)):
            radius += mann_radius[i]*M_Ks**i

        logg = np.log10(((c.G*10**log10mass*u.solMass/(radius*u.solRad)**2).to(u.cm/u.s**2)).value)

        return (delta_teff, logg, log10mass, radius)

    return get_teff_logg


def apply_m_dwarf_logg_correction():
    theta = np.array([-8.59880910e-08,  3.54159094e-05,  1.48755241e+00])

    (
        ASPCAP
        .update(
            logg=ASPCAP.raw_logg + 1.48755241e+00 + 3.54159094e-05 * ASPCAP.raw_teff - 8.59880910e-08 * fn.pow(ASPCAP.raw_teff, 2)
        )
        .where(
            (ASPCAP.raw_logg >= 3.8)
        &   (ASPCAP.raw_teff < 4100)
        )
        .execute()
    )


def apply_solar_neighbourhood_abundance_corrections():

    from astra import __version__

    where = (
            (ASPCAP.snr > 50)
        &   (~ASPCAP.flag_bad)
        &   (0.05 > ASPCAP.raw_m_h_atm)
        &   (ASPCAP.raw_m_h_atm > -0.05)
        &   (Source.plx > 2)
        &   (6000 >= ASPCAP.raw_teff)
        &   (ASPCAP.raw_teff >= 4000)
        &   (ASPCAP.v_astra_major_minor >= 7)
    )

    q_giants = Table(rows=list(
            ASPCAP
            .select()
            .join(Source, on=(ASPCAP.source_pk == Source.pk))
            .switch(ASPCAP)
            .join(ApogeeCoaddedSpectrumInApStar, on=(ASPCAP.spectrum_pk == ApogeeCoaddedSpectrumInApStar.spectrum_pk))
            .where(
                where
            &   (ASPCAP.raw_logg < 3.8)
            )
            .dicts()
        )
    )

    q_dwarfs = Table(rows=list(
            ASPCAP
            .select()
            .join(Source, on=(ASPCAP.source_pk == Source.pk))
            .switch(ASPCAP)
            .join(ApogeeCoaddedSpectrumInApStar, on=(ASPCAP.spectrum_pk == ApogeeCoaddedSpectrumInApStar.spectrum_pk))
            .where(
                where
            &   (ASPCAP.raw_logg >= 3.8)
            )
            .dicts()
        )
    )
    print(len(q_giants), len(q_dwarfs))
    field_names = (
        #"raw_m_h_atm",
        "raw_alpha_m_atm",
        "raw_al_h",
        "raw_ca_h",
        "raw_ce_h",
        #"raw_c_1_h",
        #"raw_c_h",
        "raw_co_h",
        "raw_cr_h",
        "raw_cu_h",
        "raw_fe_h",
        "raw_k_h",
        "raw_mg_h",
        "raw_mn_h",
        "raw_na_h",
        "raw_nd_h",
        "raw_ni_h",
        #"raw_n_h",
        "raw_o_h",
        "raw_p_h",
        "raw_si_h",
        "raw_s_h",
        "raw_ti_h",
        "raw_ti_2_h",
        "raw_v_h",
    )
    giant_kwds = dict()
    dwarf_kwds = dict()
    giant_vals = {}
    dwarf_vals = {}
    for field_name in field_names:
        giants = np.array(q_giants[field_name]).astype(float)
        dwarfs = np.array(q_dwarfs[field_name]).astype(float)
        giants = giants[giants > -500]
        dwarfs = dwarfs[dwarfs > -500]
        print(f"{field_name} {np.nanmean(giants):.2f} {np.nanmean(dwarfs):.2f}")

        giant_vals[field_name] = np.nanmean(giants)
        dwarf_vals[field_name] = np.nanmean(dwarfs)

        giant_kwds[field_name[4:]] = getattr(ASPCAP, field_name) - giant_vals[field_name]
        dwarf_kwds[field_name[4:]] = getattr(ASPCAP, field_name) - dwarf_vals[field_name]

    import pickle
    from astra.utils import expand_path
    with open(expand_path(f"$MWM_ASTRA/{__version__}/aux/aspcap_snc_corrections.pkl"), "wb") as fp:
        pickle.dump(dict(giants=giant_vals, dwarfs=dwarf_vals), fp)

    (
        ASPCAP
        .update(**giant_kwds)
        .where(
            (ASPCAP.raw_logg < 3.8)
        &   (ASPCAP.v_astra_major_minor >= 7)
        )
        .execute()
    )
    (
        ASPCAP
        .update(**dwarf_kwds)
        .where(
            (ASPCAP.raw_logg >= 3.8)
        &   (ASPCAP.v_astra_major_minor >= 7)
        )
        .execute()
    )

def apply_dr19_solar_neighbourhood_abundance_corrections():
    import pickle
    from astra.utils import expand_path
    with open(expand_path(f"$MWM_ASTRA/0.6.0/aux/aspcap_snc_corrections.pkl"), "rb") as fp:
        d = pickle.load(fp)

    giant_kwds = {}
    for field_name, offset in d["giants"].items():
        if hasattr(ASPCAP, field_name):
            giant_kwds[field_name[4:]] = getattr(ASPCAP, field_name) - offset

    dwarf_kwds = {}
    for field_name, offset in d["dwarfs"].items():
        if hasattr(ASPCAP, field_name):
            dwarf_kwds[field_name[4:]] = getattr(ASPCAP, field_name) - offset

    (
        ASPCAP
        .update(**giant_kwds)
        .where(
            (ASPCAP.raw_logg < 3.8)
        &   (ASPCAP.v_astra_major_minor >= 7)
        )
        .execute()
    )
    (
        ASPCAP
        .update(**dwarf_kwds)
        .where(
            (ASPCAP.raw_logg >= 3.8)
        &   (ASPCAP.v_astra_major_minor >= 7)
        )
        .execute()
    )
    return None


def apply_ipl3_logg_corrections(batch_size: int = 500, limit: int = None):
    """
    Apply the IPL-3 logg corrections to the ASPCAP data model.
    """

    # First let's construct a classifier for the RC/RGB stars.
    apokasc = Table.read(expand_path("$MWM_ASTRA/aux/external-catalogs/APOKASC_cat_v7.0.5.fits"))

    q = (
        ASPCAP
        .select(
            ASPCAP,
            Source.sdss4_apogee_id
        )
        .distinct(Source)
        .join(Source)
        .where(Source.sdss4_apogee_id.in_(list(apokasc["2MASS_ID"])))
        .objects()
    )

    fields = (
        ASPCAP.raw_teff,
        ASPCAP.raw_logg,
        ASPCAP.raw_m_h_atm,
        ASPCAP.raw_c_m_atm,
        ASPCAP.raw_n_m_atm,
        ASPCAP.raw_v_micro,
    )

    for field in fields:
        apokasc[field.name] = np.nan * np.ones(len(apokasc))

    for row in q:
        mask = (apokasc["2MASS_ID"] == row.sdss4_apogee_id)
        for field in fields:
            apokasc[field.name][mask] = getattr(row, field.name)


    clf = RandomForestClassifier(n_estimators=100, max_depth=10, random_state=0)

    X = np.array([
        apokasc["raw_teff"],
        apokasc["raw_logg"],
        apokasc["raw_m_h_atm"],
        apokasc["raw_c_m_atm"],
    ]).T
    y = np.array(apokasc["APOKASC3_EVSTATES"])

    finite = np.isfinite(X).all(axis=1)
    is_rc_or_rgb = (y == 1) | (y == 2)

    mask_clf = finite * is_rc_or_rgb

    clf.fit(X[mask_clf], y[mask_clf])

    cm = confusion_matrix(
        y[mask_clf],
        clf.predict(X[mask_clf]),
    )
    log.info(f"Confusion matrix:\n{cm}")

    norm_cm = confusion_matrix(
        y[mask_clf],
        clf.predict(X[mask_clf]),
        normalize='true'
    )
    log.info(f"Normalized confusion matrix:\n{norm_cm}")
    assert np.min(np.diag(cm)) > 0.95, "You can do better than that..."

    # For all the RC stars, construct a corrector.
    is_rc_for_fit = (
        (apokasc["APOKASC3_EVSTATES"] == 2)
    &   (apokasc["APOKASC3P_LOGG"] > 2.3)
    &   (apokasc["APOKASC3P_LOGG"] < 2.5) # remove secondary red clump
    &   (apokasc["APOKASC3P_MASS"] >= 0) # remove other secondary red clump things
    &   (np.abs(apokasc["APOKASC3P_LOGG"] - apokasc["raw_logg"] + 0.2) < 0.2) # coarse outlier removal
#    &   (apokasc["KALLINGER_EVSTATES"] != 2) # remove secondary red clump
    )

    X = np.array([
        apokasc["raw_logg"],
    ]).T

    y = apokasc["APOKASC3P_LOGG"]

    mask_lm_rc = np.isfinite(X).all(axis=1) * is_rc_for_fit

    rc_offset = np.median(apokasc["raw_logg"][mask_lm_rc] - y[mask_lm_rc])


    # For all the RGB stars, construct a corrector.
    is_rgb_for_fit = (
        (apokasc["APOKASC3_EVSTATES"] == 1)
    &   (apokasc["APOKASC3P_LOGG"] > -10)
    &   (apokasc["APOKASC3P_LOGG"] < 3.25)
    &   (apokasc["raw_logg"] < 3.25)
    &   (
            (apokasc["APOKASC3P_MASS"] > 0)
        |   (apokasc["APOKASC3P_LOGG"] < 0.85)
        )
    )

    X = np.array([
        apokasc["raw_m_h_atm"],
        apokasc["raw_logg"],
        #apokasc["raw_logg"]**2,
    ]).T
    y = apokasc["APOKASC3P_LOGG"]

    mask_lm_rgb = np.isfinite(X).all(axis=1) * is_rgb_for_fit

    lm_rgb = LinearRegression()
    lm_rgb.fit(X[mask_lm_rgb], y[mask_lm_rgb])

    def logg_correction_dwarf(r):
        return r.raw_logg - (-0.947 + 1.886e-4 * r.raw_teff + 0.410 * r.raw_m_h_atm) # Eq 2.

    print("getting mdwarf callable")
    teff_logg_m_dwarfs = get_m_dwarf_callable()
    print("ok got it")

    # We're ready to apply corrections for RGB, RC, and MS stars.
    q = (
        ASPCAP
        .select(
            ASPCAP,
            Source.bp_mag,
            Source.rp_mag,
            Source.k_mag,
            Source.plx
        )
        .join(Source)
        .where(
            ASPCAP.raw_teff.is_null(False)
        &   ASPCAP.raw_logg.is_null(False)
        &   ASPCAP.raw_m_h_atm.is_null(False)
        )
        .objects()
        .limit(limit)
    )

    # this is the isochrone logg from Szabolcs
    #def isochrone_main_sequence_logg_delta(raw_teff):
    #    a, b, c = (-1.495465498565102e-07, 0.0016514861990204446, -4.613929004721487)
    #    return a * raw_teff**2 + b*raw_teff + c
    #results["bp_mag"] - results["rp_mag"])

    with tqdm(total=0, desc="Applying corrections") as pb:
        for chunk in chunked(q, batch_size):
            any_mass_radius = False
            updated = []
            for r in chunk:
                r.calibrated_flags = 0 # remove any previous classifications
                r.logg = r.raw_logg

                #if r.raw_logg >= 3.8 and r.raw_teff < 4800 and r.bp_mag is not None and r.rp_mag is not None and 5.5 > (r.bp_mag - r.rp_mag) and (r.bp_mag - r.rp_mag) > 1.5:
                #    # dwarf
                #    r.logg = r.raw_logg - isochrone_main_sequence_logg_delta(r.raw_teff)
                #    r.flag_as_dwarf_for_calibration = True
                if r.raw_logg >= 3.8:

                    if r.raw_teff > 4100:
                        r.logg = logg_correction_dwarf(r)
                        r.flag_as_dwarf_for_calibration = True

                    else:
                        # M-dwarf
                        try:
                            bp_rp = r.bp_mag - r.rp_mag
                        except:
                            r.logg = logg_correction_dwarf(r)
                            r.flag_as_dwarf_for_calibration = True
                        else:
                            if (3.5 > bp_rp > 1.5) and np.all(np.isfinite(np.array([r.k_mag, r.plx]).astype(float))) and r.plx > 0:
                                r.flag_as_m_dwarf_for_calibration = True
                            else:
                                r.logg = logg_correction_dwarf(r)
                                r.flag_as_dwarf_for_calibration = True

                elif r.raw_logg < 3.8:
                    # evolved
                    X = np.atleast_2d([
                        r.raw_teff,
                        r.raw_logg,
                        r.raw_m_h_atm,
                        r.raw_c_m_atm,
                    ]).astype(float)
                    if np.isfinite(X).all():
                        predicted_class, = clf.predict(X)
                        if predicted_class == 1: # rgb
                            r.flag_as_giant_for_calibration = True
                            r.logg, = lm_rgb.predict(np.atleast_2d([
                                r.raw_m_h_atm,
                                r.raw_logg,
                            ]))
                        elif predicted_class == 2: # rc
                            r.flag_as_red_clump_for_calibration = True
                            r.logg = r.raw_logg - rc_offset
                        else:
                            raise ValueError("arrrgh")
                    else:
                        r.logg = r.raw_logg
                else:
                    r.logg = r.raw_logg


                if np.isfinite(r.logg):
                    updated.append(r)

            if updated:
                fields = [ASPCAP.logg, ASPCAP.calibrated_flags]
                if any_mass_radius:
                    fields.extend([
                        ASPCAP.teff,
                    ])
                #        ASPCAP.mass,
                #        ASPCAP.radius

                (
                    ASPCAP
                    .bulk_update(
                        updated,
                        fields=fields
                    )
                )
            pb.update(batch_size)

    return None

# ADD NEW FUNCTIONS #

def get_ipl4v2_teff_corrections(logg, mh, teff, e_teff=None):
    """
    Apply IPL-4v2 Teff corrections.
    Guilherme Limberg calibrated to GHB09 J-K Teff (2nd-degree poly in [M/H]).

    Returns (teff_corr, e_teff_corr, is_giant, is_dwarf).
    e_teff_corr is None when e_teff is not supplied.
    Unclassified stars (missing teff/logg) get NaN in teff_corr.
    """
    logg  = np.asarray(logg,  dtype=float)
    mh    = np.asarray(mh,    dtype=float)
    teff  = np.asarray(teff,  dtype=float)
    if e_teff is not None:
        e_teff = np.asarray(e_teff, dtype=float)

    # Luminosity classification using Kiel diagram boundaries
    mask_missing = ~np.isfinite(teff) | ~np.isfinite(logg)
    boundary1 = (-4580.0 + 0.4 * teff) / (-700.0)
    boundary2 = 0.0023 * teff - 5.4

    is_giant = (
        ~mask_missing & (
              ((teff < 4000)  & (logg < boundary2))
            | ((teff >= 4000) & (teff < 4800)  & (logg < 3.8))
            | ((teff >= 4800) & (teff <= 6200) & (logg < boundary1))
            | ((teff > 6200)  & (logg < 3.0))
        )
    )
    is_dwarf = ~mask_missing & ~is_giant

    teff_corr   = np.full_like(teff, np.nan)
    e_teff_corr = np.full_like(teff, np.nan) if e_teff is not None else None

    # Giants: clip [M/H] to [-2.2, 0.4], coefficients A=44.7, B=10.2, C=65.2, sys_err=84.3 K
    feh_g = np.clip(mh, -2.2, 0.4)
    giant_offset = 44.7 * feh_g**2 + 10.2 * feh_g + 65.2
    teff_corr[is_giant] = teff[is_giant] + giant_offset[is_giant]
    if e_teff is not None:
        e_teff_corr[is_giant] = np.sqrt(e_teff[is_giant]**2 + 84.3**2)

    # Dwarfs: clip [M/H] to [-0.9, 0.4], coefficients A=243.1, B=-69.0, C=-105.1, sys_err=161.4 K
    feh_d = np.clip(mh, -0.9, 0.4)
    dwarf_offset = 243.1 * feh_d**2 - 69.0 * feh_d - 105.1
    teff_corr[is_dwarf] = teff[is_dwarf] + dwarf_offset[is_dwarf]
    if e_teff is not None:
        e_teff_corr[is_dwarf] = np.sqrt(e_teff[is_dwarf]**2 + 161.4**2)

    return teff_corr, e_teff_corr, is_giant, is_dwarf

def get_ipl4v2_logg_corrections(logg_spec, mh, teff, maxiter=100, tol=1e-4):
    """
    IPL-4v2 logg corrections — Scenario A.

    Giant/dwarf classification uses the combined logg boundary:
      teff >= 4800 : diagonal 5e-4*teff + 1.4  (through (4800, 3.8))
      4000 <= teff < 4800 : flat at 3.8
      teff < 4000 : 0.0023*teff - 5.4
    Stars below the boundary get the giant iterative polynomial (model D).
    Stars above the boundary get DR19 MS dwarf or M-dwarf correction.
    Metal-poor M-dwarfs ([M/H] < -0.6) have logg censored to NaN.

    Returns (logg_cal, is_giant, is_ms_dwarf, is_m_dwarf, n_iter).
    """
    # Giant iterative polynomial (model D)
    COEFF_D = np.array([
        0.35694673, -0.07801092, -0.18306129,
       -0.07847194,  0.24208357,  0.09102127,
       -0.00585212, -0.01810008, -0.05627984, -0.01893467,
    ])
    _G_MIN, _G_MAX = 0.5, 3.5
    _Z_MIN, _Z_MAX = -2.0, 0.6

    def _design_matrix_2d(x, y, deg):
        cols = []
        for d in range(deg + 1):
            for i in range(d + 1):
                cols.append(x**i * y**(d - i))
        return np.column_stack(cols)

    def _correction_D(g, z):
        return _design_matrix_2d(np.clip(g, _G_MIN, _G_MAX),
                                 np.clip(z, _Z_MIN, _Z_MAX), 3) @ COEFF_D

    logg_spec = np.asarray(logg_spec, dtype=float)
    mh        = np.asarray(mh,        dtype=float)
    teff      = np.asarray(teff,       dtype=float)

    # Combined dwarf logg boundary (continuous at teff = 4000 and 4800)
    logg_boundary = np.where(teff >= 4800,  5e-4 * teff + 1.4,
                    np.where(teff >= 4000,  3.8,
                                            0.0023 * teff - 5.4))

    finite     = np.isfinite(logg_spec) & np.isfinite(mh) & np.isfinite(teff)
    above_diag = finite & (logg_spec > logg_boundary)
    is_giant    = finite & ~above_diag
    is_ms_dwarf = above_diag & (teff > 4250)
    is_m_dwarf  = above_diag & (teff <= 4250)

    logg_cal = np.full_like(logg_spec, np.nan)

    # Giant: iterative correction (function of asteroseismic logg, so iterate)
    n_iter = 0
    if is_giant.any():
        logg_est = logg_spec[is_giant].copy()
        mh_g, spec_g = mh[is_giant], logg_spec[is_giant]
        for n_iter in range(1, maxiter + 1):
            logg_new = spec_g - _correction_D(logg_est, mh_g)
            fin      = np.isfinite(logg_new) & np.isfinite(logg_est)
            delta    = np.max(np.abs(logg_new[fin] - logg_est[fin])) if fin.any() else 0.0
            logg_est = logg_new
            if delta < tol:
                break
        logg_cal[is_giant] = logg_est

    ## If you want to remove the dwarf corrections, add this code and comment the rest
    # ## HACK to skip the dwarfs for now
    # logg_cal = np.array(logg_est, dtype=float)
    # is_giant = np.ones_like(logg_cal, dtype=bool)
    # is_ms_dwarf = np.zeros_like(logg_cal, dtype=bool)
    # is_m_dwarf = np.zeros_like(logg_cal, dtype=bool)

    # MS dwarfs: DR19 correction
    logg_cal[is_ms_dwarf] = (logg_spec[is_ms_dwarf]
                             - (-0.947 + 1.886e-4 * teff[is_ms_dwarf]
                                       + 0.410   * mh[is_ms_dwarf]))

    # M-dwarfs: DR19 correction; censor metal-poor ones
    t, z = teff[is_m_dwarf], mh[is_m_dwarf]
    md_cal = (logg_spec[is_m_dwarf]
              + 1.6930001
              + 5.2043072e-07 * (t - 3800)**2
              - 3.6010967e-04 * t
              - 1.7316068 * z
              + 4.0915239e-04 * z * t)
    md_cal[z < -0.6] = np.nan   # censor metal-poor M-dwarfs
    logg_cal[is_m_dwarf] = md_cal
    
    return logg_cal, is_giant, is_ms_dwarf, is_m_dwarf, n_iter

def apply_ipl4v2_parameter_corrections(batch_size: int = 500):
    """
    Apply the IPL-4v2 corrections to the ASPCAP table.
    Teff: Guilherme Limberg calibrated to GHB09 J-K Teff
    M/H: no corrections yet
    X/M: no corrections yet
    """

    q = ASPCAP.select()
    with tqdm(total=0, desc="Applying corrections") as pb:
        for chunk in chunked(q, batch_size):
            updated = []
            raw_loggs = []
            raw_mhs = []
            raw_teffs = []
            raw_e_teffs = []

            ## Pull all the relevant informtion for calculation corrections
            ## note: Andy tells me that I cannot loop over chunk twice
            ## but Gemini says I can because it makes a list
            ## https://github.com/coleifer/peewee/blob/master/peewee.py#L419
            ## If Andy's right the code doesn't work as it loops a second time
            ## But he/Ilija will take care of it later
            for r in chunk:
                raw_loggs.append(r.raw_logg)
                raw_mhs.append(r.raw_m_h_atm)
                raw_teffs.append(r.raw_teff)
                raw_e_teffs.append(r.raw_e_teff)
            
            raw_loggs = np.array(raw_loggs) 
            raw_mhs = np.array(raw_mhs)
            raw_teffs = np.array(raw_teffs)
            raw_e_teffs = np.array(raw_e_teffs)

            ## Compute corrections

            (logg_cal_array, logg_is_giant, logg_is_ms_dwarf,
             logg_is_m_dwarf, n_iter) = get_ipl4v2_logg_corrections(raw_loggs, raw_mhs, raw_teffs)

            (teff_cal_array, e_teff_cal_array,
             teff_is_giant, teff_is_dwarf) = get_ipl4v2_teff_corrections(
                 raw_loggs, raw_mhs, raw_teffs, e_teff=raw_e_teffs)

            ## Update rows
            ## Note: this logic may need updating if we can't loop it!
            ## Also not sure about flag setting
            for (r, logg_cal_val, is_giant_val, is_ms_dw_val, is_m_dw_val,
                 teff_cal_val, e_teff_cal_val) in zip(
                    chunk, logg_cal_array, logg_is_giant, logg_is_ms_dwarf,
                    logg_is_m_dwarf, teff_cal_array, e_teff_cal_array):

                update_logg = np.isfinite(logg_cal_val)
                update_teff = np.isfinite(teff_cal_val)

                if update_logg:
                    r.logg = logg_cal_val
                    r.flag_as_giant_for_calibration    = bool(is_giant_val)
                    r.flag_as_dwarf_for_calibration    = bool(is_ms_dw_val)
                    r.flag_as_m_dwarf_for_calibration  = bool(is_m_dw_val)
                    ## TODO there might be a bug here for non-finite m dwarf logg other than MH
                    if bool(is_m_dw_val) and not np.isfinite(logg_cal_val):
                        r.flag_censored_logg_for_metal_poor_m_dwarf = True

                if update_teff:
                    r.teff = teff_cal_val
                    if e_teff_cal_val is not None and np.isfinite(e_teff_cal_val):
                        r.e_teff = float(e_teff_cal_val)
                    ## TODO probably need to set some flags here, but I don't think they exist yet

                if update_logg or update_teff:
                    updated.append(r)

            if updated:
                fields = [
                    ASPCAP.logg,
                    ASPCAP.teff,
                    ASPCAP.e_teff,
                    ASPCAP.calibrated_flags,
                ]
                (
                    ASPCAP
                    .bulk_update(updated, fields=fields)
                )
            pb.update(batch_size)

def get_dr21_teff_corrections(logg, mh, teff, e_teff=None):
    """
    Apply DR21 Teff corrections.
    These are identical to the IPL-4v2 Teff corrections:
    Guilherme Limberg calibrated to GHB09 J-K Teff (2nd-degree poly in [M/H]).

    KNOWN ARTIFACT / OPEN DECISION (APJ, 2026-07-27): the giant and dwarf
    polynomials do not agree on the classification boundary. At solar [M/H] the
    giant branch adds +65 K and the dwarf branch subtracts 105 K, so switching
    between them here drives the two populations 178 K towards each other and
    folds them past one another -- giants land on the dwarf side and vice versa.
    On DR17 that is a +28% ridge along the boundary with a -46% hole beside it,
    running through the subgiant branch (logg 3.80 at Teff 4800 K down to 3.00 at
    6200 K, crossing logg 3.5 at 5325 K).

    This is deliberately left uncorrected here. Smoothly interpolating the two
    branches removes the artifact but is not a defensible description of a
    subgiant's Teff correction -- it averages two calibrations neither of which
    was fit to subgiants. A real decision is needed on how the branches should
    meet: extend Guilherme's giant branch up in logg, fit subgiants explicitly,
    or move to a single smooth surface in (Teff, [M/H]) as DR16 Eq. (1) did,
    which had no giant/dwarf split and so never had this problem. See
    `teff_boundary_blend_debug.py` for the blend experiment and its diagnostics.

    Returns (teff_corr, e_teff_corr, is_giant, is_dwarf).
    e_teff_corr is None when e_teff is not supplied.
    Unclassified stars (missing teff/logg) get NaN in teff_corr.
    """
    logg  = np.asarray(logg,  dtype=float)
    mh    = np.asarray(mh,    dtype=float)
    teff  = np.asarray(teff,  dtype=float)
    if e_teff is not None:
        e_teff = np.asarray(e_teff, dtype=float)

    # Luminosity classification using Kiel diagram boundaries
    mask_missing = ~np.isfinite(teff) | ~np.isfinite(logg)
    boundary1 = (-4580.0 + 0.4 * teff) / (-700.0)
    boundary2 = 0.0023 * teff - 5.4

    is_giant = (
        ~mask_missing & (
              ((teff < 4000)  & (logg < boundary2))
            | ((teff >= 4000) & (teff < 4800)  & (logg < 3.8))
            | ((teff >= 4800) & (teff <= 6200) & (logg < boundary1))
            | ((teff > 6200)  & (logg < 3.0))
        )
    )
    is_dwarf = ~mask_missing & ~is_giant

    teff_corr   = np.full_like(teff, np.nan)
    e_teff_corr = np.full_like(teff, np.nan) if e_teff is not None else None

    # Giants: clip [M/H] to [-2.2, 0.4], coefficients A=44.7, B=10.2, C=65.2, sys_err=84.3 K
    feh_g = np.clip(mh, -2.2, 0.4)
    giant_offset = 44.7 * feh_g**2 + 10.2 * feh_g + 65.2
    teff_corr[is_giant] = teff[is_giant] + giant_offset[is_giant]
    if e_teff is not None:
        e_teff_corr[is_giant] = np.sqrt(e_teff[is_giant]**2 + 84.3**2)

    # Dwarfs: clip [M/H] to [-0.9, 0.4], coefficients A=243.1, B=-69.0, C=-105.1, sys_err=161.4 K
    feh_d = np.clip(mh, -0.9, 0.4)
    dwarf_offset = 243.1 * feh_d**2 - 69.0 * feh_d - 105.1
    teff_corr[is_dwarf] = teff[is_dwarf] + dwarf_offset[is_dwarf]
    if e_teff is not None:
        e_teff_corr[is_dwarf] = np.sqrt(e_teff[is_dwarf]**2 + 161.4**2)

    return teff_corr, e_teff_corr, is_giant, is_dwarf


def get_dr21_logg_corrections(logg_spec, mh, teff, maxiter=100, tol=1e-4):
    """
    DR21 logg corrections

    Only calculates logg corrections for giants right now.
    This correction was defined by a fit to APOKASC3.
    To reduce problems with the red clump, we define the result as a 
    function of the asteroseismic logg, so we iterate to convergence.
    The results work well for subgiant asteroseismology too (Serenelli et al. 2017).
    Dwarf stars are currently not calibrated, but we keep their value
    clipped to the giant correction for a global offset so as not to introduce a discontinuity in the logg distribution.

    KNOWN ARTIFACT (APJ, 2026-07-27): the clip leaves the correction continuous
    but not differentiable -- dD/dlogg drops from -0.24 (at solar [M/H]) to 0 in
    one step at _G_MAX. Because the calibration is implicit, that slope jump
    becomes a step in the spectroscopic->calibrated Jacobian (1.51 -> 1.00), so
    stars just below 3.5 are spread ~30% further apart than stars just above and
    DR17 shows a 14% density cliff along logg = 3.5 at every Teff. No individual
    star moves discontinuously; only the density does, which is why it is
    invisible in residual plots and shows up as an edge in the Kiel diagram.
    Tapering the clip fixes it -- see `logg_clip_taper_debug.py` for the
    experiment, the width trade-off, and the diagnostics.

    Returns (logg_cal, is_giant, is_ms_dwarf, is_m_dwarf, n_iter).
    """
    # Giant iterative polynomial (model D, called "D" for unimportant historical reasons)
    COEFF_D = np.array([
        0.35694673, -0.07801092, -0.18306129,
       -0.07847194,  0.24208357,  0.09102127,
       -0.00585212, -0.01810008, -0.05627984, -0.01893467,
    ])
    _G_MIN, _G_MAX = 0.5, 3.5
    _Z_MIN, _Z_MAX = -2.0, 0.6

    def _design_matrix_2d(x, y, deg):
        cols = []
        for d in range(deg + 1):
            for i in range(d + 1):
                cols.append(x**i * y**(d - i))
        return np.column_stack(cols)

    def _correction_D(g, z):
        return _design_matrix_2d(np.clip(g, _G_MIN, _G_MAX),
                                 np.clip(z, _Z_MIN, _Z_MAX), 3) @ COEFF_D

    logg_spec = np.asarray(logg_spec, dtype=float)
    mh        = np.asarray(mh,        dtype=float)
    teff      = np.asarray(teff,       dtype=float)

    finite      = np.isfinite(logg_spec) & np.isfinite(mh) & np.isfinite(teff)
    is_giant    = finite # for now everything uses a giant calibration
    # Dwarfs are not calibrated yet, so nothing is classified as one.
    is_ms_dwarf = np.zeros_like(is_giant, dtype=bool)
    is_m_dwarf  = np.zeros_like(is_giant, dtype=bool)

    logg_cal = np.full_like(logg_spec, np.nan)

    # Giant: iterative correction (function of asteroseismic logg, so iterate)
    n_iter = 0
    if is_giant.any():
        logg_est = logg_spec[is_giant].copy()
        mh_logg, spec_logg = mh[is_giant], logg_spec[is_giant]
        for n_iter in range(1, maxiter + 1):
            logg_new = spec_logg - _correction_D(logg_est, mh_logg)
            fin      = np.isfinite(logg_new) & np.isfinite(logg_est)
            delta    = np.max(np.abs(logg_new[fin] - logg_est[fin])) if fin.any() else 0.0
            logg_est = logg_new
            if delta < tol:
                break
        logg_cal[is_giant] = logg_est

    return logg_cal, is_giant, is_ms_dwarf, is_m_dwarf, n_iter


def apply_dr21_parameter_corrections(batch_size: int = 500):
    """
    Apply the DR21 corrections to the ASPCAP table.
    Teff: Guilherme Limberg calibrated to GHB09 J-K Teff (same as IPL-4v2)
    logg: giants only for now
    X/M: solar neighbourhood abundance corrections, applied last
    """

    q = ASPCAP.select()
    with tqdm(total=0, desc="Applying corrections") as pb:
        for chunk in chunked(q, batch_size):
            updated = []
            raw_loggs = []
            raw_mhs = []
            raw_teffs = []
            raw_e_teffs = []

            ## Pull all the relevant informtion for calculation corrections
            ## note: Andy tells me that I cannot loop over chunk twice
            ## but Gemini says I can because it makes a list
            ## https://github.com/coleifer/peewee/blob/master/peewee.py#L419
            ## If Andy's right the code doesn't work as it loops a second time
            ## But he/Ilija will take care of it later
            for r in chunk:
                raw_loggs.append(r.raw_logg)
                raw_mhs.append(r.raw_m_h_atm)
                raw_teffs.append(r.raw_teff)
                raw_e_teffs.append(r.raw_e_teff)

            raw_loggs = np.array(raw_loggs)
            raw_mhs = np.array(raw_mhs)
            raw_teffs = np.array(raw_teffs)
            raw_e_teffs = np.array(raw_e_teffs)

            ## Compute corrections

            (logg_cal_array, logg_is_giant, logg_is_ms_dwarf,
             logg_is_m_dwarf, n_iter) = get_dr21_logg_corrections(raw_loggs, raw_mhs, raw_teffs)

            (teff_cal_array, e_teff_cal_array,
             teff_is_giant, teff_is_dwarf) = get_dr21_teff_corrections(
                 raw_loggs, raw_mhs, raw_teffs, e_teff=raw_e_teffs)

            ## Update rows
            ## Note: this logic may need updating if we can't loop it!
            ## Also not sure about flag setting
            for (r, logg_cal_val, is_giant_val, is_ms_dw_val, is_m_dw_val,
                 teff_cal_val, e_teff_cal_val) in zip(
                    chunk, logg_cal_array, logg_is_giant, logg_is_ms_dwarf,
                    logg_is_m_dwarf, teff_cal_array, e_teff_cal_array):

                update_logg = np.isfinite(logg_cal_val)
                update_teff = np.isfinite(teff_cal_val)

                if update_logg:
                    r.logg = logg_cal_val
                    r.flag_as_giant_for_calibration    = bool(is_giant_val)
                    r.flag_as_dwarf_for_calibration    = bool(is_ms_dw_val)
                    r.flag_as_m_dwarf_for_calibration  = bool(is_m_dw_val)
                    ## TODO there might be a bug here for non-finite m dwarf logg other than MH
                    if bool(is_m_dw_val) and not np.isfinite(logg_cal_val):
                        r.flag_censored_logg_for_metal_poor_m_dwarf = True

                if update_teff:
                    r.teff = teff_cal_val
                    if e_teff_cal_val is not None and np.isfinite(e_teff_cal_val):
                        r.e_teff = float(e_teff_cal_val)
                    ## TODO probably need to set some flags here, but I don't think they exist yet

                if update_logg or update_teff:
                    updated.append(r)

            if updated:
                fields = [
                    ASPCAP.logg,
                    ASPCAP.teff,
                    ASPCAP.e_teff,
                    ASPCAP.calibrated_flags,
                ]
                (
                    ASPCAP
                    .bulk_update(updated, fields=fields)
                )
            pb.update(batch_size)


def apply_dr16_parameter_corrections(batch_size: int = 500):
    """
    Apply the DR16 corrections from arXiv:2007.05537 to the ASPCAP table.
    """

    q = ASPCAP.select()

    def logg_correction_dwarf(r):
        return r.raw_logg - (-0.947 + 1.886e-4 * r.raw_teff + 0.410 * r.raw_m_h_atm) # Eq 2.

    def logg_correction_rgb(r):
        logg_prime = np.clip(r.raw_logg, 1.2795, None)
        m_h_atm_prime = np.clip(r.raw_m_h_atm, -2.5, 0.5)
        return r.raw_logg - (-0.441 + 0.7588 * logg_prime - 0.2667 * logg_prime**2 + 0.02819 * logg_prime**3 + 0.1346 * m_h_atm_prime) # Eq (4)

    def logg_correction_red_clump(r):
        return r.raw_logg - (-4.532 + 3.222 * r.raw_logg - 0.528 * r.raw_logg**2)

    def teff_correction(r):
        m_h_atm_prime = np.clip(r.raw_m_h_atm, -2.5, 0.75)
        teff_prime = np.clip(r.raw_teff, 4500, 7000)
        return r.raw_teff + 610.81 - 4.275 * m_h_atm_prime - 0.116 * teff_prime # Eq (1)

    def e_stellar_param_correction(r, theta):
        X = np.array([1, (r.raw_teff - 4500), np.clip(r.snr - 100, 100, None), r.raw_m_h_atm])
        return np.exp(np.atleast_2d(theta) @ X)

    def e_teff_correction(r):
        return e_stellar_param_correction(r, [4.583, 2.965e-4, -2.177e-3, -0.117])#, 98])

    def e_logg_correction_dwarf(r):
        return e_stellar_param_correction(r, [-2.327, -1.349e-4, 2.269e-4, -0.306])#, 0.10])

    def e_logg_correction_red_clump(r):
        return e_stellar_param_correction(r, [-3.444, 9.584e-4, -5.617e-4, -0.181])#, 0.03])

    def e_logg_correction_rgb(r):
        return e_stellar_param_correction(r, [-2.923, 2.296e-4, 6.900e-4, -0.277])#, 0.05])

    updated = []
    for r in tqdm(q, desc="Applying corrections"):

        r.teff = teff_correction(r)
        r.e_teff = e_teff_correction(r)
        dT = r.raw_teff - (4400 + 552.6 * (r.raw_logg - 2.5) - 324.6 * r.raw_m_h_atm)

        if r.raw_logg > 4 or r.raw_teff > 6000:
            # dwarf
            r.logg = logg_correction_dwarf(r) #
            r.e_logg = e_logg_correction_dwarf(r)
        elif 2.38 < r.raw_logg < 3.5 and ((r.raw_c_m_atm - r.raw_n_m_atm) > 0.04 - 0.46 * r.raw_m_h_atm - 0.0028 * dT):
            # red clump stars
            r.logg = logg_correction_red_clump(r) # Eq (3)
            r.e_logg = e_logg_correction_red_clump(r)
        elif r.raw_logg <= 3.5 and r.raw_teff <= 6000:
            # RGB stars
            r.logg = logg_correction_rgb(r)
            r.e_logg = e_logg_correction_rgb(r)
        elif 3.5 < r.raw_logg <= 4.0 and r.raw_teff <= 6000:
            # weighted correction
            w = (4 - r.raw_logg)/(4 - 3.5)
            # • for stars with uncalibrated 3.5 <log g< 4.0 and Teff< 6000 K, a correction is determined using both the RGB and dwarf corrections, and a weighted correction is adopted based on log g.
            r.logg = (w * logg_correction_dwarf(r) + (1 - w) * logg_correction_rgb(r))
            r.e_logg = (w * e_logg_correction_dwarf(r) + (1 - w) * e_logg_correction_rgb(r))
        else:
            raise ValueError("argh")

        # TODO: uncertainties in abundances.

        r.calibrated = True
        updated.append(r)

    N_updated = 0
    with tqdm(total=len(updated), desc="Saving") as pb:
        for chunk in chunked(updated, batch_size):
            N_updated += (
                ASPCAP
                .bulk_update(
                    chunk,
                    fields=[
                        ASPCAP.teff,
                        ASPCAP.logg,
                        ASPCAP.e_teff,
                        ASPCAP.e_logg,
                        ASPCAP.calibrated
                    ]
                )
            )
            pb.update(N_updated)

    return N_updated
