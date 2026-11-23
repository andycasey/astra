"""
Apply the DR21 ASPCAP corrections (same steps and order as scripts/dr21_aspcap_corrections.py)
to only the ASPCAP rows written since a given time, i.e. the rows re-run for andycasey/astra#159.

    python aspcap_subset_corrections.py "2026-11-24 09:00"      # check only, changes nothing
    python aspcap_subset_corrections.py "2026-11-24 09:00" --apply

Use the time the ASPCAP re-run started. An ASPCAP run is the only thing that changes
`aspcap.modified` (the corrections do not), so `modified >= start` selects exactly the rows
written by that run.
"""
import sys

from astra.models.aspcap import ASPCAP
from astra.pipelines.aspcap import corrections

EXPECTED = 82_268   # rows backdated for the re-run (remediate.sql, statement 3)

if len(sys.argv) < 2:
    sys.exit(__doc__)
start = sys.argv[1]
apply = "--apply" in sys.argv[2:]

where = (ASPCAP.modified >= start)

n = ASPCAP.select().where(where).count()
# A row straight out of the pipeline has calibrated_flags NULL (no default); every corrected
# row has a non-zero value (apply_dr21_parameter_corrections always sets bits).
n_already_corrected = ASPCAP.select().where(where & (ASPCAP.calibrated_flags != 0)).count()
n_still_backdated = ASPCAP.select().where(ASPCAP.modified == "1970-01-01").count()

print(f"Rows with modified >= {start}:       {n:,}   (expected about {EXPECTED:,})")
print(f"  of which already corrected:         {n_already_corrected:,}   (must be 0)")
print(f"Rows still waiting to re-run (1970): {n_still_backdated:,}   (should be 0)")

if n_already_corrected > 0:
    sys.exit(
        "Refusing to run: some selected rows are already corrected. The abundance-flag step "
        "would convert their flags a second time. Check the start time."
    )
if n_still_backdated > 0:
    print("WARNING: the ASPCAP re-run has not finished; those rows would not be corrected now.")
if not apply:
    sys.exit("Check only. Re-run with --apply to write the corrections.")

print("1/5 apply_dr21_parameter_corrections")
corrections.apply_dr21_parameter_corrections(where=where)
print("2/5 apply_dr19_solar_neighbourhood_abundance_corrections")
corrections.apply_dr19_solar_neighbourhood_abundance_corrections(where=where)
print("3/5 flag_boundaries_and_abundances")
# These rows come straight from the pipeline, so their {X}_flags are in the FERRE layout.
corrections.flag_boundaries_and_abundances(where=where, ferre_layout=True)
print("4/5 flag_upper_limits_by_hayes_2022")
corrections.flag_upper_limits_by_hayes_2022(where=where)
print("5/5 apply_flags")
corrections.apply_flags(where=where)

print(
    "Done. Rows now corrected:",
    ASPCAP.select().where(where & (ASPCAP.calibrated_flags != 0)).count(),
    "of", n,
)
