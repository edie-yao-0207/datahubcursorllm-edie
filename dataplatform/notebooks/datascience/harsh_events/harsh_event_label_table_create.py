from samsaradev.datascience.labeling.csv import read_and_write_csvs
from samsaradev.datascience.labeling.harsh_event_label_helpers import (
    create_harsh_event_crash_label_view,
)
from samsaradev.datascience.labeling.hev2_sheet_manifest import (
    CRASH_VIEW_CONFIGS,
    HEV2_LABEL_SHEET_CONFIGS,
)

read_and_write_csvs(
    spark, HEV2_LABEL_SHEET_CONFIGS, "datascience.raw_harsh_event_labels"
)
create_harsh_event_crash_label_view(
    spark,
    CRASH_VIEW_CONFIGS,
    "datascience.raw_harsh_event_labels",
    "datascience.crash_labels",
)
