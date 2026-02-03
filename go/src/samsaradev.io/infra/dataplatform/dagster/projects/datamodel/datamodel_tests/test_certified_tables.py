from datetime import datetime

from datamodel.common.datahub_utils import get_asset_specs_by_key
from datamodel.ops.datahub.certified_tables import (
    beta_tables,
    certified_tables,
    excluded_tables,
    views,
)


def test_expired_beta_tables():
    expired_beta_tables = []

    for table, timestamp in beta_tables.items():
        if datetime.now() > datetime.strptime(timestamp, "%Y-%m-%d"):
            expired_beta_tables.append(table)

    assert not expired_beta_tables, (
        f"Expired beta tables found, you must either certify them or extend their expiration date (with approval): "
        + ", ".join(expired_beta_tables)
    )


def test_no_overlapping_tables():
    overlapping_tables = (
        set(beta_tables.keys()) & set(certified_tables) & set(excluded_tables)
    )
    assert not overlapping_tables, (
        f"Tables found in both beta/exclusion and certified tables, you must either certify them or remove them from beta/exclusion: "
        + ", ".join(overlapping_tables)
    )


def test_certified_tables():

    repo_def = get_asset_specs_by_key()

    uncertified_tables = []

    unexpired_beta_tables = {
        table
        for table, timestamp in beta_tables.items()
        if datetime.now() <= datetime.strptime(timestamp, "%Y-%m-%d")
    }

    for asset_key, _ in repo_def.items():
        if len(asset_key.path) != 3:
            continue

        region, database, table = asset_key.path

        if (
            not table.startswith("dq_")
            and region == "us-west-2"
            and database
            in [
                "datamodel_core",
                "datamodel_telematics",
                "datamodel_safety",
                "datamodel_platform",
                "product_analytics",
                "dataengineering",
            ]
        ):
            if (
                f"{database}.{table}" not in certified_tables
                and f"{database}.{table}" not in unexpired_beta_tables
                and f"{database}.{table}" not in excluded_tables
                and f"{database}.{table}" not in views
            ):
                uncertified_tables.append(f"{database}.{table}")

    if uncertified_tables:
        error_message = (
            "The following tables are not certified. All new gold layer tables must be certified or added to beta table list/exclusion list:\n"
            + "\n".join(uncertified_tables)
        )
        assert False, error_message
    else:
        print("All tables are certified.")
