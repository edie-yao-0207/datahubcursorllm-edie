from dagster import AssetExecutionContext, DailyPartitionsDefinition
from dataweb import NonEmptyDQCheck, NonNullDQCheck, PrimaryKeyDQCheck, table
from dataweb.userpkgs.constants import (
    DATAENGINEERING,
    AWSRegion,
    ColumnDescription,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.utils import (
    build_table_description,
    get_run_env,
    partition_key_ranges_from_context,
)

PRIMARY_KEYS = [
    "date",
    "feature_flag_key",
]

SCHEMA = [
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": ColumnDescription.DATE
        },
    },
    {
        "name": "feature_flag_key",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Name of feature flag"
        },
    },
    {
        "name": "maintainer_email",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Email address of the feature flag maintainer"
        },
    },
    {
        "name": "creation_date",
        "type": "date",
        "nullable": True,
        "metadata": {
            "comment": "Creation date of the feature flag"
        },
    },
    {
        "name": "deprecated",
        "type": "boolean",
        "nullable": True,
        "metadata": {
            "comment": "Whether the feature flag is deprecated or not"
        },
    },
    {
        "name": "deprecated_date",
        "type": "date",
        "nullable": True,
        "metadata": {
            "comment": "Date when the feature flag was deprecated"
        },
    },
    {
        "name": "archived",
        "type": "boolean",
        "nullable": True,
        "metadata": {
            "comment": "Whether the feature flag is archived or not"
        },
    },
    {
        "name": "fallthrough_variation",
        "type": "integer",
        "nullable": True,
        "metadata": {
            "comment": "Default variation that serves FF"
        },
    },
    {
        "name": "on_variation",
        "type": "integer",
        "nullable": True,
        "metadata": {
            "comment": "Default variation when the FF is on"
        },
    },
    {
        "name": "off_variation",
        "type": "integer",
        "nullable": True,
        "metadata": {
            "comment": "Default variation when the FF is off"
        },
    },
    {
        "name": "is_flag_on",
        "type": "boolean",
        "nullable": True,
        "metadata": {
            "comment": "Whether the feature flag is currently active or not"
        },
    },
    {
        "name": "prerequisite_flags",
        "type": {
            "type": "array",
            "elementType": {
                "type": "map",
                "keyType": "string",
                "valueType": "string",
                "valueContainsNull": True
            },
            "containsNull": True
        },
        "nullable": True,
        "metadata": {
            "comment": "Prerequisite flags for the FF"
        },
    },
    {
        "name": "rule_clause_details",
        "type": {
            "type": "array",
            "elementType": {
                "type": "struct",
                "fields": [
                    {
                        "name": "attribute",
                        "type": "string",
                        "nullable": True,
                        "metadata": {
                            "comment": "Type of attribute being evaluated (orgId, orgLocale, etc.)"
                        }
                    },
                    {
                        "name": "op",
                        "type": "string",
                        "nullable": True,
                        "metadata": {
                            "comment": "Type of operator being used (in, =, etc.)"
                        }
                    },
                    {
                        "name": "negate",
                        "type": "boolean",
                        "nullable": True,
                        "metadata": {
                            "comment": "Whether result should be flipped or not"
                        }
                    },
                    {
                        "name": "variation",
                        "type": "integer",
                        "nullable": True,
                        "metadata": {
                            "comment": "Which variation (index) this serves"
                        }
                    },
                    {
                        "name": "values",
                        "type": {
                            "type": "array",
                            "elementType": "string",
                            "containsNull": True
                        },
                        "nullable": True,
                        "metadata": {
                            "comment": "Values from rule clause"
                        }
                    }
                ]
            },
            "containsNull": True
        },
        "nullable": True,
        "metadata": {
            "comment": "Array of rule clause details for targeting"
        },
    },
    {
        "name": "targets",
        "type": {
            "type": "array",
            "elementType": {
                "type": "struct",
                "fields": [
                    {
                        "name": "context_kind",
                        "type": "string",
                        "nullable": True,
                        "metadata": {
                            "comment": "Type of object being targeted (user, org, etc.)"
                        }
                    },
                    {
                        "name": "values",
                        "type": "string",
                        "nullable": True,
                        "metadata": {
                            "comment": "Values from target"
                        }
                    },
                    {
                        "name": "variation",
                        "type": "integer",
                        "nullable": True,
                        "metadata": {
                            "comment": "Variation (index) being served"
                        }
                    }
                ]
            },
            "containsNull": True
        },
        "nullable": True,
        "metadata": {
            "comment": "Array of individual targets for the feature flag"
        },
    },
    {
        "name": "variations",
        "type": {
            "type": "array",
            "elementType": {
                "type": "struct",
                "fields": [
                    {
                        "name": "name",
                        "type": "string",
                        "nullable": True,
                        "metadata": {
                            "comment": "Name of variation"
                        }
                    },
                    {
                        "name": "value",
                        "type": "string",
                        "nullable": True,
                        "metadata": {
                            "comment": "Value of variation"
                        }
                    },
                    {
                        "name": "index",
                        "type": "integer",
                        "nullable": True,
                        "metadata": {
                            "comment": "Index of variation"
                        }
                    }
                ]
            },
            "containsNull": True
        },
        "nullable": True,
        "metadata": {
            "comment": "Array of possible variations for the feature flag"
        },
    },
    {
        "name": "segment_rule_clause_details",
        "type": {
            "type": "array",
            "elementType": {
                "type": "struct",
                "fields": [
                    {
                        "name": "segment_key",
                        "type": "string",
                        "nullable": True,
                        "metadata": {
                            "comment": "Segment where rule clause comes from"
                        }
                    },
                    {
                        "name": "attribute",
                        "type": "string",
                        "nullable": True,
                        "metadata": {
                            "comment": "Type of attribute being evaluated (orgId, orgLocale, etc.)"
                        }
                    },
                    {
                        "name": "op",
                        "type": "string",
                        "nullable": True,
                        "metadata": {
                            "comment": "Type of operator being used (in, =, etc.)"
                        }
                    },
                    {
                        "name": "negate",
                        "type": "boolean",
                        "nullable": True,
                        "metadata": {
                            "comment": "Whether result should be flipped or not"
                        }
                    },
                    {
                        "name": "values",
                        "type": {
                            "type": "array",
                            "elementType": "string",
                            "containsNull": True
                        },
                        "nullable": True,
                        "metadata": {
                            "comment": "Values from rule clause"
                        }
                    }
                ]
            },
            "containsNull": True
        },
        "nullable": True,
        "metadata": {
            "comment": "Array of segment rule clause details"
        },
    },
    {
        "name": "segments",
        "type": {
            "type": "array",
            "elementType": "string",
            "containsNull": True
        },
        "nullable": True,
        "metadata": {
            "comment": "Array of segment keys associated with the feature flag"
        },
    },
    {
        "name": "segment_targets",
        "type": {
            "type": "array",
            "elementType": "string",
            "containsNull": True
        },
        "nullable": True,
        "metadata": {
            "comment": "Array of segment target keys"
        },
    },
]

QUERY = """
WITH initial_flag_details AS (
    SELECT
        key,
        maintainer_email,
        creation_date,
        deprecated,
        DATE(FROM_UNIXTIME(deprecated_date / 1000)) AS deprecated_date,
        archived,
        on_variation,
        off_variation
    FROM {source}.feature_flag
    WHERE
        date = '{PARTITION_START}'
        AND project_key = 'samsara'
),
fallthrough_variation AS (
    SELECT
        feature_flag_key,
        fallthrough_variation,
        is_on
    FROM datamodel_launchdarkly_bronze.feature_flag_environment
    WHERE
        date = '{PARTITION_START}'
        AND feature_flag_project_key = 'samsara'
),
prerequisite_flags AS (
    SELECT
        feature_flag_key,
        COLLECT_LIST(
            MAP(
                'prerequisite_flag_name', key,
                'prerequisite_flag_variation', variation
            )
        ) AS prerequisite_flags
    FROM datamodel_launchdarkly_bronze.feature_flag_environment_prerequisite
    WHERE
        date = '{PARTITION_START}'
        AND feature_flag_project_key = 'samsara'
    GROUP BY feature_flag_key
),
rule_clauses AS (
    SELECT
        ffr.feature_flag_key,
        COLLECT_LIST(STRUCT(
            rc.attribute AS attribute,
            rc.op AS op,
            rc.negate AS negate,
            ffr.variation AS variation,
            FROM_JSON(rc.values, 'array<string>') AS values
        )) AS rule_clause_details
    FROM datamodel_launchdarkly_bronze.feature_flag_environment_rule_clause rc
    JOIN datamodel_launchdarkly_bronze.feature_flag_environment_rule ffr
        ON ffr.id = rc.feature_flag_environment_rule_id
        AND ffr.date = rc.date
    WHERE
        ffr.date = '{PARTITION_START}'
        AND rc.feature_flag_project_key = 'samsara'
    GROUP BY ffr.feature_flag_key
),
targets AS (
    SELECT
        feature_flag_key,
        COLLECT_LIST(STRUCT(
            context_kind,
            values,
            variation
        )) AS targets
    FROM datamodel_launchdarkly_bronze.feature_flag_environment_target
    WHERE
        date = '{PARTITION_START}'
        AND feature_flag_project_key = 'samsara'
    GROUP BY feature_flag_key
),
variations AS (
    SELECT
        ffv.feature_flag_key,
        COLLECT_LIST(STRUCT(
            ffv.name,
            ffv.value,
            vim.variation_index AS index
        )) AS variations
    FROM datamodel_launchdarkly_bronze.feature_flag_variation ffv
    JOIN release_management.ld_flag_variation_index_map vim
        ON ffv.feature_flag_key = vim.key
        AND UPPER(ffv.value) = UPPER(vim.variation_value)
    WHERE
        ffv.date = '{PARTITION_START}'
        AND ffv.feature_flag_project_key = 'samsara'
    GROUP BY ffv.feature_flag_key
),
segment_rule_clauses AS (
    SELECT
        sf.key AS feature_flag_key,
        COLLECT_LIST(STRUCT(
            sf.segment_key AS segment_key,
            rc.attribute AS attribute,
            rc.op AS op,
            rc.negate AS negate,
            FROM_JSON(rc.values, 'array<string>') AS values
        )) AS segment_rule_clause_details
    FROM datamodel_launchdarkly_bronze.rule_clause rc
    JOIN datamodel_launchdarkly_bronze.segment_flag sf
        ON sf.date = rc.date
        AND rc.segment_key = sf.segment_key
    WHERE
        rc.date = '{PARTITION_START}'
        AND sf.project_key = 'samsara'
    GROUP BY sf.key
),
segments AS (
    SELECT
        key AS feature_flag_key,
        COLLECT_LIST(segment_key) AS segments
    FROM datamodel_launchdarkly_bronze.segment_flag
    WHERE
        date = '{PARTITION_START}'
        AND project_key = 'samsara'
    GROUP BY key
),
segment_targets AS (
    SELECT
        sf.key AS feature_flag_key,
        COLLECT_LIST(st.target_key) AS segment_targets
    FROM datamodel_launchdarkly_bronze.segment_target st
    JOIN datamodel_launchdarkly_bronze.segment_flag sf
        ON st.date = sf.date
        AND st.segment_key = sf.segment_key
    WHERE
        st.date = '{PARTITION_START}'
        AND sf.project_key = 'samsara'
    GROUP BY sf.key
)
SELECT
    '{PARTITION_START}' AS date,
    ifd.key AS feature_flag_key,
    ifd.creation_date,
    ifd.deprecated,
    ifd.deprecated_date,
    ifd.archived,
    ifd.off_variation,
    ifd.on_variation,
    ifd.maintainer_email,
    fv.fallthrough_variation,
    fv.is_on AS is_flag_on,
    pf.prerequisite_flags,
    rc.rule_clause_details,
    t.targets,
    v.variations,
    src.segment_rule_clause_details,
    s.segments,
    st.segment_targets
FROM initial_flag_details ifd
LEFT OUTER JOIN fallthrough_variation fv
    ON ifd.key = fv.feature_flag_key
LEFT OUTER JOIN prerequisite_flags pf
    ON ifd.key = pf.feature_flag_key
LEFT OUTER JOIN rule_clauses rc
    ON ifd.key = rc.feature_flag_key
LEFT OUTER JOIN targets t
    ON ifd.key = t.feature_flag_key
LEFT OUTER JOIN variations v
    ON ifd.key = v.feature_flag_key
LEFT OUTER JOIN segment_rule_clauses src
    ON ifd.key = src.feature_flag_key
LEFT OUTER JOIN segments s
    ON ifd.key = s.feature_flag_key
LEFT OUTER JOIN segment_targets st
    ON ifd.key = st.feature_flag_key
"""


@table(
    database=Database.DATAMODEL_LAUNCHDARKLY_SILVER,
    description=build_table_description(
        table_desc="""This dataset helps gather details on each FF, such as the various rule clauses, segments, prerequisite flags, etc.""",
        row_meaning="""Each row represents a breakdown for a given FF""",
        related_table_info={},
        table_type=TableType.STAGING,
        freshness_slo_updated_by="9am PST",
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DailyPartitionsDefinition(start_date="2025-07-01"),
    write_mode=WarehouseWriteMode.OVERWRITE,
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_feature_flag_details"),
        PrimaryKeyDQCheck(
            name="dq_pk_feature_flag_details",
            primary_keys=PRIMARY_KEYS,
            block_before_write=True,
        ),
        NonNullDQCheck(
            name="dq_non_null_feature_flag_details",
            non_null_columns=PRIMARY_KEYS,
            block_before_write=True,
        ),
    ],
    upstreams=[
        "us-west-2:datamodel_launchdarkly_bronze.feature_flag",
        "us-west-2:datamodel_launchdarkly_bronze.feature_flag_environment",
        "us-west-2:datamodel_launchdarkly_bronze.feature_flag_environment_prerequisite",
        "us-west-2:datamodel_launchdarkly_bronze.feature_flag_environment_rule_clause",
        "us-west-2:datamodel_launchdarkly_bronze.feature_flag_environment_rule",
        "us-west-2:datamodel_launchdarkly_bronze.feature_flag_environment_target",
        "us-west-2:datamodel_launchdarkly_bronze.feature_flag_variation",
        "us-west-2:datamodel_launchdarkly_bronze.rule_clause",
        "us-west-2:datamodel_launchdarkly_bronze.segment_flag",
        "us-west-2:datamodel_launchdarkly_bronze.segment_target",
        "us-west-2:release_management.ld_flag_variation_index_map",
    ],
)
def feature_flag_details(context: AssetExecutionContext) -> str:
    context.log.info("Updating feature_flag_details")
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]
    source = "datamodel_launchdarkly_bronze"
    if get_run_env() == "dev":
        source = "datamodel_dev"
    query = QUERY.format(
        PARTITION_START=PARTITION_START,
        source=source,
    )
    return query

