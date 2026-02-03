"""
fct_signal_promotion_transitions

Captures all state transitions for signal promotions with previous stage/status context.
Each row represents a transition point in a promotion's lifecycle.
"""

from dagster import AssetExecutionContext
from dataweb import build_general_dq_checks, table
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    FIRMWAREVDP,
    FRESHNESS_SLO_9AM_PST,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    columns_to_schema,
    get_primary_keys,
    get_non_null_columns,
    struct_with_comments,
    map_of,
)
from dataweb.userpkgs.firmware.table import ProductAnalyticsStaging, SignalPromotionDb
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import build_table_description

# Common columns reused across signal promotion assets
PROMOTION_UUID_COLUMN = Column(
    name="promotion_uuid",
    type=DataType.STRING,
    nullable=True,
    primary_key=True,
    metadata=Metadata(comment="Unique identifier for the promotion"),
)

POPULATION_UUID_COLUMN = Column(
    name="population_uuid",
    type=DataType.STRING,
    metadata=Metadata(comment="Population UUID associated with the promotion"),
)

DATA_SOURCE_COLUMN = Column(
    name="data_source",
    type=DataType.INTEGER,
    nullable=True,
    metadata=Metadata(
        comment="Data source ID from signal. Join to definitions.data_source for name."
    ),
)

CREATED_BY_COLUMN = Column(
    name="created_by",
    type=DataType.LONG,
    metadata=Metadata(
        comment="User ID who created the promotion. Join to datamodel_platform.dim_users for user details."
    ),
)

USER_ID_COLUMN = Column(
    name="user_id",
    type=DataType.LONG,
    metadata=Metadata(
        comment="User ID who performed this transition. Join to datamodel_platform.dim_users for user details."
    ),
)

# Shared struct types for current and previous state
CURRENT_STATE_STRUCT_TYPE = struct_with_comments(
    (
        "stage",
        DataType.SHORT,
        "Stage ID (0=INITIAL, 1=FAILED, 2=PRE_ALPHA, 3=ALPHA, 4=BETA, 5=GA). Join to definitions.promotion_stage for names.",
    ),
    (
        "status",
        DataType.SHORT,
        "Status ID (0=UNDEFINED, 1=LIVE, 2=PENDING_APPROVAL, 3=PENDING_ROLLBACK). Join to definitions.promotion_status for names.",
    ),
    (
        "activity",
        DataType.SHORT,
        "Activity type (0=UNDEFINED, 1=CREATED, 2=PENDING_APPROVAL, 3=PENDING_ROLLBACK, 4=APPROVED, 5=REJECTED, 6=POPULATION_IGNORE_VIN_ENABLED, 7=POPULATION_IGNORE_VIN_DISABLED, 8=RESTORED). Join to definitions.activity_type for names.",
    ),
)

PREVIOUS_STATE_STRUCT_TYPE = struct_with_comments(
    (
        "stage",
        DataType.SHORT,
        "Previous stage ID. NULL for first transition.",
    ),
    (
        "status",
        DataType.SHORT,
        "Previous status ID. NULL for first transition.",
    ),
    (
        "activity",
        DataType.SHORT,
        "Previous activity type. NULL for first transition.",
    ),
)

# Promotion outcome classification
PROMOTION_OUTCOME_COLUMN = Column(
    name="promotion_outcome",
    type=DataType.STRING,
    metadata=Metadata(
        comment="Mutually exclusive classification: SUCCESS (BETA+LIVE), PRODUCTION_FAILURE (rollback from BETA/GA), ALPHA_ROLLBACK (rollback from Alpha testing), PROMOTION_FROM_TRAINING_DATA (derived from ML training labels), IN_PROGRESS (all other states including early-stage failures)"
    ),
)

# Shared state columns using the struct types
CURRENT_STATE_COLUMN = Column(
    name="current",
    type=CURRENT_STATE_STRUCT_TYPE,
    metadata=Metadata(comment="Current state (stage, status, activity)"),
)

PREVIOUS_STATE_COLUMN = Column(
    name="previous",
    type=PREVIOUS_STATE_STRUCT_TYPE,
    nullable=True,
    metadata=Metadata(
        comment="Previous state before this transition. NULL for first transition."
    ),
)

COMMENT_COLUMN = Column(
    name="comment",
    type=DataType.STRING,
    nullable=True,
    metadata=Metadata(comment="Comment associated with the promotion activity"),
)

# Parsed comment struct - validates and extracts structured labels, tags, and computes audit flags
PARSED_COMMENT_COLUMN = Column(
    name="parsed_comment",
    type=struct_with_comments(
        (
            "labels",
            map_of(
                key_type=DataType.STRING,
                value_type=DataType.BOOLEAN,
                value_contains_null=True,
            ),
            "Map of all parsed labels (key) to validity (true=valid, false=invalid). Valid labels: INTERNAL_TESTING, IS_COVERED, HAS_CONFIG_ISSUE, PROMOTION_FROM_TRAINING_DATA, EXISTING_PROMOTION, ACTIVE_REQUEST.",
        ),
        (
            "tags",
            map_of(
                key_type=DataType.STRING,
                value_type=DataType.BOOLEAN,
                value_contains_null=True,
            ),
            "Map of all parsed tags (key) to validity (true=valid, false=invalid). Required only when HAS_CONFIG_ISSUE label is present. 26 valid tags: UNKNOWN, WRONG_BIT_START, WRONG_BIT_LENGTH, WRONG_BIT_OFFSET, WRONG_SCALE, WRONG_OFFSET, WRONG_BOUNDS, WRONG_POLARITY, WRONG_MAPPING, WRONG_CAN_ID, WRONG_REQUEST_ID, WRONG_RESPONSE_ID, WRONG_OBD_MODE, WRONG_PID, WRONG_PGN, WRONG_OBD_PROTOCOL, WRONG_ENDIANNESS, WRONG_SIGNEDNESS, WRONG_UNIT, WRONG_FRAME_RATE, STATE_MAPPING_INCOMPLETE, STATE_DEFAULTED, MUX_MISMATCH, CHECKSUM_MISMATCH, NOT_PRESENT_IN_TRACE, INTERNAL_TESTING.",
        ),
        (
            "note",
            DataType.STRING,
            "Free-text note extracted from comment",
        ),
        (
            "is_comment_valid",
            DataType.BOOLEAN,
            "Validation status for rollback transitions only. NULL=not a rollback (format not expected), TRUE=rollback with valid formatted comment (labels valid, tags valid if HAS_CONFIG_ISSUE present), FALSE=rollback with missing/invalid comment format.",
        ),
        (
            "is_rollback_from_testing",
            DataType.BOOLEAN,
            "True if promotion rolled back from ALPHA(3) or BETA(4) to FAILED(1)",
        ),
        (
            "is_internal_testing",
            DataType.BOOLEAN,
            "True if promotion has INTERNAL_TESTING label (exclude from success metrics)",
        ),
        (
            "is_promotion_from_training_data",
            DataType.BOOLEAN,
            "True if promotion has PROMOTION_FROM_TRAINING_DATA label (derived from ML training labels)",
        ),
        (
            "is_ml_trainable",
            DataType.BOOLEAN,
            "True if suitable for ML training (IS_COVERED or IS_IN_BETA, excluding HAS_CONFIG_ISSUE/INTERNAL_TESTING)",
        ),
    ),
    nullable=True,
    metadata=Metadata(
        comment="Parsed comment struct (only populated for rollback transitions). Contains labels/tags (mapped to validity), note, and audit flags. is_comment_valid checks if the expected structured format is present and valid for rollbacks. Tags are only required when HAS_CONFIG_ISSUE label is present."
    ),
)

COLUMNS = [
    PROMOTION_UUID_COLUMN,
    ColumnType.SIGNAL_UUID,
    POPULATION_UUID_COLUMN,
    DATA_SOURCE_COLUMN,
    Column(
        name="created_at_ms",
        type=DataType.LONG,
        primary_key=True,
        metadata=Metadata(
            comment="Timestamp in milliseconds from activity_history when this activity occurred"
        ),
    ),
    Column(
        name="created_at_date",
        type=DataType.DATE,
        metadata=Metadata(comment="Date derived from created_at_ms"),
    ),
    Column(
        name="updated_at_ms",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment="Timestamp in milliseconds from promotions table when the promotion record was last updated"
        ),
    ),
    Column(
        name="updated_at_date",
        type=DataType.DATE,
        nullable=True,
        metadata=Metadata(comment="Date derived from updated_at_ms"),
    ),
    CURRENT_STATE_COLUMN,
    PREVIOUS_STATE_COLUMN,
    CREATED_BY_COLUMN,
    USER_ID_COLUMN,
    COMMENT_COLUMN,
    PARSED_COMMENT_COLUMN,
    PROMOTION_OUTCOME_COLUMN,
]

PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)
SCHEMA = columns_to_schema(*COLUMNS)

QUERY = r"""
WITH valid_types AS (
  -- Define valid label and tag values as arrays (to avoid subqueries in higher-order functions)
  SELECT
    array(
      'INTERNAL_TESTING',
      'IS_COVERED',
      'HAS_CONFIG_ISSUE',
      'PROMOTION_FROM_TRAINING_DATA',
      'EXISTING_PROMOTION',
      'ACTIVE_REQUEST'
    ) AS valid_labels,
    array(
      'UNKNOWN',
      'WRONG_BIT_START',
      'WRONG_BIT_LENGTH',
      'WRONG_BIT_OFFSET',
      'WRONG_SCALE',
      'WRONG_OFFSET',
      'WRONG_BOUNDS',
      'WRONG_POLARITY',
      'WRONG_MAPPING',
      'WRONG_CAN_ID',
      'WRONG_REQUEST_ID',
      'WRONG_RESPONSE_ID',
      'WRONG_OBD_MODE',
      'WRONG_PID',
      'WRONG_PGN',
      'WRONG_OBD_PROTOCOL',
      'WRONG_ENDIANNESS',
      'WRONG_SIGNEDNESS',
      'WRONG_UNIT',
      'WRONG_FRAME_RATE',
      'STATE_MAPPING_INCOMPLETE',
      'STATE_DEFAULTED',
      'MUX_MISMATCH',
      'CHECKSUM_MISMATCH',
      'NOT_PRESENT_IN_TRACE',
      'INTERNAL_TESTING'
    ) AS valid_tags
),

-- Get all transitions from activity_history with status from promotions using ASOF join
transitions_with_status AS (
  SELECT
    ah.promotion_uuid,
    ah.created_at_ms,
    CAST(DATE(FROM_UNIXTIME(ah.created_at_ms / 1000.0)) AS DATE) AS created_at_date,
    ah.activity,
    ah.stage,
    ah.user_id,
    LAG(ah.stage) OVER (
      PARTITION BY ah.promotion_uuid
      ORDER BY ah.created_at_ms
    ) AS previous_stage,
    LAG(ah.activity) OVER (
      PARTITION BY ah.promotion_uuid
      ORDER BY ah.created_at_ms
    ) AS previous_activity,
    p.status,
    p.updated_at_ms,
    CAST(DATE(FROM_UNIXTIME(p.updated_at_ms / 1000.0)) AS DATE) AS updated_at_date,
    LAG(p.status) OVER (
      PARTITION BY ah.promotion_uuid
      ORDER BY ah.created_at_ms
    ) AS previous_status,
    ROW_NUMBER() OVER (
      PARTITION BY ah.promotion_uuid, ah.created_at_ms
      ORDER BY p.created_at_ms DESC
    ) AS status_rn,
    comment
  FROM signalpromotiondb.activity_history ah
  LEFT JOIN signalpromotiondb.promotions p 
    ON ah.promotion_uuid = p.promotion_uuid 
    AND p.created_at_ms <= ah.created_at_ms
),

activity_transitions AS (
  SELECT
    promotion_uuid,
    created_at_ms,
    created_at_date,
    activity,
    stage,
    user_id,
    previous_stage,
    previous_activity,
    status,
    updated_at_ms,
    updated_at_date,
    previous_status,
    comment
  FROM transitions_with_status
  WHERE status_rn = 1
),

-- Get promotion metadata with data_source from signals
promotion_info AS (
  SELECT
    promotion_uuid,
    signal_uuid,
    population_uuid,
    created_by,
    data_source
  FROM (
    SELECT
      p.promotion_uuid,
      p.signal_uuid,
      p.population_uuid,
      p.created_by,
      s.data_source,
      ROW_NUMBER() OVER (
        PARTITION BY p.promotion_uuid
        ORDER BY p.created_at_ms DESC
      ) AS rn
    FROM signalpromotiondb.promotions p
    LEFT JOIN signalpromotiondb.signals s ON p.signal_uuid = s.signal_uuid
  ) t
  WHERE rn = 1
),

-- Parse all comment fields once (labels, tags, note, validation)
parsed_comment_fields AS (
  SELECT
    t.promotion_uuid,
    t.created_at_ms,
    t.stage,
    t.previous_stage,
    -- Parse and split raw labels/tags arrays
    -- Pattern matches: LABEL= followed by values until semicolon, space+uppercase (next field), or end
    CASE
      WHEN t.comment RLIKE 'LABEL=' THEN
        SPLIT(REGEXP_EXTRACT(t.comment, 'LABEL=([^;]+?)(?:;|\\s+[A-Z]|$)', 1), '\\|')
      ELSE ARRAY()
    END AS raw_labels,
    CASE
      WHEN t.comment RLIKE 'TAGS=' THEN
        SPLIT(REGEXP_EXTRACT(t.comment, 'TAGS=([^;]+?)(?:;|\\s+[A-Z]|$)', 1), '\\|')
      ELSE ARRAY()
    END AS raw_tags,
    -- Extract note (can contain spaces, stops at semicolon or end)
    CASE 
      WHEN t.comment IS NOT NULL AND t.comment RLIKE 'NOTE=' THEN
        TRIM(REGEXP_EXTRACT(t.comment, 'NOTE=([^;]+)', 1))
      ELSE NULL
    END AS note
  FROM activity_transitions t
),

-- Build combined maps from parsed arrays (valid=true, invalid=false)
comment_maps AS (
  SELECT
    pcf.promotion_uuid,
    pcf.created_at_ms,
    pcf.stage,
    pcf.previous_stage,
    pcf.note,
    -- Combined labels map: all labels with validity flag (deduplicated)
    CASE 
      WHEN SIZE(FILTER(pcf.raw_labels, label -> LENGTH(label) > 0)) > 0 THEN
        MAP_FROM_ARRAYS(
          ARRAY_DISTINCT(FILTER(pcf.raw_labels, label -> LENGTH(label) > 0)),
          TRANSFORM(
            ARRAY_DISTINCT(FILTER(pcf.raw_labels, label -> LENGTH(label) > 0)),
            label -> ARRAY_CONTAINS(vt.valid_labels, label)
          )
        )
      ELSE NULL
    END AS labels_map,
    -- Combined tags map: all tags with validity flag (deduplicated)
    CASE 
      WHEN SIZE(FILTER(pcf.raw_tags, tag -> LENGTH(tag) > 0)) > 0 THEN
        MAP_FROM_ARRAYS(
          ARRAY_DISTINCT(FILTER(pcf.raw_tags, tag -> LENGTH(tag) > 0)),
          TRANSFORM(
            ARRAY_DISTINCT(FILTER(pcf.raw_tags, tag -> LENGTH(tag) > 0)),
            tag -> ARRAY_CONTAINS(vt.valid_tags, tag)
          )
        )
      ELSE NULL
    END AS tags_map
  FROM parsed_comment_fields pcf
  CROSS JOIN valid_types vt
),

-- Compute derived validation and audit flags (rename to match struct field names)
comment_validation_base AS (
 SELECT
    cm.promotion_uuid,
    cm.created_at_ms,
    cm.labels_map,
    cm.tags_map,
    cm.note,
    cm.stage,
    (cm.stage = 1 AND cm.previous_stage IN (3, 4)) AS is_rollback_from_testing
  FROM comment_maps cm
),

comment_validation AS (
  SELECT
    cvb.promotion_uuid,
    cvb.created_at_ms,
    cvb.labels_map AS labels,
    cvb.tags_map AS tags,
    cvb.note,
    cvb.is_rollback_from_testing,
    -- Validation: TRUE if valid structured content, FALSE if missing/invalid
    -- (Only used for rollbacks; non-rollbacks get entire struct set to NULL in main SELECT)
    -- Tags are only required when HAS_CONFIG_ISSUE label is present and valid
    CASE
      WHEN cvb.labels_map IS NULL AND cvb.tags_map IS NULL AND cvb.note IS NULL THEN FALSE  -- Missing expected format
      WHEN NOT ARRAY_CONTAINS(COALESCE(MAP_VALUES(cvb.labels_map), ARRAY()), FALSE) THEN
        -- Labels are valid, now check tags only if HAS_CONFIG_ISSUE is present and valid
        CASE
          WHEN COALESCE(cvb.labels_map['HAS_CONFIG_ISSUE'], FALSE) = TRUE THEN
            -- HAS_CONFIG_ISSUE is present and valid, so tags must be present and valid
            NOT ARRAY_CONTAINS(COALESCE(MAP_VALUES(cvb.tags_map), ARRAY()), FALSE) AND cvb.tags_map IS NOT NULL
          ELSE TRUE  -- HAS_CONFIG_ISSUE not present or invalid, tags validation not required
        END
      ELSE FALSE  -- Invalid labels present
    END AS is_comment_valid,
    -- Audit flags (label values: true=valid&present, false=invalid&present, NULL=absent)
    COALESCE(cvb.labels_map['INTERNAL_TESTING'], FALSE) AS is_internal_testing,
    COALESCE(cvb.labels_map['PROMOTION_FROM_TRAINING_DATA'], FALSE) AS is_promotion_from_training_data,
    CASE
      WHEN COALESCE(cvb.labels_map['INTERNAL_TESTING'], FALSE) THEN FALSE
      WHEN COALESCE(cvb.labels_map['HAS_CONFIG_ISSUE'], FALSE) THEN FALSE
      WHEN COALESCE(cvb.labels_map['IS_COVERED'], FALSE) THEN TRUE
      WHEN cvb.stage = 4 THEN TRUE  -- IS_IN_BETA derived from stage
      ELSE FALSE
    END AS is_ml_trainable
  FROM comment_validation_base cvb
)

SELECT
  t.promotion_uuid,
  pi.signal_uuid,
  pi.population_uuid,
  pi.data_source,
  t.created_at_ms,
  t.created_at_date,
  t.updated_at_ms,
  t.updated_at_date,
  STRUCT(t.stage AS stage, t.status AS status, t.activity AS activity) AS current,
  STRUCT(t.previous_stage AS stage, t.previous_status AS status, t.previous_activity AS activity) AS previous,
  pi.created_by,
  t.user_id,
  t.comment,
  
  -- Use pre-computed parsed comment from comment_validation CTE (only for rollback transitions)
  CASE
    WHEN cv.is_rollback_from_testing THEN
      STRUCT(
        cv.labels,
        cv.tags,
        cv.note,
        cv.is_comment_valid,
        cv.is_rollback_from_testing,
        cv.is_internal_testing,
        cv.is_promotion_from_training_data,
        cv.is_ml_trainable
      )
    ELSE NULL
  END AS parsed_comment,
  
  -- Mutually exclusive outcome classification
  CASE
    WHEN t.stage = 4 AND t.status = 1 THEN 'SUCCESS'  -- BETA + LIVE
    WHEN t.previous_stage IN (4, 5) AND t.stage = 1 THEN 'PRODUCTION_FAILURE'  -- Rollback from BETA/GA
    WHEN t.previous_stage = 3 AND t.stage = 1 THEN 'ALPHA_ROLLBACK'  -- Rollback from Alpha testing
    WHEN cv.is_promotion_from_training_data THEN 'PROMOTION_FROM_TRAINING_DATA'  -- Derived from ML training labels
    ELSE 'IN_PROGRESS'  -- All other states (includes early-stage, transitions, other failures)
  END AS promotion_outcome

FROM activity_transitions t
LEFT JOIN promotion_info pi USING (promotion_uuid)
LEFT JOIN comment_validation cv ON t.promotion_uuid = cv.promotion_uuid AND t.created_at_ms = cv.created_at_ms
ORDER BY t.promotion_uuid, t.created_at_ms
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="All state transitions for signal promotions, showing activity, stage, status, and previous values. "
        "External dependencies: signalpromotiondb.promotions, signalpromotiondb.activity_history. "
        "Definition tables: definitions.promotion_stage, definitions.promotion_status, definitions.activity_type.",
        row_meaning="Each row represents a single state transition in a promotion's lifecycle.",
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    upstreams=[
        AnyUpstream(SignalPromotionDb.PROMOTIONS),
        AnyUpstream(SignalPromotionDb.ACTIVITY_HISTORY),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_SIGNAL_PROMOTION_TRANSITIONS.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
)
def fct_signal_promotion_transitions(context: AssetExecutionContext) -> str:
    return QUERY
