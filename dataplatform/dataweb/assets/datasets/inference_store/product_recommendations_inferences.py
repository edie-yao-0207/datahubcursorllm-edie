import multiprocessing as mp
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from pandas.api.types import is_integer_dtype, is_float_dtype
from dagster import AssetExecutionContext, WeeklyPartitionsDefinition
from dataweb import table
from dataweb.userpkgs.constants import (
    DATAENGINEERING,
    AWSRegion,
    Database,
    WarehouseWriteMode,
    TableType,
    InstanceType,
)
from dataweb.userpkgs.query import create_run_config_overrides
from dataweb.userpkgs.utils import build_table_description, get_run_env
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score
from sklearn.model_selection import StratifiedKFold, cross_val_predict
from sklearn.preprocessing import MaxAbsScaler, OneHotEncoder
import optuna

RANDOM_STATE = 123
MIN_ELIGIBILITY = 0.05  # Drop products eligible for < 5% of base
OPTUNA_STARTUP = 10
OPTUNA_TRIALS = 30

SCHEMA = [
    {"name": "org_id", "type": "long", "nullable": False, "metadata": {}},
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "The date the features were generated for inferences."},
    },
    {
        "name": "product",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "The product being recommended."},
    },
    {
        "name": "goodness_of_fit",
        "type": "double",
        "nullable": False,
        "metadata": {"comment": "The goodness of fit of the model."},
    },
    {
        "name": "last_adopted",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The last date the product was adopted (org_active_month = 1) in the past 182 days."
        },
    },
    {
        "name": "is_currently_adopted",
        "type": "integer",
        "nullable": True,
        "metadata": {
            "comment": "Most recent adoption status (org_active_month) in the past 182 days. Usually 0 since adopted products are excluded from recommendations. A value of 1 indicates adoption occurred after the model training snapshot."
        },
    },
    {
        "name": "is_disadopted",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "1 if org was using the product within the last 84 days but is no longer using it. Used to filter out recommendations for recently churned products. 0 otherwise."
        },
    },
]


def train_single_product_model(args):
    """
    Train a model for a single product. Runs in a separate process.

    Args:
        args: Tuple of (j, prod_name, X_dense, A_binary_full, A_val_eval, Y_train, Y_val, Y_full)

    Returns:
        Tuple of (j, prod_name, final_scores, val_scores)
    """
    # Suppress optuna logging
    optuna.logging.set_verbosity(optuna.logging.WARNING)

    j, prod_name, X_dense, A_binary_full, A_val_eval, Y_train, Y_val, Y_full = args

    # Feature construction
    cols_keep = np.arange(A_binary_full.shape[1]) != j
    X_aug_full = np.hstack([X_dense, A_binary_full[:, cols_keep]])
    X_aug_val = np.hstack([X_dense, A_val_eval[:, cols_keep]])

    # Labels
    mask_tr = (Y_train[:, j] != -99) & (~np.isnan(Y_train[:, j]))
    y_tr = Y_train[mask_tr, j].astype(int)
    X_tr = X_aug_full[mask_tr]

    # Validation set for tuning
    mask_val_opt = ~np.isnan(Y_val[:, j])
    y_val_opt = Y_val[mask_val_opt, j].astype(int)
    X_val_opt = X_aug_val[mask_val_opt]

    # Initialize score arrays
    final_scores = np.zeros(Y_full.shape[0])
    val_scores = np.zeros(Y_full.shape[0])

    # Skip degenerate cases
    if len(np.unique(y_tr)) < 2:
        fill = np.mean(y_tr) if len(y_tr) > 0 else 0.0
        final_scores[:] = fill
        val_scores[:] = fill
        return (j, prod_name, final_scores, val_scores)

    # Optuna tuning
    best_params = None
    if len(y_val_opt) > 10 and len(np.unique(y_val_opt)) > 1:

        def objective(trial):
            clf = RandomForestClassifier(
                n_estimators=trial.suggest_int("n_estimators", 75, 600),
                max_depth=trial.suggest_int("max_depth", 5, 25),
                min_samples_split=trial.suggest_int("min_samples_split", 2, 20),
                min_samples_leaf=trial.suggest_int("min_samples_leaf", 2, 15),
                max_samples=trial.suggest_float("max_samples", 0.5, 0.9),
                max_features=trial.suggest_categorical(
                    "max_features", ["sqrt", "log2", 0.5, 0.8]
                ),
                class_weight="balanced_subsample",
                n_jobs=1,  # Each process uses 1 core
                random_state=RANDOM_STATE,
            )
            clf.fit(X_tr, y_tr)

            if len(np.unique(y_val_opt)) < 2:
                return 0.0

            preds = clf.predict_proba(X_val_opt)[:, 1]
            return average_precision_score(y_val_opt, preds)

        sampler = optuna.samplers.TPESampler(
            seed=RANDOM_STATE, n_startup_trials=OPTUNA_STARTUP
        )
        study = optuna.create_study(direction="maximize", sampler=sampler)
        study.optimize(objective, n_trials=OPTUNA_TRIALS, show_progress_bar=False)
        best_params = study.best_params
    else:
        best_params = {
            "n_estimators": 600,
            "max_depth": 20,
            "min_samples_split": 10,
            "min_samples_leaf": 5,
            "max_features": "sqrt",
            "max_samples": 0.7,
        }

    # Final fit with cross-validated calibration
    rf_final = RandomForestClassifier(
        **best_params,
        class_weight="balanced_subsample",
        n_jobs=1,  # Each process uses 1 core
        random_state=RANDOM_STATE,
    )

    skf = StratifiedKFold(n_splits=3, shuffle=True, random_state=RANDOM_STATE)
    y_scores_cv = cross_val_predict(
        rf_final, X_tr, y_tr, cv=skf, method="predict_proba", n_jobs=1
    )[:, 1]

    rf_final.fit(X_tr, y_tr)

    # Calibration
    pos = (y_tr == 1).sum()
    neg = (y_tr == 0).sum()
    calibrator = None

    if pos >= 100 and neg >= 100:
        calibrator = IsotonicRegression(out_of_bounds="clip")
        calibrator.fit(y_scores_cv, y_tr)
    elif pos >= 20 and neg >= 20:
        calibrator = LogisticRegression(C=1.0, solver="lbfgs", max_iter=100)
        calibrator.fit(y_scores_cv.reshape(-1, 1), y_tr)

    # Prediction
    def apply_calibration(raw_probs, calibrator):
        if calibrator is None:
            return raw_probs
        if isinstance(calibrator, IsotonicRegression):
            return calibrator.predict(raw_probs)
        elif isinstance(calibrator, LogisticRegression):
            return calibrator.predict_proba(raw_probs.reshape(-1, 1))[:, 1]
        return raw_probs

    final_scores = apply_calibration(
        rf_final.predict_proba(X_aug_full)[:, 1], calibrator
    )
    val_scores = apply_calibration(rf_final.predict_proba(X_aug_val)[:, 1], calibrator)

    return (j, prod_name, final_scores, val_scores)


def load_and_merge_features(spark, source_db, partition_key):
    """Load adoption and organization features from source and merge them."""
    # Fetch adoption features
    df_adopt = spark.sql(
        f"""
        SELECT * FROM {source_db}.product_recommendations_adoption_features
        WHERE date = '{partition_key}'
    """
    )

    # Fetch organization features
    df_org = spark.sql(
        f"""
        SELECT * FROM {source_db}.product_recommendations_organization_features
        WHERE date = '{partition_key}'
    """
    )

    # Pivot adoption features
    df_wide_adopt = (
        df_adopt.groupBy("org_id")
        .pivot("feature")
        .agg(F.max("org_active_month"))
        .fillna(-99)
    )

    # Deduplicate org features if needed
    org_count = df_org.count()
    org_unique = df_org.select("org_id").distinct().count()
    if org_count > org_unique:
        df_org = df_org.orderBy(F.desc("tenure")).dropDuplicates(subset=["org_id"])

    # Merge
    df_merged = df_wide_adopt.join(df_org.drop("date"), on="org_id", how="inner")
    return df_merged.toPandas()


def detect_product_columns(pdf):
    """Detect which columns are product adoption columns (values in {-99, 0, 1})."""
    prod_cols = []
    for c in pdf.columns:
        if c == "org_id":
            continue
        vals = pdf[c].dropna().unique()
        if len(vals) > 0 and set(vals).issubset({-99.0, 0.0, 1.0, -99, 0, 1}):
            if len(vals) > 1:
                prod_cols.append(c)
    return prod_cols


def filter_products_by_eligibility(pdf, prod_cols, min_eligibility=MIN_ELIGIBILITY):
    """Filter products to those with sufficient eligibility rate."""
    Y_raw = pdf[prod_cols].to_numpy()
    eligibility_rates = (Y_raw != -99).mean(axis=0)
    keep_mask = eligibility_rates >= min_eligibility
    return [p for i, p in enumerate(prod_cols) if keep_mask[i]]


def vectorize_features(pdf, feat_cols):
    """Transform feature columns into numerical matrix."""
    num_cols = list(pdf[feat_cols].select_dtypes(include=[np.number, "bool"]).columns)
    cat_cols = list(pdf[feat_cols].select_dtypes(exclude=[np.number]).columns)

    preprocessor = ColumnTransformer(
        [
            ("num", MaxAbsScaler(), num_cols),
            (
                "cat",
                OneHotEncoder(handle_unknown="ignore", sparse_output=False),
                cat_cols,
            ),
        ],
        remainder="drop",
        sparse_threshold=0,
    )

    return preprocessor.fit_transform(pdf[feat_cols])


def create_binary_coadoption_matrix(Y):
    """Convert adoption matrix to binary co-adoption (treating -99 as 0)."""
    A = Y.copy()
    A[A == -99] = 0.0
    return (A > 0.5).astype(np.float32)


def create_loao_split(Y_full, random_state=RANDOM_STATE):
    """Create Leave-One-Adopted-Out train/validation split."""
    rng = np.random.default_rng(random_state)
    adoption_rates = np.mean(Y_full, axis=0, where=(Y_full != -99))
    median_rate = np.median(adoption_rates[adoption_rates > 0])
    n_neg_val = max(5, min(15, int(10 * (1 - median_rate))))

    Y_train = Y_full.copy()
    Y_val = np.full_like(Y_full, np.nan)

    for i, row in enumerate(Y_full):
        elig_idx = np.where(row != -99)[0]
        pos_idx = np.where(row[elig_idx] == 1)[0]
        neg_idx = np.where(row[elig_idx] == 0)[0]

        if len(pos_idx) >= 2 and len(neg_idx) >= 5:
            p_sel = rng.choice(elig_idx[pos_idx], 1, replace=False)
            neg_pool = elig_idx[neg_idx]
            n_neg = min(n_neg_val, len(neg_pool))
            if n_neg > 0:
                n_sel = rng.choice(neg_pool, size=n_neg, replace=False)
                Y_val[i, p_sel] = 1.0
                Y_val[i, n_sel] = 0.0
                Y_train[i, p_sel] = np.nan
                Y_train[i, n_sel] = np.nan

    return Y_train, Y_val


def load_disadoption_data(spark, partition_latest_date, product_usage_global_table):
    """
    Load disadoption metrics for each organization-product pair.

    Args:
        spark: SparkSession
        partition_latest_date: The last date of the partition week for filtering
        product_usage_global_table: The table name for agg_product_usage_global (region-specific)

    Returns:
        pandas DataFrame with columns: org_id, product, last_adopted, is_currently_adopted
    """

    df_disadoption = spark.sql(
        f"""--sql
        SELECT
            org_id
            , feature
            , MAX(CASE WHEN org_active_month = 1 THEN date ELSE NULL END) AS last_adopted
            , MAX_BY(org_active_month, date) AS is_currently_adopted
        FROM {product_usage_global_table}
        WHERE
            date <= '{partition_latest_date}'
            AND date >= DATE_SUB(CAST('{partition_latest_date}' AS DATE), 182)
        GROUP BY org_id, feature
    --endsql"""
    )

    # Apply the same feature name transformation as in product_recommendations_adoption_features
    # Convert to lowercase and replace spaces/hyphens with underscores to match product names
    # Explicitly cast is_currently_adopted to integer to ensure consistent schema
    df_disadoption = (
        df_disadoption.withColumn(
            "product", F.regexp_replace(F.lower(F.col("feature")), r"[ -]+", "_")
        )
        .withColumn(
            "is_currently_adopted", F.col("is_currently_adopted").cast("integer")
        )
        .drop("feature")
    )

    return df_disadoption.toPandas()


def prepare_feature_matrices(spark, source_db, partition_key):
    """Complete data preparation pipeline shared by driver and workers."""
    # Load and merge data
    pdf = load_and_merge_features(spark, source_db, partition_key)

    # Detect product columns
    prod_cols = detect_product_columns(pdf)
    feat_cols = [c for c in pdf.columns if c not in prod_cols and c != "org_id"]

    # Filter products by eligibility
    final_prod_cols = filter_products_by_eligibility(pdf, prod_cols)

    # Prepare matrices
    Y_full = pdf[final_prod_cols].to_numpy(dtype=np.float32)
    X_dense = vectorize_features(pdf, feat_cols)
    A_binary_full = create_binary_coadoption_matrix(Y_full)
    Y_train, Y_val = create_loao_split(Y_full)

    # Create validation co-adoption matrix (zero out held-out items)
    A_val_eval = A_binary_full.copy()
    rows_val, cols_val = np.where(Y_val == 1.0)
    A_val_eval[rows_val, cols_val] = 0.0

    return {
        "pdf": pdf,
        "final_prod_cols": final_prod_cols,
        "X_dense": X_dense,
        "A_binary_full": A_binary_full,
        "A_val_eval": A_val_eval,
        "Y_train": Y_train,
        "Y_val": Y_val,
        "Y_full": Y_full,
    }


@table(
    database=Database.INFERENCE_STORE,
    description=build_table_description(
        table_desc="""Table containing model inference results for product recommendations, including input features, model goodness-of-fit, and adoption status for each organization-product-date combination.""",
        row_meaning="""Each row represents a product recommendation for an organization for a given week""",
        related_table_info={},
        table_type=TableType.AGG,
        freshness_slo_updated_by="Weekly on Monday by 12pm PST",
    ),
    schema=SCHEMA,
    primary_keys=["org_id", "date", "product"],
    partitioning=WeeklyPartitionsDefinition(start_date="2025-10-01"),
    upstreams=[
        "feature_store.product_recommendations_adoption_features",
        "feature_store.product_recommendations_organization_features",
        "product_analytics.agg_product_usage_global",
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    regions=[AWSRegion.US_WEST_2, AWSRegion.EU_WEST_1],
    owners=[DATAENGINEERING],
    run_config_overrides=create_run_config_overrides(
        max_workers=4,  # Since we're only using the driver node, we can use fewer workers
        driver_instance_type=InstanceType.C_FLEET_4XLARGE,
        worker_instance_type=InstanceType.MD_FLEET_XLARGE,
    ),
    # TODO: Move some in-asset DQ checks to the DQ check Dataweb framework here
)
def product_recommendations_inferences(context: AssetExecutionContext) -> DataFrame:
    """
    PARALLELIZATION APPROACH: Python Multiprocessing on Driver Node

    This function uses Python's multiprocessing.Pool to parallelize model training across
    CPU cores on the driver node, rather than distributing work across a Spark cluster.

    CONSTRAINING FACTORS:
    - Large dataset: Feature matrices + target data total ~100-200MB when serialized
    - Scikit-learn & Optuna: Single-machine ML libraries not designed for distributed computing
        * Cannot natively distribute across Spark workers
        * Require all data in memory on a single machine
        * Would need complete rewrite to use Spark MLlib equivalents
    - Spark Connect: Databricks environment uses Spark Connect with strict limitations:
        * No direct SparkContext access (no RDD support)
        * Strict gRPC message size limits (~10-50MB)
        * Workers cannot create SparkSessions or query tables
    - Sequential processing was too slow: Original implementation took 1:51 for ~50 products

    METHODS ATTEMPTED AND FAILURES:

    1. RDD-based Parallelization (sparkContext.parallelize)
       - Failure: Spark Connect doesn't support sparkContext access
       - Error: "Attribute `sparkContext` is not supported in Spark Connect"

    2. DataFrame with Serialized Data (mapInPandas with embedded data)
       - Failure: Hit gRPC message size limits when embedding serialized data in rows
       - Error: "CodedInputStream encountered an embedded string or message which
                 claimed to have negative size"

    3. File-Based Data Sharing (DBFS temp files)
       - Failure: DBFS /tmp doesn't support os.makedirs in Spark Connect
       - Error: "OSError: [Errno 95] Operation not supported: '/dbfs/tmp'"

    4. Worker-Based Data Reloading (workers query tables independently)
       - Failure: Workers cannot create SparkSessions (fundamental Spark limitation)
       - Error: "SparkContext can only be used on the driver, not in code that runs on workers"

    CURRENT SOLUTION: Python Multiprocessing
    - Uses Python's multiprocessing.Pool on driver node
    - Parallelizes across all CPU cores (typically 8-32 cores on driver)
    - All processes share read-only data via copy-on-write (efficient)
    - No Spark distribution complexity or message size limits
    - Expected speedup: 6-8x faster than sequential (linear with core count)

    OTHER POSSIBLE SOLUTIONS (not implemented):

    1. Pre-materialize Per-Product DataFrames
       - Create separate DataFrame rows with all data needed per product
       - Pros: Could work with mapInPandas
       - Cons: Still risks hitting message size limits; complex serialization logic

    2. Increase Driver Node Resources
       - Request larger driver instance with more cores
       - Pros: Current approach benefits directly from more cores
       - Cons: Cost increase; doesn't fundamentally solve cluster scaling

    3. Use Spark MLlib Instead of Scikit-learn
       - Replace RandomForest/sklearn with pyspark.ml equivalents
       - Pros: Native Spark distribution
       - Cons: Major rewrite; MLlib less feature-rich; model quality may differ

    4. Hybrid: Databricks Jobs API
       - Submit separate jobs for product batches via REST API
       - Pros: True cluster scaling
       - Cons: Complex orchestration; slower startup; not suitable for Dagster workflows
    """
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    partition_key = context.partition_key
    context.log.info(f"Partition key: {partition_key}; type: {type(partition_key)}")

    source = "feature_store"
    if get_run_env() == "dev":
        source = "datamodel_dev"

    # Determine the appropriate table reference based on region
    region = context.asset_key.path[0]
    if region != AWSRegion.US_WEST_2:
        # Product usage global table aggregates data from all regions and runs in the US only.
        # That table is then delta shared to other regions.
        product_usage_global_table = (
            "data_tools_delta_share.product_analytics.agg_product_usage_global"
        )
    else:
        product_usage_global_table = (
            "default.product_analytics.agg_product_usage_global"
        )
    context.log.info(f"Using product usage global table: {product_usage_global_table}")

    # Prepare feature matrices using shared function
    context.log.info("Loading and preparing feature matrices...")
    data = prepare_feature_matrices(spark, source, partition_key)

    pdf = data["pdf"]
    final_prod_cols = data["final_prod_cols"]
    X_dense = data["X_dense"]
    A_binary_full = data["A_binary_full"]
    A_val_eval = data["A_val_eval"]
    Y_train = data["Y_train"]
    Y_val = data["Y_val"]
    Y_full = data["Y_full"]

    context.log.info(
        f"Prepared data: {len(pdf)} organizations, {len(final_prod_cols)} products"
    )
    context.log.info(
        f"Validation samples: {(~np.isnan(Y_val)).sum()} total, {(Y_val == 1.0).sum()} positives"
    )

    # Parallel training using Python multiprocessing on driver
    context.log.info(
        f"Training models for {len(final_prod_cols)} products using multiprocessing..."
    )

    # Prepare arguments for each product
    training_args = [
        (j, prod_name, X_dense, A_binary_full, A_val_eval, Y_train, Y_val, Y_full)
        for j, prod_name in enumerate(final_prod_cols)
    ]

    # Use multiprocessing to parallelize across CPU cores
    # Use 7/8 of the cores to avoid overloading the driver memory
    n_cores = int(mp.cpu_count() * 7 / 8)
    context.log.info(f"Using {n_cores} CPU cores for parallel training")

    # Train models in parallel with progress logging
    final_scores_all = np.zeros_like(Y_full)
    val_scores_all = np.zeros_like(Y_full)
    completed_count = 0

    with mp.Pool(processes=n_cores) as pool:
        # Use imap_unordered to get results as they complete
        for j, prod_name, final_scores, val_scores in pool.imap_unordered(
            train_single_product_model, training_args
        ):
            # Store results as they arrive
            final_scores_all[:, j] = final_scores
            val_scores_all[:, j] = val_scores

            # Log progress
            completed_count += 1
            if (
                completed_count % 5 == 0
                or completed_count == 1
                or completed_count == len(final_prod_cols)
            ):
                context.log.info(
                    f"Training progress: {completed_count}/{len(final_prod_cols)} products completed ({completed_count/len(final_prod_cols):.0%})"
                )

    context.log.info("Parallel model training complete")

    # Generate output
    context.log.info("Generating predictions table...")
    org_ids = pdf["org_id"].to_numpy()
    output_rows = []

    for i in range(len(org_ids)):
        # Eligible (!-99) AND Not Adopted
        elig_indices = np.where(Y_full[i] != -99)[0]

        for j in elig_indices:
            # Skip already adopted products
            if Y_full[i, j] == 1.0:
                continue

            score = float(final_scores_all[i, j])
            # Optional: Filter very low scores to save space
            if score > 0.01:
                output_rows.append((int(org_ids[i]), final_prod_cols[j], score))

    df_out = pd.DataFrame(output_rows, columns=["org_id", "product", "goodness_of_fit"])

    # Merge output with disadoption metrics for each org-product as of the end of the partition week
    partition_latest_date = (
        datetime.strptime(partition_key, "%Y-%m-%d") + timedelta(days=6)
    ).strftime("%Y-%m-%d")
    context.log.info(
        f"Loading disadoption data for partition_latest_date = {partition_latest_date}"
    )
    df_disadoption = load_disadoption_data(
        spark, partition_latest_date, product_usage_global_table
    )
    context.log.info(f"Loaded {len(df_disadoption)} disadoption records")

    # Left join to add disadoption metrics
    df_out = df_out.merge(df_disadoption, on=["org_id", "product"], how="left")
    context.log.info(
        f"Joined disadoption data: {len(df_out)} total records, {df_out['last_adopted'].notna().sum()} with disadoption history"
    )

    # Calculate is_disadopted: 1 if org was using product within last 84 days but stopped
    # Used to filter out recommendations for recently churned products
    # Note: String comparison of 'yyyy-mm-dd' format works correctly since lexicographic order matches chronological order
    disadoption_cutoff = (
        datetime.strptime(partition_latest_date, "%Y-%m-%d") - timedelta(days=84)
    ).strftime("%Y-%m-%d")
    df_out["is_disadopted"] = (
        df_out["last_adopted"].notna()
        & (df_out["last_adopted"] >= disadoption_cutoff)
        & (df_out["is_currently_adopted"] == 0)
    ).astype(int)
    disadopted_count = df_out["is_disadopted"].sum()
    context.log.info(
        f"Identified {disadopted_count} disadopted org-product pairs (last_adopted >= {disadoption_cutoff}, will be filtered from recommendations)"
    )

    # Validation Tests
    context.log.info("Running validation tests...")

    def test_output_not_empty():
        n_recs = len(df_out)
        assert (
            n_recs > 0
        ), f"Output is empty - all {n_recs} predictions had scores below 0.01 threshold or inference otherwise failed"
        context.log.info(f"✓ Output not empty: {n_recs} predictions generated")

    def test_output_schema():
        expected_cols = {
            "org_id",
            "product",
            "goodness_of_fit",
            "last_adopted",
            "is_currently_adopted",
            "is_disadopted",
        }
        assert (
            set(df_out.columns) == expected_cols
        ), f"Output schema mismatch. Expected {expected_cols}, got {set(df_out.columns)}"
        assert is_integer_dtype(df_out["org_id"]), "org_id should be integer dtype"
        assert is_float_dtype(
            df_out["goodness_of_fit"]
        ), "goodness_of_fit should be float dtype"
        assert is_integer_dtype(
            df_out["is_disadopted"]
        ), "is_disadopted should be integer dtype"
        context.log.info("✓ Output schema valid")

    def test_score_ranges():
        s = df_out["goodness_of_fit"]
        assert s.notna().all(), "Scores contain NaN"
        assert np.isfinite(s).all(), "Scores contain non-finite values"
        mn, mx = float(s.min()), float(s.max())
        assert mn >= 0.0, "Scores contain negatives"
        assert mx <= 1.0, "Scores exceed 1.0"
        context.log.info(f"✓ Score range valid: [{mn:.4f}, {mx:.4f}]")

    def test_no_duplicate_recommendations():
        n_rows = len(df_out)
        n_unique = df_out[["org_id", "product"]].drop_duplicates().shape[0]
        assert (
            n_rows == n_unique
        ), f"Found {n_rows - n_unique} duplicate recommendations"
        context.log.info("✓ No duplicate recommendations")

    def test_recommendations_per_org():
        # Cap per-org recs by per-org eligibility
        elig_per_org = (Y_full != -99).sum(axis=1)
        recs_per_org = (
            df_out.groupby("org_id").size().reindex(org_ids, fill_value=0).to_numpy()
        )
        assert (recs_per_org >= 0).all()
        assert (
            recs_per_org <= elig_per_org
        ).all(), "Some orgs have more recs than eligible products"
        context.log.info(
            f"✓ Recommendations per org look reasonable (max observed={recs_per_org.max()})"
        )

    def test_all_products_processed():
        # If a model was skipped (constant labels), you fill with the mean—variance may be tiny.
        std_by_prod = np.std(final_scores_all, axis=0)
        frac_constant = (std_by_prod < 1e-9).mean()
        assert (
            frac_constant < 0.50
        ), f"Too many products have constant predictions ({frac_constant:.0%})"
        context.log.info(
            f"✓ {(1-frac_constant):.0%} of products have varying predictions"
        )

    def test_score_matrix_completeness():
        assert final_scores_all.shape == Y_full.shape, "Final scores shape mismatch"
        assert val_scores_all.shape == Y_full.shape, "Validation scores shape mismatch"
        assert (
            np.isfinite(final_scores_all).all() and np.isfinite(val_scores_all).all()
        ), "Non-finite entries in score matrices"
        assert (
            final_scores_all.sum() > 0.0
        ), "Final scores sum to zero (model likely didn't run)"
        context.log.info("✓ Score matrices complete and populated")

    def test_no_already_adopted_products():
        # Ensure no (org, product) already adopted is recommended
        prod_to_idx = {p: i for i, p in enumerate(final_prod_cols)}
        adopted_count = 0
        for r in df_out.itertuples(index=False):
            i = np.where(org_ids == r.org_id)[0]
            if len(i) == 0:
                continue
            j = prod_to_idx[r.product]
            adopted_count += int(Y_full[i[0], j] == 1.0)
        assert (
            adopted_count == 0
        ), f"Found {adopted_count} recs for already-adopted products"
        context.log.info("✓ No recommendations for already-adopted products")

    def test_no_ineligible_products():
        # Ensure no ineligible products are recommended
        prod_to_idx = {p: i for i, p in enumerate(final_prod_cols)}
        ineligible = 0
        for r in df_out.itertuples(index=False):
            i = np.where(org_ids == r.org_id)[0]
            if len(i) == 0:
                continue
            j = prod_to_idx[r.product]
            ineligible += int(Y_full[i[0], j] == -99)
        assert ineligible == 0, f"Found {ineligible} ineligible recommendations"
        context.log.info("✓ No recommendations for ineligible products")

    def test_output_volume_reasonable():
        # Expected volume bounds based on eligibility
        elig_total = int((Y_full != -99).sum())
        min_expected = max(1, int(0.05 * elig_total))
        max_expected = int(0.80 * elig_total)  # shouldn't recommend almost everything
        n_recs = len(df_out)
        assert (
            min_expected <= n_recs <= max_expected
        ), f"Output volume {n_recs} outside expected range [{min_expected}, {max_expected}]"
        context.log.info(
            f"✓ Output volume reasonable: {n_recs:,}/{elig_total:,} eligible ({n_recs/elig_total:.1%})"
        )

    tests = [
        test_output_not_empty,
        test_output_schema,
        test_score_ranges,
        test_no_duplicate_recommendations,
        test_recommendations_per_org,
        test_all_products_processed,
        test_score_matrix_completeness,
        test_no_already_adopted_products,
        test_no_ineligible_products,
        test_output_volume_reasonable,
    ]

    results = []
    for t in tests:
        try:
            t()
            results.append((t.__name__, "PASSED", ""))
        except AssertionError as e:
            results.append((t.__name__, "FAILED", str(e)))
        except Exception as e:
            results.append((t.__name__, "ERROR", str(e)))

    passed = sum(1 for _, s, _ in results if s == "PASSED")
    total = len(results)
    for name, status, msg in results:
        context.log.info(f"{status:8s} - {name}" + (f" :: {msg}" if msg else ""))

    context.log.info(
        f"Validation: {passed}/{total} tests passed ({passed/total:.0%} success rate)"
    )

    # Create final output with explicit type casting to match expected schema
    # Note: After left join, NaN values cause pandas to convert integer columns to float64
    df_predictions = (
        spark.createDataFrame(df_out)
        .withColumn("date", F.lit(partition_key))
        .withColumn(
            "is_currently_adopted", F.col("is_currently_adopted").cast("integer")
        )
        .withColumn("is_disadopted", F.col("is_disadopted").cast("integer"))
    )
    context.log.info(
        f"Generated {len(df_out)} predictions for {len(org_ids)} organizations"
    )

    return df_predictions
