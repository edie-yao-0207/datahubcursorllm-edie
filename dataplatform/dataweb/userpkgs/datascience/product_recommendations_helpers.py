from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, MaxAbsScaler # StandardScaler
from typing import Sequence, Tuple, Union, List, Optional, Callable, Dict, Any
import pandas as pd
import numpy as np
import scipy.sparse as sps
from sklearn.ensemble import RandomForestClassifier
from dataclasses import dataclass
from sklearn.isotonic import IsotonicRegression


@dataclass
class _IsoItem:
    model: Optional[IsotonicRegression]  # None => identity

@dataclass
class RFModel:
    """
    Per-product Random Forest model for leave-one-out prediction.
    Fallback to constant probability when insufficient data.
    """
    estimator: Optional[RandomForestClassifier]
    const_prob: float
    j: int



def _is_eligible(arr: np.ndarray) -> np.ndarray:
    """
    Check if values represent eligibility (not -99 sentinel value).

    Args:
        arr: Array to check for eligibility.

    Returns:
        Boolean array where True indicates eligible entries.
    """
    return arr != -99


def _sentinel_to_num(arr: np.ndarray, fill_value: float = 0.0) -> np.ndarray:
    """
    Replace -99 sentinel values and NaN with a specified fill value.

    Args:
        arr: Array potentially containing -99 sentinel values and NaN.
        fill_value: Value to replace -99 and NaN with (default 0.0).

    Returns:
        Array with -99 and NaN replaced by fill_value (preserves input dtype).
    """
    arr = np.asarray(arr)
    result = arr.copy()
    # Replace both sentinel and NaN values
    result[(arr == -99) | np.isnan(arr)] = fill_value
    return result


def _augment_X_with_A_minus_j(X: Union[sps.spmatrix, np.ndarray], A: np.ndarray, j: int) -> Union[sps.spmatrix, np.ndarray]:
    """Concatenate org features with co-adoption bits excluding target product j."""
    cols = np.arange(A.shape[1]) != j

    if sps.issparse(X):
        A_csr = sps.csr_matrix(A, dtype=np.float32)
        return sps.hstack([X, A_csr[:, cols]], format="csr")
    else:
        A_np = np.asarray(A, np.float32)
        return np.concatenate([np.asarray(X), A_np[:, cols]], axis=1)



def fit_per_item_isotonic(
    Y_val: np.ndarray,
    S_val: np.ndarray,
    min_pos: int = 10,
    min_neg: int = 10,
) -> Callable[[np.ndarray], np.ndarray]:
    """
    Fit **per-product isotonic regressions** to turn scores into calibrated probabilities.
    - Raw scores may not be on a probability scale and often differ **by product**.
      Isotonic provides a **monotone** mapping that preserves ranking but fixes calibration.
    - We require `min_pos`/`min_neg` to avoid fitting calibrators on degenerate splits.

    Inputs:
        Y_val: Validation labels with {1,0,NaN} where NaN indicates not in validation set.
        S_val: Validation **scores/probabilities** from the model.

    Returns:
        A callable `apply(scores)` that maps [n_orgs, n_products] â†' calibrated probabilities,
        using identity for items without sufficient validation signal.
    """
    P = Y_val.shape[1]
    items: List[_IsoItem] = []
    for j in range(P):
        # Only use samples that are actually in the validation set (not NaN)
        mask = ~np.isnan(Y_val[:, j])
        y = Y_val[mask, j]
        s = S_val[mask, j]
        if y.size == 0:
            items.append(_IsoItem(model=None)); continue
        n_pos = int(np.sum(y == 1.0))
        n_neg = int(np.sum(y == 0.0))
        if n_pos >= min_pos and n_neg >= min_neg:
            ir = IsotonicRegression(out_of_bounds="clip")
            ir.fit(s, y); items.append(_IsoItem(model=ir))
        else:
            items.append(_IsoItem(model=None))

    def _apply(scores: np.ndarray) -> np.ndarray:
        scores = np.asarray(scores, np.float32)
        out = np.zeros_like(scores, dtype=np.float32)
        for j in range(P):
            out[:, j] = items[j].model.predict(scores[:, j]) if items[j].model is not None else scores[:, j]
        return out
    return _apply


def preprocess_features(
    df: pd.DataFrame,
    org_features: Sequence[str],
    sparse_threshold: float = 0.3,
) -> Tuple[np.ndarray, ColumnTransformer]:
    """
    Fit a robust, mixed-type preprocessor and transform splits.

    How:
    - **StandardScaler** on numeric columns prevents features with large scales
      from dominating early training and improves optimizer stability.
    - **OneHotEncoder(handle_unknown="ignore")** avoids train/serve skew when new categories appear.
    - Using a single fitted `ColumnTransformer` preserves **column order and dtypes** across splits.
    - `sparse_threshold` lets us keep a sparse matrix if the one-hot dominates, but still allows
      dense outputs when numeric features are prevalentâ€”useful for GPU models.

    Returns:
        (X_train, fitted_preprocessor)
    """

    X = df[org_features].copy()

    num_cols = list(X.select_dtypes(include=[np.number, "bool"]).columns)
    cat_cols = [c for c in org_features if c not in num_cols]

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", MaxAbsScaler(), num_cols),
            ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=True), cat_cols),
        ],
        remainder="drop",
        sparse_threshold=sparse_threshold,
    )
    X_transformed = preprocessor.fit_transform(X)
    return X_transformed, preprocessor

def build_loao(
    Y_full: np.ndarray,
    n_pos_val: int = 1,
    n_neg_val: int = 5,
    n_pos_test: int = 1,
    min_adoptions: int = 2,
    seed: int = 123,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """
    Hybrid Leave-One-Adoption-Out split for calibration + ranking evaluation.

    Strategy:
      - VALIDATION: positives + explicit negatives (for calibration fitting)
      - TEST: positives only (pure ranking)
      - TRAIN: everything else (held-out cells removed from train labels)

    IMPORTANT:
      - Co-adoption A_* are built from the FULL snapshot (no global zeroing).
      - Prevent self-leakage by constructing A^{-j} at training/scoring time:
          For target product j, set column j to 0 in the A you feed the model.
      - Validation and Test positives are disjoint; held-out cells are -99 in Y_train.

    Args:
        Y_full: [N, P] with {1, 0, -99}
        n_pos_val: # positives held out per org for validation
        n_neg_val: # negatives held out per org for validation
        n_pos_test: # positives held out per org for test
        min_adoptions: minimum positives to consider an org
        seed: RNG seed

    Returns:
        Y_train, Y_val, Y_test, A_train, A_val, A_test
          - Y_val has explicit 1s and 0s (others NaN)
          - Y_test has only 1s (others NaN)
          - A_* are identical full-snapshot co-adoption matrices (no masking)
    """
    rng = np.random.default_rng(seed)
    N, P = Y_full.shape

    # Initialize label splits
    Y_train = Y_full.copy().astype(float)
    Y_val   = np.full_like(Y_full, np.nan, dtype=float)
    Y_test  = np.full_like(Y_full, np.nan, dtype=float)

    # Track held-out indices (for sanity checks only)
    hold_val_pos  = np.zeros((N, P), dtype=bool)
    hold_test_pos = np.zeros((N, P), dtype=bool)

    for i in range(N):
        eligible  = _is_eligible(Y_full[i])
        positives = np.where((Y_full[i] == 1) & eligible)[0]
        negatives = np.where((Y_full[i] == 0) & eligible)[0]

        n_pos_needed = n_pos_val + n_pos_test
        # Require enough positives to hold out and still satisfy min_adoptions,
        # and enough negatives to sample for validation.
        if (positives.size < max(min_adoptions, n_pos_needed)) or (negatives.size < n_neg_val):
            # Not enough signal to split; keep entire row in train
            continue

        rng.shuffle(positives)
        rng.shuffle(negatives)

        # === VALIDATION (explicit pos + neg) ===
        val_pos_idx = positives[:n_pos_val] if n_pos_val > 0 else np.array([], dtype=int)
        val_neg_idx = negatives[:n_neg_val] if n_neg_val > 0 else np.array([], dtype=int)

        if val_pos_idx.size:
            Y_val[i, val_pos_idx]   = 1.0
            Y_train[i, val_pos_idx] = np.nan
            hold_val_pos[i, val_pos_idx] = True

        if val_neg_idx.size:
            Y_val[i, val_neg_idx]   = 0.0
            Y_train[i, val_neg_idx] = np.nan  # hide sampled negatives from training

        # === TEST (positives only from remaining) ===
        remaining_pos = positives[n_pos_val:]
        if remaining_pos.size >= n_pos_test and n_pos_test > 0:
            test_pos_idx = remaining_pos[:n_pos_test]
            Y_test[i, test_pos_idx]  = 1.0
            Y_train[i, test_pos_idx] = np.nan
            hold_test_pos[i, test_pos_idx] = True

    # Sanity: no overlap of val/test positives
    if np.any((Y_val == 1) & (Y_test == 1)):
        raise RuntimeError("Validation and Test positives overlap; check sampling logic.")

    # --- Co-adoption matrices from FULL SNAPSHOT (no zeroing) ---
    A_base = _sentinel_to_num(Y_full, fill_value=0.0).astype(np.float32)
    A_train = A_base.copy()
    A_val   = A_base.copy()
    A_test  = A_base.copy()
    # Zero out held-out positives to prevent leakage
    A_val[Y_val == 1.0] = 0.0
    A_test[Y_test == 1.0] = 0.0

    return Y_train, Y_val, Y_test, A_train, A_val, A_test



def fit_random_forests(X_train, Y_train, A_train) -> List[RFModel]:
    """
    Train **one Random Forest per product** on [X || A^{-j}].
    - Using `class_weight="balanced"` and `max_samples=0.7` to handle imbalance and improve generalization.
    - We skip fitting when a product lacks label diversity and fall back to **constant prevalence**.

    Ignores:
    - -99 sentinel labels (ineligibility) and NaN values (held-out for validation/test) are excluded from fitting.

    Returns:
        List[RFModel] of length P.
    """

    P = Y_train.shape[1]
    models: List[RFModel] = []
    for j in range(P):
        # Exclude both ineligible (-99) and held-out (NaN) values
        mask = _is_eligible(Y_train[:, j]) & ~np.isnan(Y_train[:, j])
        y = Y_train[mask, j].astype(int)
        if y.size == 0 or np.unique(y).size < 2:
            # Not enough signal → back off to prevalence (or 0.5 if empty)
            p_prev = float(np.mean(y)) if y.size > 0 else 0.5
            models.append(RFModel(estimator=None, const_prob=p_prev, j=j))
            continue
        X_aug = _augment_X_with_A_minus_j(X_train[mask], A_train[mask], j)
        # Convert sparse to dense to improve training time
        if sps.issparse(X_aug):
            X_aug = X_aug.toarray()

        est = RandomForestClassifier(
            n_estimators=600,
            max_depth=20,             # prevents overfitting
            min_samples_split=10,     # require substantial samples for splits
            min_samples_leaf=5,       # ensure leaves have enough samples
            max_features="sqrt",
            max_samples=0.7,          # bootstrap with 70% of data
            class_weight="balanced_subsample",  # handle imbalance per-product
            n_jobs=-1,
            random_state=123,
        )
        est.fit(X_aug, y)
        models.append(RFModel(estimator=est, const_prob=0.0, j=j))
    return models


def predict_random_forest(models: List[RFModel], X: Union[sps.spmatrix, np.ndarray], A_bits: np.ndarray) -> np.ndarray:
    """
    Predict per-product probabilities with the random forest ensemble.
    - We preserve the **per-product conditioning** by re-building [X || A^{-j}] for each j.
    - Fallback models return a constant probability when data was insufficient to fit.

    Returns:
        [n_orgs, n_products] probability matrix.
    """
    N = X.shape[0]; P = len(models)
    out = np.zeros((N, P), dtype=np.float32)
    for j, m in enumerate(models):
        X_aug = _augment_X_with_A_minus_j(X, A_bits, j)
        if sps.issparse(X_aug):
            X_aug = X_aug.toarray()
        if m.estimator is None:
            out[:, j] = m.const_prob
        else:
            out[:, j] = m.estimator.predict_proba(X_aug)[:, 1].astype(np.float32)
    return out
