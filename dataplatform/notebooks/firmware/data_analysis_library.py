# Copied modified version of data analysis library from Data Science & Services

# COMMAND ----------

# MAGIC %run ./bocpd

# COMMAND ----------

import copy
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import ruptures as rpt
from sklearn.ensemble import IsolationForest
from csaps import csaps
from functools import partial


def data_cleaning(
    data, time_column, value_column, data_type="continuous", frequency="d"
):
    """
    Input: panda.DataFrame which at least has two columns, a column of values of the metric, and a column of timestamps
    Task: make sure equal distance/time points
    Output: 1-D array of cleaned time series
    """
    DATA = copy.deepcopy(data)
    if data_type == "continuous":
        DATA[time_column] = pd.to_datetime(DATA[time_column])
        DOW = DATA[time_column].dt.day_name()[0]
        DATA = DATA.set_index(time_column)
        DATA = DATA.sort_index()
        DATA = DATA[value_column]

        if frequency in ["W", "w", "week", "Week"]:
            DATA = DATA.asfreq("W-" + DOW[:3])
        elif frequency in ["M", "m", "month", "Month"]:
            DATA = DATA.asfreq("MS")
        elif frequency in ["D", "d", "day", "Day"]:
            DATA = DATA.asfreq(frequency)
        else:
            raise Exception(
                "Cannot recognize the specified frequency, acceptable frequency 'week', 'month', 'day'"
            )

        # Drop rows with missing data
        DATA = DATA.dropna()
        DATA2 = np.array(DATA)

    else:
        # for discrete data, the input DATA has k+1 columns, one timestamp column and k columns for the k levels
        DATA[time_column] = pd.to_datetime(DATA[time_column])
        DOW = DATA[time_column].dt.day_name()[0]
        DATA = DATA.set_index(time_column)
        DATA = DATA[value_column]  # k columns for the k levels of the variable

        if frequency in ["W", "w", "week", "Week"]:
            DATA = DATA.asfreq("W-" + DOW[:3])
        elif frequency in ["M", "m", "month", "Month"]:
            DATA = DATA.asfreq("MS")
        elif frequency in ["D", "d", "day", "Day"]:
            DATA = DATA.asfreq(frequency)
        else:
            raise Exception(
                "Cannot recognize the specified frequency, acceptable frequency 'week', 'month', 'day'"
            )

        # Drop rows with missing data
        DATA = DATA.dropna()
        DATA2 = np.array(DATA)

    return DATA2, DATA


# COMMAND ----------


def hyperparameters(task_method, **kwargs):
    # season
    if task_method == "MSTL":
        periods = kwargs.get(
            "periods", [7]
        )  # can specify multiple periods using tuple (7,30) etc.
        windows = kwargs.get("windows", list(np.repeat(61, len(periods))))
        lmbda = kwargs.get("lmbda", "auto")
        lmbda_range = kwargs.get("lmbda_range", np.arange(-3, 3, 0.1))
        hps = {
            "periods": periods,
            "windows": windows,
            "lmbda": lmbda,
            "lmbda_range": lmbda_range,
        }

    # noise
    elif task_method == "spline_isoForest":
        forest_n_estimators = kwargs.get("forest_n_estimators", 100)
        forest_max_samples = kwargs.get("forest_max_samples", "auto")
        forest_contamination = kwargs.get("forest_contamination", 0.05)
        forest_bootstrap = kwargs.get("forest_bootstrap", False)
        random_state = kwargs.get("random_state", 123)
        hps = {
            "forest_n_estimators": forest_n_estimators,
            "forest_max_samples": forest_max_samples,
            "forest_contamination": forest_contamination,
            "forest_bootstrap": forest_bootstrap,
            "random_state": random_state,
        }

    # trend
    elif task_method == "PELT":
        res_data = kwargs.get("data", 0)
        model = kwargs.get("model", "clinear")
        min_size = kwargs.get("min_size", 1)
        jump = kwargs.get("jump", 1)
        hps = {"model": model, "min_size": min_size, "jump": jump}
        if model == "clinear":
            penalty = kwargs.get("penalty", 10**8)
            fit_data = kwargs.get("fit_data", res_data.reshape(-1, 1))
            hps["penalty"] = penalty
            hps["fit_data"] = fit_data
        elif model == "l2":
            penalty = kwargs.get("penalty", 10)
            fit_data = kwargs.get("fit_data", res_data)
            hps["penalty"] = penalty
            hps["fit_data"] = fit_data
        elif model == "normal":
            penalty = kwargs.get("penalty", 30)
            fit_data = kwargs.get("fit_data", res_data)
            hps["penalty"] = penalty
            hps["fit_data"] = fit_data
        elif model == "ar":
            penalty = kwargs.get("penalty", 1)
            ar_order = kwargs.get("ar_order", 3)
            fit_data = kwargs.get("fit_data", res_data)
            hps["penalty"] = penalty
            hps["fit_data"] = fit_data
            hps["ar_order"] = ar_order
        elif model == "linear":
            penalty = kwargs.get("penalty", 10**6)
            fit_data = kwargs.get(
                "fit_data",
                np.column_stack(
                    (
                        res_data.reshape(-1, 1),
                        np.vstack(
                            (np.arange(0, len(res_data)), np.ones(len(res_data)))
                        ).T,
                    )
                ),
            )
            hps["penalty"] = penalty
            hps["fit_data"] = fit_data
    elif task_method == "Dynp":
        res_data = kwargs.get("data", 0)
        model = kwargs.get("model", "clinear")
        min_size = kwargs.get("min_size", 1)
        jump = kwargs.get("jump", 1)
        n_bkps = kwargs.get("n_bkps", 4)
        hps = {"model": model, "min_size": min_size, "jump": jump, "n_bkps": n_bkps}
        if model == "clinear":
            fit_data = kwargs.get("fit_data", res_data.reshape(-1, 1))
        elif model == "l2":
            fit_data = kwargs.get("fit_data", res_data)
        elif model == "normal":
            fit_data = kwargs.get("fit_data", res_data)
        elif model == "ar":
            ar_order = kwargs.get("ar_order", 3)
            fit_data = kwargs.get("fit_data", res_data)
            hps["ar_order"] = ar_order
        elif model == "linear":
            fit_data = kwargs.get(
                "fit_data",
                np.column_stack(
                    (
                        res_data.reshape(-1, 1),
                        np.vstack(
                            (np.arange(0, len(res_data)), np.ones(len(res_data)))
                        ).T,
                    )
                ),
            )
        hps["fit_data"] = fit_data
    elif task_method == "BOCPD":
        const_hazard = kwargs.get("const_hazard", 100)
        ORDER = kwargs.get("ORDER", 1)
        MuInit = kwargs.get("MuInit", [0 for _ in range(ORDER + 1)])
        LambdaInit = kwargs.get("LambdaInit", 1e-2)
        AlphaInit = kwargs.get("AlphaInit", 0.1)
        BetaInit = kwargs.get("BetaInit", 0.1)
        hps = {
            "const_hazard": const_hazard,
            "ORDER": ORDER,
            "MuInit": MuInit,
            "LambdaInit": LambdaInit,
            "AlphaInit": AlphaInit,
            "BetaInit": BetaInit,
        }
    # raise warning if some args not used

    # sanity check for each method
    return hps


# COMMAND ----------


def detect(
    data,
    time_column="dfweek",
    value_column="value",
    frequency="W",
    data_type="continuous",
    task={"season": "MSTL", "outlier": "spline_isoForest", "trend": "Dynp"},
    zoom_in_point=None,
    **kwargs
):
    # ----------- #
    # input check #
    # ----------- #
    assert isinstance(
        task, dict
    ), "Specify task as a dictionary, key:value specifies task:task_method. for example, task={'season':'MSTL','outlier':'spline_isoForest','trend':'PELT'} for seasonal+outlier+trend detection. MSTL, spline_isoForest, PELT are used for each task."

    assert all(
        item in ["season", "outlier", "trend"] for item in list(task.keys())
    ), "Supported task: 'season', 'outlier', 'trend'"

    for KEYS in list(task.keys()):
        if KEYS == "season":
            assert task[KEYS] in ["MSTL"], "Supported task: 'MSTL'"
        elif KEYS == "outlier":
            assert task[KEYS] in [
                "spline_isoForest"
            ], "Supported task: 'spline_isoForest'"
        elif KEYS == "trend":
            assert task[KEYS] in [
                "PELT",
                "Dynp",
                "BOCPD",
            ], "Supported task: 'PELT', 'Dynp', 'BOCPD'"

    assert data_type in [
        "continuous",
        "discrete",
    ], "Supported task: 'continuous', 'discrete'"

    assert isinstance(data, pd.DataFrame), "data should be a pandas dataframe"

    detected_lag = None
    season_image = None

    # ------------- #
    # data cleaning #
    # ------------- #
    # for continuous variable, value column is a single string containing the name for all levels
    # for categorical variable, value column is a list of string containing the name for all levels
    data, data_with_date = data_cleaning(
        data=data,
        time_column=time_column,
        value_column=value_column,
        frequency=frequency,
        data_type=data_type,
    )

    # ----------------------------------- #
    # short term analysis - truncate data #
    # ----------------------------------- #
    if zoom_in_point:
        zoom_in_point = data_with_date.index.get_loc(zoom_in_point)
    zoom_in_before = kwargs.get("zoom_in_before", 60)
    zoom_in_after = kwargs.get("zoom_in_after", 11)
    if zoom_in_point:  # zoom_in_point start from 0
        if zoom_in_point < zoom_in_before:
            if (zoom_in_point + zoom_in_after) < len(data):
                data = data[: (zoom_in_point + zoom_in_after)]
                data_with_date = data_with_date[: (zoom_in_point + zoom_in_after)]
            else:
                data = data[:]
                data_with_date = data_with_date[:]
            anchor_point = zoom_in_point
        else:
            if (zoom_in_point + zoom_in_after) < len(data):
                data = data[
                    (zoom_in_point - zoom_in_before) : (zoom_in_point + zoom_in_after)
                ]
                data_with_date = data_with_date[
                    (zoom_in_point - zoom_in_before) : (zoom_in_point + zoom_in_after)
                ]
            else:
                data = data[(zoom_in_point - zoom_in_before) :]
                data_with_date = data_with_date[(zoom_in_point - zoom_in_before) :]
            anchor_point = zoom_in_before
    else:
        anchor_point = None

    # --------------- #
    # continuous case #
    # --------------- #
    (
        season,
        trend,
        resid,
        deseason,
        outlier_x,
        outlier_y,
        impute_data,
        change_x,
        change_y,
    ) = ([], [], [], [], [], [], [], [], [])
    INPUT_TASKS = list(task.keys())
    PARAMETERS = {}

    if data_type == "continuous":
        ### algorithm
        if "season" in INPUT_TASKS:
            PARAMETERS[task["season"]] = hyperparameters(
                task_method=task["season"], **kwargs
            )
            season, trend, resid, deseason, season_image = detect_season(
                data,
                data_with_date,
                PARAMETERS[task["season"]],
                method=task["season"],
                **kwargs
            )
        if "outlier" in INPUT_TASKS:
            PARAMETERS[task["outlier"]] = hyperparameters(
                task_method=task["outlier"], **kwargs
            )
            if "season" in INPUT_TASKS:
                outlier_x, outlier_y, impute_data = detect_outlier(
                    resid, PARAMETERS[task["outlier"]], method="isoForest", **kwargs
                )
            else:
                outlier_x, outlier_y, impute_data = detect_outlier(
                    data, PARAMETERS[task["outlier"]], method=task["outlier"], **kwargs
                )
        if "trend" in INPUT_TASKS:
            if ("season" in INPUT_TASKS) and ("outlier" in INPUT_TASKS):
                # need to change the prior to be the trend from the season module, not the original data
                order_in = kwargs.get("ORDER", 1)
                kwargs["MuInit"] = kwargs.get("MuInit", [np.average(trend)])
                if len(kwargs["MuInit"]) == 1:  # else: user specified, skip
                    kwargs["MuInit"].extend([0 for _ in range(order_in)])
                PARAMETERS[task["trend"]] = hyperparameters(
                    task_method=task["trend"], data=trend, **kwargs
                )
                change_x, change_y, detected_lag, trend = detect_trend(
                    trend,
                    PARAMETERS[task["trend"]],
                    data_type=data_type,
                    method=task["trend"],
                    anchor_point=anchor_point,
                    data_with_date=data_with_date,
                    **kwargs
                )
            elif ("season" in INPUT_TASKS) and ("outlier" not in INPUT_TASKS):
                # need to change the prior to be the trend from the season module, not the original data
                order_in = kwargs.get("ORDER", 1)
                kwargs["MuInit"] = kwargs.get("MuInit", [np.average(trend)])
                if len(kwargs["MuInit"]) == 1:  # else: user specified, skip
                    kwargs["MuInit"].extend([0 for _ in range(order_in)])
                PARAMETERS[task["trend"]] = hyperparameters(
                    task_method=task["trend"], data=trend, **kwargs
                )
                change_x, change_y, detected_lag, trend = detect_trend(
                    trend,
                    PARAMETERS[task["trend"]],
                    data_type=data_type,
                    method=task["trend"],
                    data_with_date=data_with_date,
                    anchor_point=anchor_point,
                    **kwargs
                )
            elif ("season" not in INPUT_TASKS) and ("outlier" in INPUT_TASKS):
                # need to change the prior to be the imputed data, not the original data
                order_in = kwargs.get("ORDER", 1)
                kwargs["MuInit"] = kwargs.get("MuInit", [np.average(impute_data)])
                if len(kwargs["MuInit"]) == 1:  # else: user specified, skip
                    kwargs["MuInit"].extend([0 for _ in range(order_in)])
                PARAMETERS[task["trend"]] = hyperparameters(
                    task_method=task["trend"], data=impute_data, **kwargs
                )
                change_x, change_y, detected_lag, trend = detect_trend(
                    impute_data,
                    PARAMETERS[task["trend"]],
                    data_type=data_type,
                    method=task["trend"],
                    data_with_date=data_with_date,
                    anchor_point=anchor_point,
                    **kwargs
                )
            else:
                # need to change the prior to be the cleaned data, not the original data
                order_in = kwargs.get("ORDER", 1)
                kwargs["MuInit"] = kwargs.get("MuInit", [np.average(data)])
                if len(kwargs["MuInit"]) == 1:  # else: user specified, skip
                    kwargs["MuInit"].extend([0 for _ in range(order_in)])
                PARAMETERS[task["trend"]] = hyperparameters(
                    task_method=task["trend"], data=data, **kwargs
                )
                change_x, change_y, detected_lag, trend = detect_trend(
                    data,
                    PARAMETERS[task["trend"]],
                    data_type=data_type,
                    method=task["trend"],
                    data_with_date=data_with_date,
                    anchor_point=anchor_point,
                    **kwargs
                )

        ### plot
        figure = kwargs.get("figure", True)
        figsize = kwargs.get("figsize", [18, 6])

        if figure:
            if "trend" in INPUT_TASKS and (task["trend"] != "BOCPD"):
                if data_type == "continuous":
                    y = copy.deepcopy(data)
                    y = y.reshape(
                        -1,
                    )
                    x = np.arange(len(data))

                    fig, ax = plt.subplots(figsize=figsize)
                    ax.plot(x, y)
                    if "outlier" in INPUT_TASKS:
                        ax.scatter(
                            outlier_x, y[outlier_x], color="red", label="outlier points"
                        )
                    if "trend" in INPUT_TASKS:
                        ax.scatter(
                            change_x,
                            y[change_x],
                            color="blue",
                            label="trend segmentation point",
                        )
                    ax.set_title("Original Data and Change Points")
                    ax.legend()

    # ------------- #
    # discrete case #
    # ------------- #
    elif data_type == "discrete":

        ### algorithm
        # for each one of the column of the discrete variable, we pass it into season, outlier and trend module
        for COL in range(data.shape[1]):
            print("========================================================")
            print("Process the " + str(COL) + "-th level of the categorical variable")
            print("========================================================")
            data_C = data[:, COL]
            if "season" in INPUT_TASKS:
                PARAMETERS[task["season"]] = hyperparameters(
                    task_method=task["season"], **kwargs
                )
                season, trend, resid, deseason, season_image = detect_season(
                    data_C,
                    data_with_date,
                    PARAMETERS[task["season"]],
                    method=task["season"],
                    COL=COL + 1,
                    **kwargs
                )
                # replace each column with the output from season module
                data[:, COL] = trend
            if "outlier" in INPUT_TASKS:
                PARAMETERS[task["outlier"]] = hyperparameters(
                    task_method=task["outlier"], **kwargs
                )
                if "season" in INPUT_TASKS:
                    outlier_x, outlier_y, impute_data = detect_outlier(
                        resid,
                        PARAMETERS[task["outlier"]],
                        method=task["outlier"],
                        **kwargs
                    )
                    # each column should be replaced by the output trend from season module, already updated, no replacement here
                else:
                    outlier_x, outlier_y, impute_data = detect_outlier(
                        data_C,
                        PARAMETERS[task["outlier"]],
                        method=task["outlier"],
                        **kwargs
                    )
                    # replace each column with the output from outlier module
                    data[:, COL] = impute_data

        print("========================================================")
        print("Finished Processing all the levels")
        print("========================================================")
        # passed the processed data into trend module at once, not column by column
        # the data is not replaced by the processed data from season and outlier modules
        if "trend" in INPUT_TASKS:
            PARAMETERS[task["trend"]] = hyperparameters(
                task_method=task["trend"], **kwargs
            )
            change_x, change_y, detected_lag, trend = detect_trend(
                data.round(),
                PARAMETERS[task["trend"]],
                data_type=data_type,
                method=task["trend"],
                **kwargs
            )

        ### plot
        figure = kwargs.get("figure", True)
        figsize = kwargs.get("figsize", [18, 6])

        if figure:
            if "trend" in INPUT_TASKS and (task["trend"] != "BOCPD"):
                if data_type == "continuous":
                    y = copy.deepcopy(data)
                    y = y.reshape(
                        -1,
                    )
                    x = np.arange(len(data))

                    fig, ax = plt.subplots(figsize=figsize)
                    ax.plot(x, y)
                    if "outlier" in INPUT_TASKS:
                        ax.scatter(
                            outlier_x, y[outlier_x], color="red", label="outlier points"
                        )
                    if "trend" in INPUT_TASKS:
                        ax.scatter(
                            change_x,
                            y[change_x],
                            color="blue",
                            label="trend segmentation point",
                        )
                    ax.set_title("Original Data and Change Points")
                    ax.legend()

    # -------------- #
    # return results #
    # -------------- #
    print("all tasks finished")
    if season_image is not None:
        return (
            season,
            trend,
            resid,
            deseason,
            outlier_x,
            outlier_y,
            impute_data,
            change_x,
            change_y,
            detected_lag,
            data_with_date,
            season_image,
        )
    return (
        season,
        trend,
        resid,
        deseason,
        outlier_x,
        outlier_y,
        impute_data,
        change_x,
        change_y,
        detected_lag,
        data_with_date,
    )


# COMMAND ----------

# def detect_season_boxcox(data,PARAMETERS,method='MSTL',data_type='continuous',**kwargs):
#   # keep this function but not used
#   print("... removing seasonal effect ...")
#   DATA = copy.deepcopy(data)

#   periods=PARAMETERS['periods'] # can specify multiple periods using tuple (7,30) etc.
#   windows=PARAMETERS['windows']
#   lmbda=PARAMETERS['lmbda']
#   lmbda_range=PARAMETERS['lmbda_range']

#   res = MSTL(DATA,periods=periods,windows=windows,lmbda=lmbda).fit()
#   lm,lm_p,f,f_p = ssd.het_breuschpagan(res.resid, np.vstack((np.ones(len(res.resid)),np.arange(len(res.resid)))).T, robust=True)

#   if f_p < 0.05: # still heteroskedesticity
#     print(".... GridSearch BoxCox ....")
#     Ls = []
#     f_ps = []

#     for L in lmbda_range:
#       res = MSTL(DATA,periods=periods,windows=windows,lmbda=L).fit()
#       lm,lm_p,f,f_p = ssd.het_breuschpagan(res.resid, np.vstack((np.ones(len(res.resid)),np.arange(len(res.resid)))).T, robust=True)
#       Ls.append(L)
#       f_ps.append(f_p)

#     # saved each L and f_p
#     # choose L that has the largest f_p
#     L_refit = Ls[np.argmax(f_ps)]

#     # refit using that L
#     res = MSTL(DATA,periods=periods,windows=windows,lmbda=L_refit).fit()
#     lm,lm_p,f,f_p = ssd.het_breuschpagan(res.resid, np.vstack((np.ones(len(res.resid)),np.arange(len(res.resid)))).T, robust=True)
#     print('.... final lambda: ',L_refit," final p-value for Breusch-Pagan test: ",f_p," ....")

#     if f_p < 0.05:
#       warnings.warn("BoxCox failed, still have heteroskedasticity")

#   figure = kwargs.get('figure',True)
#   figsize = kwargs.get('figsize',[18,6])

#   # plots
#   if figure:
#     fig, ax = plt.subplots(4,figsize=figsize)
#     ax[0].plot(res.observed)
#     ax[0].set_ylabel('Original Data')
#     ax[1].plot(res.seasonal)
#     ax[1].set_ylabel('Seasonality')
#     ax[2].plot(res.trend)
#     ax[2].set_ylabel('Trend')
#     ax[3].plot(res.resid)
#     ax[3].set_ylabel('Residual')

#   return res.seasonal,res.trend,res.resid,res.observed-res.seasonal

# COMMAND ----------


def detect_season(
    data,
    data_with_date,
    PARAMETERS,
    method="MSTL",
    data_type="continuous",
    COL=None,
    **kwargs
):

    print("... removing seasonal effect ...")
    DATA = copy.deepcopy(data)

    periods_unfiltered = PARAMETERS[
        "periods"
    ]  # can specify multiple periods using tuple (7,30) etc.
    window_unfiltered = PARAMETERS["windows"]

    periods = []
    windows = []

    for ind in range(len(periods_unfiltered)):
        if periods_unfiltered[ind] > 0.5 * len(data):
            print(
                "Period is longer than the data, the seasonal effect for "
                + str(periods_unfiltered[ind])
                + " will be ignored"
            )
        else:
            periods.append(periods_unfiltered[ind])
            windows.append(window_unfiltered[ind])

    N_Periods = len(periods)

    res = MSTL(DATA, periods=periods, windows=windows).fit()

    figure = kwargs.get("figure", True)
    figsize = kwargs.get("figsize", [18, 6])

    # print(res.seasonal.shape)
    # print(np.sum(res.seasonal,axis=1))

    # plots
    fig, ax = plt.subplots(4, figsize=figsize)
    ax[0].plot(res.observed)
    ax[0].set_ylabel("Original Data")
    if COL:
        ax[0].set_title(str(COL - 1) + "-th level of the categorical variable")
    if N_Periods > 1:
        for col in range(res.seasonal.shape[1]):
            ax[1].plot(res.seasonal[:, col], label="period: " + str(periods[col]))
        ax[1].legend()
    else:
        ax[1].plot(res.seasonal)
    ax[1].set_ylabel("Seasonality")
    ax[2].plot(res.trend)
    ax[2].set_ylabel("Trend")
    ax[3].plot(res.resid)
    ax[3].set_ylabel("Residual")

    date_labels = data_with_date.index.strftime("%Y-%m-%d").tolist()
    step = max(1, len(date_labels) // 12)
    selected_indices = range(0, len(date_labels), step)
    print(selected_indices)
    selected_labels = [date_labels[i] for i in selected_indices]

    ax[3].set_xticks(selected_indices)
    ax[3].set_xticklabels(selected_labels, rotation=45)

    # Save plot to BytesIO object
    from io import BytesIO

    image_stream = BytesIO()
    fig.savefig(image_stream, format="png")
    image_stream.seek(0)  # Rewind the stream to start

    if not figure:
        plt.close(fig)

    if N_Periods > 1:
        return (
            res.seasonal,
            res.trend,
            res.resid,
            res.observed - np.sum(res.seasonal, axis=1),
            image_stream,
        )
    else:
        return (
            res.seasonal,
            res.trend,
            res.resid,
            res.observed - res.seasonal,
            image_stream,
        )


# COMMAND ----------


def detect_outlier(data, PARAMETERS, method="spline_isoForest", **kwargs):
    print("... detecting outliers ...")

    figsize = kwargs.get("figsize", [18, 6])
    figure = kwargs.get("figure", True)

    if (method == "spline_isoForest") or (method == "isoForest"):
        forest_n_estimators = PARAMETERS["forest_n_estimators"]
        forest_max_samples = PARAMETERS["forest_max_samples"]
        forest_contamination = PARAMETERS["forest_contamination"]
        forest_bootstrap = PARAMETERS["forest_bootstrap"]
        random_state = PARAMETERS["random_state"]

        y = copy.deepcopy(data)
        y = y.reshape(
            -1,
        )
        x = np.arange(len(data))

        if method == "spline_isoForest":
            ys = csaps(x, y, x)
            fitted = ys[0]
            residuals = y - fitted
            # isolation forest on residuals
            data1 = np.array([residuals]).reshape(-1, 1)
        elif method == "isoForest":
            data1 = y.reshape(-1, 1)

        iforest = IsolationForest(
            n_estimators=forest_n_estimators,
            max_samples=forest_max_samples,
            contamination=forest_contamination,
            bootstrap=forest_bootstrap,
            random_state=random_state,
        )
        iforest_pred = iforest.fit_predict(data1)
        outlier_x = np.where(iforest_pred < 0)[0]
        outlier_y = data1[outlier_x].reshape(
            -1,
        )

    # plots
    if figure:
        fig, ax = plt.subplots(figsize=figsize)
        ax.plot(x, y)  # original data
        ax.scatter(
            outlier_x, y[outlier_x], color="red"
        )  # labeled outliers on original data

        if method == "spline_isoForest":
            ax.plot(x, fitted)  # spline fitted data
            ax.set_title("Data, Smoothed Fit and Outliers")
            fig, ax = plt.subplots(figsize=figsize)
            ax.plot(data1)  # residual after spline fit
            ax.scatter(
                outlier_x, outlier_y, color="red"
            )  # labeled outliers on residual
            ax.set_title("Smoothed Fit Residuals and Outliers")
        elif method == "isoForest":
            ax.set_title("Input Residuals and Outliers")

    # replace outlier points with average around it
    for tempx in outlier_x:
        replace1 = tempx - 1
        replace2 = tempx + 1
        while replace1 in outlier_x:
            replace1 -= 1
        while replace2 in outlier_x:
            replace2 += 1

        if ((replace1 < 0) or (replace1 >= len(y))) and (
            (replace2 < 0) or (replace2 >= len(y))
        ):
            y[tempx] = np.mean(y)
        elif (replace1 < 0) or (
            replace1 >= len(y)
        ):  # it is the first point or the last point, do not change
            y[tempx] = y[tempx]
        elif (replace2 < 0) or (
            replace2 >= len(y)
        ):  # it is the first point or the last point, do not change
            y[tempx] = y[tempx]
        else:
            y[tempx] = np.mean(y[[replace1, replace2]])

    return outlier_x, outlier_y, y


# COMMAND ----------


def detect_trend(
    data,
    PARAMETERS,
    method="Dynp",
    data_type="continuous",
    anchor_point=None,
    data_with_date=0,
    **kwargs
):

    print("... detecting trend segmentations ...")
    res_data = copy.deepcopy(data)

    figure = kwargs.get("figure", True)
    figsize = kwargs.get("figsize", [18, 6])
    n_bt_c = kwargs.get("n_bt", 10000)
    n_bt_d = kwargs.get("n_bt", 1000)
    significance_level = kwargs.get("significance_level", 0.05)

    detected_lag = None
    plot_x = 0

    if data_type == "continuous":
        if method == "PELT":
            model = PARAMETERS["model"]
            min_size = PARAMETERS["min_size"]
            jump = PARAMETERS["jump"]
            penalty = PARAMETERS["penalty"]
            fit_data = PARAMETERS["fit_data"]

            if model != "ar":
                algo = rpt.detection.Pelt(
                    model=model, min_size=min_size, jump=jump
                ).fit(fit_data)
            else:
                ar_order = PARAMETERS["ar_order"]
                algo = rpt.detection.Pelt(
                    model=model,
                    min_size=min_size,
                    jump=jump,
                    params={"order": ar_order},
                ).fit(fit_data)

            my_bkps = np.array(algo.predict(pen=penalty))
            my_bkps = my_bkps[:-1]  # the last one is always the last obs. remove

            # plot
            if figure:
                fig, ax = plt.subplots(figsize=figsize)
                ax.plot(res_data)
                ax.scatter(my_bkps, res_data[my_bkps], color="red")
                ax.set_title("Trend Change Points")

            change_x = my_bkps
            change_y = res_data[my_bkps]

        elif method == "Dynp":
            model = PARAMETERS["model"]
            min_size = PARAMETERS["min_size"]
            jump = PARAMETERS["jump"]
            n_bkps = PARAMETERS["n_bkps"]
            fit_data = PARAMETERS["fit_data"]

            if model != "ar":
                algo = rpt.detection.Dynp(
                    model=model, min_size=min_size, jump=jump
                ).fit(fit_data)
            else:
                algo = rpt.detection.Dynp(
                    model=model,
                    min_size=min_size,
                    jump=jump,
                    params={"order": ar_order},
                ).fit(fit_data)

            my_bkps = np.array(algo.predict(n_bkps=n_bkps))
            my_bkps = my_bkps[:-1]  # the last one is always the last obs. remove

            # plot
            if figure:
                fig, ax = plt.subplots(figsize=figsize)
                ax.plot(res_data)
                ax.scatter(my_bkps, res_data[my_bkps], color="red")
                ax.set_title("Trend and Trend Change Points")

                # fig, ax_arr = rpt.display(fit_data, [], my_bkps, figsize=(10, 6))
                # plt.show()

            change_x = my_bkps
            change_y = res_data[my_bkps]

        elif method == "BOCPD":

            const_hazard = PARAMETERS["const_hazard"]
            ORDER = PARAMETERS["ORDER"]
            MuInit = PARAMETERS["MuInit"]
            LambdaInit = PARAMETERS["LambdaInit"]
            AlphaInit = PARAMETERS["AlphaInit"]
            BetaInit = PARAMETERS["BetaInit"]

            res_data = res_data.reshape((-1, 1))
            hazard_function = partial(constant_hazard, const_hazard)
            (
                R,
                maxes,
                MAP,
                P_M,
                P_V,
                CI_LOC,
                CI_SCALE,
                CI_DF,
                btCIL,
                btCIM,
                btCIU,
            ) = online_changepoint_detection2(
                data=res_data,
                hazard_function=hazard_function,
                log_likelihood_class=Bayesian_Regression(
                    order=ORDER,
                    alpha=AlphaInit,
                    beta=BetaInit,
                    LambdaInit=LambdaInit,
                    MuInit=MuInit,
                ),
                n_bt=n_bt_c,
                significance_level=significance_level,
            )

            all_detected, detected_lag, ontime_detected, seg_pair = BOCPD_result(maxes)

            change_x = np.array(all_detected).astype(int)
            change_y = res_data[change_x]
            # plot
            if figure:
                fig, ax = plt.subplots(figsize=figsize)
                plot_x = np.arange(0, len(res_data))
                if n_bt_c > 0:
                    ax.set_ylim(
                        np.min([np.min(res_data), np.quantile(btCIL, 0.1)]),
                        np.max([np.quantile(btCIU, 0.9), np.max(res_data)]),
                    )
                else:
                    ax.set_ylim(np.min(res_data), np.max(res_data))
                ax.scatter(plot_x, res_data, c="k")
                ax.plot(plot_x, res_data, c="k")

                # for keys, pairs in seg_pair.items():
                #   pairs_x = pairs
                #   pairs_y = res_data[pairs_x]
                #   ax.scatter(pairs_x,pairs_y)

                # post. pred. CI

                # P_V = np.where(P_V>0,P_V,0)
                # ax.plot(plot_x, P_M, c='k',label='theoretical prediction mean')
                if n_bt_c > 0:
                    ax.plot(
                        plot_x,
                        btCIM,
                        c="b",
                        alpha=0.4,
                        label="bootstrap prediction median",
                    )
                    ax.plot(plot_x[1:], btCIL[1:], c="pink", alpha=0.4, ls="--")
                    ax.plot(plot_x[1:], btCIU[1:], c="pink", alpha=0.4, ls="--")
                    ax.fill_between(
                        plot_x[1:],
                        btCIL[1:],
                        btCIU[1:],
                        color="pink",
                        alpha=0.3,
                        label=str(round((1 - significance_level) * 100))
                        + "% bootstrap prediction CI",
                    )

                LL = 0
                for CX in change_x:
                    if CX == 0:
                        continue
                    else:
                        if LL == 0:
                            if n_bt_c > 0:
                                ax.vlines(
                                    CX,
                                    np.min(btCIL[1:]),
                                    np.max(btCIU[1:]),
                                    color="k",
                                    alpha=0.4,
                                    ls="--",
                                    label="breakpoints (starting a new segment)",
                                )
                            else:
                                ax.vlines(
                                    CX,
                                    np.min(res_data),
                                    np.max(res_data),
                                    color="k",
                                    alpha=0.4,
                                    ls="--",
                                    label="breakpoints (starting a new segment)",
                                )
                            ax.scatter(
                                CX,
                                res_data[CX],
                                c="red",
                                label="breakpoints (starting a new segment)",
                            )
                            LL += 1
                        else:
                            if n_bt_c > 0:
                                ax.vlines(
                                    CX,
                                    np.min(btCIL[1:]),
                                    np.max(btCIU[1:]),
                                    color="k",
                                    alpha=0.4,
                                    ls="--",
                                )
                            else:
                                ax.vlines(
                                    CX,
                                    np.min(res_data),
                                    np.max(res_data),
                                    color="k",
                                    alpha=0.4,
                                    ls="--",
                                )
                            ax.scatter(CX, res_data[CX], c="red")

                if anchor_point:
                    ax.vlines(
                        anchor_point,
                        np.min(res_data),
                        np.max(res_data),
                        color="red",
                        ls="dotted",
                    )
                    ax.text(
                        anchor_point,
                        np.min(res_data),
                        "zoom_in_point",
                        color="red",
                        ha="center",
                    )
                ax.legend()
                ax.set_title(
                    "Anomaly Detection using BOCPD (both outlier and trend change points): "
                    + data_with_date.index[anchor_point].strftime("%Y-%m-%d")
                )

    elif data_type == "discrete":
        if method == "BOCPD":
            const_hazard = PARAMETERS["const_hazard"]
            hazard_function = partial(constant_hazard, const_hazard)
            R, maxes, MAP, P_M, btCIs = online_changepoint_detection3(
                data=res_data,
                hazard_function=hazard_function,
                log_likelihood_class=multinomial_discrete(levels=res_data.shape[1]),
                n_bt=n_bt_d,
                significance_level=significance_level,
            )

            rowSum = np.sum(res_data, axis=1)
            levels = np.zeros(shape=(len(rowSum), res_data.shape[1]))
            for L in range(res_data.shape[1]):
                templevel = res_data[:, L] / rowSum
                levels[:, L] = templevel

            all_detected, detected_lag, ontime_detected, seg_pair = BOCPD_result(maxes)
            change_x = np.array(all_detected).astype(int)

            change_y = res_data[change_x]
            # plot
            if figure:
                fig, ax = plt.subplots(figsize=figsize)
                for L in range(levels.shape[1]):
                    ax.plot(
                        np.arange(len(levels[:, L])),
                        levels[:, L],
                        label="level " + str(L),
                    )
                    # plot point prediction of each level
                    # level_L_pred = [i[L] for i in P_M]
                    # ax.plot(np.arange(len(levels[:,L])),level_L_pred,label='level '+str(L)+' theoretical prediction mean')

                LL = 0
                for CX in change_x:
                    if CX == 0:
                        continue
                    else:
                        if LL == 0:
                            ax.vlines(
                                CX,
                                0,
                                1,
                                color="k",
                                alpha=0.4,
                                ls="--",
                                label="breakpoints (starting a new segment)",
                            )
                            ax.scatter(
                                CX,
                                levels[CX, 0],
                                c="red",
                                label="breakpoints (starting a new segment)",
                            )
                            for L in range(1, levels.shape[1]):
                                ax.vlines(CX, 0, 1, color="k", alpha=0.4, ls="--")
                                ax.scatter(CX, levels[CX, L], c="red")
                            LL += 1
                        else:
                            for L in range(levels.shape[1]):
                                ax.vlines(CX, 0, 1, color="k", alpha=0.4, ls="--")
                                ax.scatter(CX, levels[CX, L], c="red")

                ax.set_title("Categorical Data (all levels) and Change Points")
                ax.legend()

                for L in range(levels.shape[1]):
                    fig, ax = plt.subplots(figsize=figsize)
                    plotx = np.arange(len(levels[:, L]))
                    ax.plot(plotx, levels[:, L], label="level " + str(L))
                    # plot bootstrap prediction and CI
                    ax.plot(
                        plotx[1:],
                        btCIs[L]["M"][1:],
                        color="blue",
                        alpha=0.4,
                        label="level " + str(L) + " bootstrap prediction median",
                    )
                    ax.plot(
                        plotx[1:], btCIs[L]["L"][1:], color="pink", alpha=0.4, ls="--"
                    )
                    ax.plot(
                        plotx[1:], btCIs[L]["U"][1:], color="pink", alpha=0.4, ls="--"
                    )
                    ax.fill_between(
                        plotx[1:],
                        btCIs[L]["L"][1:],
                        btCIs[L]["U"][1:],
                        color="pink",
                        alpha=0.3,
                        label=str(round((1 - significance_level) * 100))
                        + "% bootstrap prediction CI",
                    )
                    ax.set_title(
                        "Categorical Data (level "
                        + str(L)
                        + "), Change Points and Confidence Interval"
                    )

                    LL = 0
                    for CX in change_x:
                        if CX == 0:
                            continue
                        else:
                            if LL == 0:
                                ax.vlines(
                                    CX,
                                    0,
                                    1,
                                    color="k",
                                    alpha=0.4,
                                    ls="--",
                                    label="breakpoints (starting a new segment)",
                                )
                                ax.scatter(
                                    CX,
                                    levels[CX, L],
                                    c="red",
                                    label="breakpoints (starting a new segment)",
                                )
                                LL += 1
                            else:
                                ax.vlines(CX, 0, 1, color="k", alpha=0.4, ls="--")
                                ax.scatter(CX, levels[CX, L], c="red")

                    ax.legend()

        else:
            raise Exception("Only BOCPD is supported for discrete variables")

    return change_x, change_y, detected_lag, btCIM


# COMMAND ----------


def BOCPD_result(maxes):
    argmaxes = (
        maxes[1:] - 1
    )  # 0's are the changepoints, except the first 0 which is hypothetical
    # some maybe 0 and now becomes -1, restore them to be 0
    indx_1 = np.where(argmaxes == -1)[0]
    argmaxes[indx_1] = 0
    allincrease = np.array([i for i in range(1, argmaxes.shape[0] + 1)])
    temparg = allincrease - argmaxes

    all_detected = set()  # t=1,2,...
    detected_time = np.array([])  # t=1,2,...
    for i in range(1, temparg.shape[0]):
        if (temparg[i] != temparg[i - 1]) and (temparg[i] not in all_detected):
            all_detected.add(temparg[i])
            detected_time = np.concatenate((detected_time, np.array([i + 1])))

    all_detected = np.sort(np.array([i for i in all_detected]))

    detected_lag = (
        detected_time - all_detected
    )  # how many more data does the algorithm need to detect above anomalies
    all_detected = all_detected - 1  # now t=0,1,2,...
    ontime_detected = np.where(argmaxes == 0)[0][
        1:
    ]  # the first 1 is just go from 0 to 1, not anomaly point

    # in temparg, index (time points) with the same number belongs to the same segmentation/model
    unique_index = np.unique(temparg)
    seg_pair = {}  # t = 0,1,2,...
    for i in unique_index:
        for k in range(len(temparg)):
            if temparg[k] == i:
                if i in seg_pair.keys():
                    seg_pair[i].append(k)
                else:
                    seg_pair[i] = [k]

    return all_detected, detected_lag, ontime_detected, seg_pair
