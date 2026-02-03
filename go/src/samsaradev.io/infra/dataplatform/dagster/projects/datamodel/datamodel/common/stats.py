# from csaps import csaps
import copy
import warnings
from functools import partial

import numpy as np
import pandas as pd
import scipy.stats as ss
from sklearn.ensemble import IsolationForest


def online_changepoint_detection(data, hazard_function, log_likelihood_class):

    maxes = np.zeros(len(data) + 1)

    R = np.zeros((len(data) + 1, len(data) + 1))
    R[0, 0] = 1
    MAP = []

    for t, x in enumerate(data):
        # Evaluate the predictive distribution for the new datum under each of
        # the parameters.  This is the standard thing from Bayesian inference.
        predprobs = log_likelihood_class.pdf(x, t)

        # Evaluate the hazard function for this interval
        H = hazard_function(np.array(range(t + 1)))

        # Evaluate the growth probabilities - shift the probabilities down and to
        # the right, scaled by the hazard function and the predictive
        # probabilities.
        R[1 : t + 2, t + 1] = R[0 : t + 1, t] * predprobs * (1 - H)

        # Evaluate the probability that there *was* a changepoint and we're
        # accumulating the mass back down at r = 0.
        R[0, t + 1] = np.sum(R[0 : t + 1, t] * predprobs * H)

        # Renormalize the run length probabilities for improved numerical
        # stability.

        R[:, t + 1] = R[:, t + 1] / np.sum(R[:, t + 1])

        maxes[t] = R[:, t].argmax()
        EST = log_likelihood_class.all_estimates()

        MAP.append([])
        for i in EST:
            if int(maxes[t]) == 0:
                MAP[-1].append(i[0])
            else:
                MAP[-1].append(i[int(maxes[t]) - 1])

        # Update the parameter sets for each possible run length.
        log_likelihood_class.update_theta(x, t)

    # EST = log_likelihood_class.all_estimates()
    maxes[len(data)] = R[:, len(data)].argmax()
    MAP.append([])
    for i in EST:
        if int(maxes[len(data)]) == 0:
            MAP[-1].append(i[0])
        else:
            MAP[-1].append(i[int(maxes[len(data)]) - 1])

    return R, maxes, MAP


def online_changepoint_detection2(
    data, hazard_function, log_likelihood_class, n_bt, significance_level
):

    maxes = np.zeros(len(data) + 1)

    R = np.zeros((len(data) + 1, len(data) + 1))
    R[0, 0] = 1
    MAP = []

    P_M, P_V = [], []
    CIbt_L_ALL, CIbt_mid_ALL, CIbt_U_ALL = [], [], []

    ORDER = log_likelihood_class.order

    for t, x in enumerate(data):
        # print("t: ",t)
        # print("x: ",x)
        # Post. pred.
        # use the latest parameters just updated
        EST_pred = log_likelihood_class.all_estimates()
        # the mean and std of shifted t with loc and scale is
        # if   Y~t_df which has E(Y)=0 and Var(Y)=df/(df-2)
        # then X~t_df(loc,scale) has E(X)=loc and Var(X)=(scale^2)*Var(Y) since X=scale*Y+loc
        # then X_w := X * P(r_t=l|X_1:t) := X * w_l
        # for each value of l, need to find the loc_new = loc * w_l and scale_new = scale * w_l
        # (a) w_l
        w_l = R[: (t + 1), t]
        # (b) loc and scale
        MU = EST_pred[0]
        LAMBDA = EST_pred[1]
        ALPHA = EST_pred[2]
        BETA = EST_pred[3]
        DF = (2 * ALPHA).reshape((-1,))
        X = []
        for tempi in range(ORDER + 1):
            X.append(t**tempi)
        X = np.array([[X]])  # reshape to 1x1x3 to match dimension of other arrays
        LOC = np.matmul(X, MU.transpose((0, 2, 1))).reshape((-1,))
        SCALE = np.sqrt(
            BETA
            * (
                1
                + np.matmul(np.matmul(X, np.linalg.inv(LAMBDA)), X.transpose((0, 2, 1)))
            )
            / ALPHA
        ).reshape((-1,))
        # (c) Expectation and Variance for each l
        EXP = (
            w_l * LOC
        )  # this is theoretical formula, CI has no closed form so use bootstrap below
        VAR = (
            (w_l**2) * (SCALE**2) * DF / (DF - 2)
        )  # this is only an approximation for CI, not used now
        # Calculate the CI assuming independent
        post_mean = np.sum(EXP)
        post_var = np.sum(
            VAR
        )  # assume independent for simplicity, exact result has no closed form
        P_M.append(post_mean)
        P_V.append(post_var)
        # Construct CI using bootstrap
        if n_bt > 0:
            CIbt_L, CIbt_mid, CIbt_U = CIbt_c(
                SCALE, LOC, DF, w_l, n_bt, significance_level
            )
            CIbt_L_ALL.append(CIbt_L)
            CIbt_mid_ALL.append(CIbt_mid)
            CIbt_U_ALL.append(CIbt_U)

        # 3. predictive distribution
        predprobs = log_likelihood_class.pdf(x, t)
        # Evaluate the hazard function for this interval
        H = hazard_function(np.array(range(t + 1)))
        # 4. growth probabilities
        R[1 : t + 2, t + 1] = R[0 : t + 1, t] * predprobs * (1 - H)
        # 5. probability that there *was* a changepoint
        R[0, t + 1] = np.sum(R[0 : t + 1, t] * predprobs * H)
        # 6 & 7. Evidence & run length dist.
        R[:, t + 1] = R[:, t + 1] / np.sum(R[:, t + 1])
        maxes[t] = R[:, t].argmax()
        EST = log_likelihood_class.all_estimates()
        # print("EST: ",EST)
        MAP.append([])
        for i in EST:
            if int(maxes[t]) == 0:
                MAP[-1].append(i[0])
            else:
                MAP[-1].append(i[int(maxes[t]) - 1])
        # 8. Update the parameter sets for each possible run length.
        log_likelihood_class.update_theta(x, t)

    # EST = log_likelihood_class.all_estimates()
    maxes[len(data)] = R[:, len(data)].argmax()
    MAP.append([])
    for i in EST:
        if int(maxes[len(data)]) == 0:
            MAP[-1].append(i[0])
        else:
            MAP[-1].append(i[int(maxes[len(data)]) - 1])

    return (
        R,
        maxes,
        MAP,
        np.array(P_M),
        np.array(P_V),
        LOC,
        SCALE,
        DF,
        CIbt_L_ALL,
        CIbt_mid_ALL,
        CIbt_U_ALL,
    )


def online_changepoint_detection3(
    data, hazard_function, log_likelihood_class, n_bt, significance_level
):

    maxes = np.zeros(len(data) + 1)

    R = np.zeros((len(data) + 1, len(data) + 1))
    R[0, 0] = 1
    MAP = []
    P_M = []

    CIs = {}

    for t, x in enumerate(data):
        # Post. pred.
        # use the latest parameters just updated
        EST_pred = log_likelihood_class.all_estimates()
        # print("=====")
        # print('t: ',t)
        # (a) w_l
        w_l = np.array([R[: (t + 1), t]]).reshape(-1, 1)
        # print('w_l: ',w_l)
        # (b) parameters
        # print("EST_pred: ",EST_pred)
        ALPHA = EST_pred[0]
        # print('ALPHA: ',ALPHA)
        # print('np.sum(ALPHA,axis=1): ',np.sum(ALPHA,axis=1,keepdims=True))
        ALPHA_scaled = ALPHA / np.sum(ALPHA, axis=1, keepdims=True)
        # print('ALPHA_reweight: ',ALPHA)
        # (c) Expectation and Variance for each l
        EXP = (
            w_l * ALPHA_scaled
        )  # this is theoretical formula, CI has no closed form so use bootstrap below
        # print('EXP: ',EXP)
        post_mean = np.sum(
            EXP, axis=0
        )  # list of dim 1*k where k is the number of levels
        # print('post_mean: ',post_mean)
        P_M.append(post_mean)
        # print('P_M: ',P_M)

        # Construct CI using bootstrap
        sample_quantiles = CIbt_d(
            ALPHA=ALPHA,
            w_l=w_l,
            data=x,
            n_bt=n_bt,
            significance_level=significance_level,
        )

        for col in range(sample_quantiles.shape[1]):
            tempcol = np.array(sample_quantiles.iloc[:, col])
            levelname = col
            if col not in CIs:
                CIs[col] = {"L": [tempcol[0]], "M": [tempcol[1]], "U": [tempcol[2]]}
            else:
                CIs[col]["L"].append(tempcol[0])
                CIs[col]["M"].append(tempcol[1])
                CIs[col]["U"].append(tempcol[2])

        # 3. predictive distribution
        predprobs = log_likelihood_class.pdf(x, t)
        # Evaluate the hazard function for this interval
        H = hazard_function(np.array(range(t + 1)))

        # 4. growth probabilities
        R[1 : t + 2, t + 1] = R[0 : t + 1, t] * predprobs * (1 - H)

        # 5. probability that there *was* a changepoint
        R[0, t + 1] = np.sum(R[0 : t + 1, t] * predprobs * H)

        # 6 & 7. Evidence & run length dist.
        R[:, t + 1] = R[:, t + 1] / np.sum(R[:, t + 1])

        maxes[t] = R[:, t].argmax()
        EST = log_likelihood_class.all_estimates()
        MAP.append([])
        for i in EST:
            if int(maxes[t]) == 0:
                MAP[-1].append(i[0])
            else:
                MAP[-1].append(i[int(maxes[t]) - 1])

        # 8. Update the parameter sets for each possible run length.
        log_likelihood_class.update_theta(x, t)

    # EST = log_likelihood_class.all_estimates()
    maxes[len(data)] = R[:, len(data)].argmax()
    MAP.append([])
    for i in EST:
        if int(maxes[len(data)]) == 0:
            MAP[-1].append(i[0])
        else:
            MAP[-1].append(i[int(maxes[len(data)]) - 1])

    return R, maxes, MAP, P_M, CIs


def CIbt_d(ALPHA, w_l, data, n_bt, significance_level):

    SAMPLES = []
    # print('data: ',data)
    # (1) do for all n_bt at the same time since they have the same weights w_l
    # print(ALPHA.shape[0])
    # print(w_l.flatten())
    dist_indx = np.random.choice(
        np.arange(ALPHA.shape[0]), size=n_bt, replace=True, p=w_l.flatten()
    )
    # print(dist_indx)
    # (2)
    for bt_i in range(n_bt):
        dist = dist_indx[bt_i]  # specify which distribution we are drawing from
        alpha_ = ALPHA[dist, :]
        # if bt_i==0:
        #   print('alpha_: ',alpha_)

        # generate DM distribution
        RV = DMrvs(alpha=alpha_, data=data)

        SAMPLES.append(RV)

    # (3)
    SAMPLES = pd.DataFrame(SAMPLES)
    sample_quantiles = SAMPLES.quantile(q=[0.025, 0.5, 0.975])

    return sample_quantiles


# generate Dirichlet-multinomial random samples
# https://www.tensorflow.org/probability/api_docs/python/tfp/distributions/DirichletMultinomial
def DMrvs(alpha, data):
    # (1) probs = [p_0,...,p_{K-1}] ~ Dir(alpha)
    probs = ss.dirichlet.rvs(alpha)
    # (2) counts = [n_0,...,n_{K-1}] ~ Multinomial(total_count, probs)
    # print(np.sum(data))
    # print(probs)
    # print(probs.flatten())
    proportion = ss.multinomial.rvs(n=np.sum(data), p=probs.flatten()) / np.sum(data)

    return proportion


# bootstrap CI for post. pred. CI
# default, use 1000 bootstrap samples

# we have a mixture of shifted t distribution
# For each bootstrap sample, DO:
# (1) randomly select one shifted t distribution according to the weight w_l
# (2) generate from that t distribution
# (3) construct CI using the quantiles of drawn samples


def CIbt_c(SCALE, LOC, DF, w_l, n_bt, significance_level):
    # print("in CIbt")
    SAMPLES = []
    # (1) do for all n_bt at the same time since they have the same weights w_l
    dist_indx = np.random.choice(np.arange(len(SCALE)), size=n_bt, replace=True, p=w_l)
    # print("(2)")
    # (2)
    for bt_i in range(n_bt):
        dist = dist_indx[bt_i]  # specify which t distribution we are drawing from
        scale_ = SCALE[dist]
        loc_ = LOC[dist]
        df_ = DF[dist]
        RV = ss.t.rvs(df=df_, loc=loc_, scale=scale_, size=1)
        SAMPLES.append(RV)
    # print("(3)")
    # (3)
    CIbt_L, CIbt_mid, CIbt_U = np.quantile(
        SAMPLES, [significance_level / 2, 0.5, 1 - significance_level / 2]
    )
    # print("(4)")
    return CIbt_L, CIbt_mid, CIbt_U


class multinomial_discrete:  # prior is Gaussian with only unknown mean
    def __init__(self, levels):
        self.levels = levels
        self.alpha0 = self.alpha = np.array(
            [[30 / self.levels for _ in range(levels)]]
        )  # prior parameters alpha = 1/(# of levels)

    def pdf(self, data: np.array, t):  # post. pred. distribution is DirMult
        """
        Return the pdf function of the t distribution

        Parmeters:
            data - the datapoints to be evaluated (shape: 1 x K vector, the i-th element represents the count for level i)
        """

        return ss.dirichlet_multinomial.pmf(x=data, alpha=self.alpha, n=np.sum(data))

    def update_theta(self, data: np.array, t):
        """
        Performs a bayesian update on the prior parameters, given data
        Parmeters:
            data - the datapoints to be evaluated (shape: 1 x D vector)
        """
        alphaT0 = np.concatenate((self.alpha0, self.alpha + data))
        self.alpha = alphaT0

    def all_estimates(self):
        return [self.alpha]


# hazard_functions
def constant_hazard(lam, r):
    """
    Hazard function for bayesian online learning
    Arguments:
        lam - inital prob
        r - R matrix
    """
    return 1 / lam * np.ones(r.shape)


class G_unknown_mean_var:  # prior is Gaussian with unknown mean and unknown variance or precision (update is exactly the same)
    def __init__(
        self, alpha: float = 0.1, beta: float = 0.1, kappa: float = 1, mu: float = 0
    ):

        self.alpha0 = self.alpha = np.array([alpha])
        self.beta0 = self.beta = np.array([beta])
        self.kappa0 = self.kappa = np.array([kappa])
        self.mu0 = self.mu = np.array([mu])

    def pdf(self, data: np.array, t):
        """
        Return the pdf function of the t distribution

        Parmeters:
            data - the datapoints to be evaluated (shape: 1 x D vector)
        """
        return ss.t.pdf(
            x=data,
            df=2 * self.alpha,
            loc=self.mu,
            scale=np.sqrt(self.beta * (self.kappa + 1) / (self.alpha * self.kappa)),
        )

    def update_theta(self, data: np.array, t):
        """
        Performs a bayesian update on the prior parameters, given data
        Parmeters:
            data - the datapoints to be evaluated (shape: 1 x D vector)
        """
        muT0 = np.concatenate(
            (self.mu0, (self.kappa * self.mu + data) / (self.kappa + 1))
        )
        kappaT0 = np.concatenate((self.kappa0, self.kappa + 1.0))
        alphaT0 = np.concatenate((self.alpha0, self.alpha + 0.5))
        betaT0 = np.concatenate(
            (
                self.beta0,
                self.beta
                + (self.kappa * (data - self.mu) ** 2) / (2.0 * (self.kappa + 1.0)),
            )
        )

        self.mu = muT0
        self.kappa = kappaT0
        self.alpha = alphaT0
        self.beta = betaT0

    def all_estimates(self):
        return [self.mu, self.kappa, self.alpha, self.beta]


class G_unknown_mean:  # prior is Gaussian with only unknown mean
    def __init__(self, known_var, mu=0, var=1):
        self.mu0 = self.mu = np.array([mu])
        self.var0 = self.var = np.array([var])
        self.known_var = known_var

    def pdf(self, data: np.array, t):
        """
        Return the pdf function of the t distribution

        Parmeters:
            data - the datapoints to be evaluated (shape: 1 x D vector)
        """
        return ss.norm.pdf(
            x=data, loc=self.mu, scale=np.sqrt(self.var + self.known_var)
        )

    def update_theta(self, data: np.array, t):
        """
        Performs a bayesian update on the prior parameters, given data
        Parmeters:
            data - the datapoints to be evaluated (shape: 1 x D vector)
        """
        muT0 = np.concatenate(
            (
                self.mu0,
                (self.mu / self.var + data / self.known_var)
                / (1 / self.var + 1 / self.known_var),
            )
        )
        varT0 = np.concatenate((self.var0, 1 / (1 / self.var + 1 / self.known_var)))

        self.mu = muT0
        self.var = varT0

    def all_estimates(self):
        return self.mu, self.var


class Bayesian_Regression:  # prior is Bayesian Polynomial Regression (on 1,t,t^2,...) with arbitrary (need to specify) order and unknown variance
    def __init__(
        self,
        order,
        alpha,
        beta,
        LambdaInit,
        MuInit,  # order is the order of polynomial, order = 1,2,... order=1 includes both intercept and slope t, order=2 includes 1, t, t^2
    ):
        self.order = order
        self.alpha0 = self.alpha = np.array(
            [[[alpha]]]
        )  # reshape to 1x1x1 - first dimension for concatenate later
        self.beta0 = self.beta = np.array(
            [[[beta]]]
        )  # reshape to 1x1x1 - first dimension for concatenate later
        if isinstance(LambdaInit, list):
            self.Lambda0 = self.Lambda = np.array(
                [LambdaInit]
            )  # reshape to 1x(order+1)x(order+1) - first dimension for concatenate later
        else:
            self.Lambda0 = self.Lambda = np.array(
                [np.diag([LambdaInit for _ in range(self.order + 1)])]
            )  # reshape to 1x3x3 - first dimension for concatenate later
        self.mu0 = self.mu = np.array(
            [[np.array(MuInit)]]
        )  # reshape to 1x1x(order+1) - first dimension for concatenate later

    def pdf(self, data: np.array, t: float):  # posterior prediction
        """
        Return the pdf function of the t distribution

        Parmeters:
            data - the datapoints to be evaluated (shape: 1 x D vector)
            t - current time point, used to create design matrix of the polynomial regression (shape: 1 x (order+1) vector)
        """
        data = np.array([[data]])  # reshape to 1x1x1 to match dimension of other arrays

        X = []
        for tempi in range(self.order + 1):
            X.append(t**tempi)

        X = np.array([[X]])  # reshape to 1x1x3 to match dimension of other arrays

        DF = 2 * self.alpha
        LOC = np.matmul(X, self.mu.transpose((0, 2, 1)))

        SHAPE = (
            self.beta
            * (
                1
                + np.matmul(
                    np.matmul(X, np.linalg.inv(self.Lambda)), X.transpose((0, 2, 1))
                )
            )
            / self.alpha
        )

        DF_flat = DF.reshape((-1,))
        LOC_flat = LOC.reshape((-1,))
        SHAPE_flat = SHAPE.reshape((-1,))

        return ss.t.pdf(x=data, df=DF_flat, loc=LOC_flat, scale=np.sqrt(SHAPE_flat))

    def update_theta(self, data: np.array, t: float):
        """
        Performs a bayesian update on the prior parameters, given data
        Parmeters:
            data - the datapoints to be evaluated (shape: 1 x D vector)
            t - current time point, used to create design matrix of the polynomial regression (shape: 1 x (order+1) vector)
        """

        data = np.array([[data]])  # reshape to 1x1x1 to match dimension of other arrays

        X = []
        for tempi in range(self.order + 1):
            X.append(t**tempi)

        X = np.array([[X]])  # reshape to 1x1x3 to match dimension of other arrays
        new_Lambda = np.matmul(X.transpose((0, 2, 1)), X) + self.Lambda
        new_Mu0 = np.linalg.inv(new_Lambda)
        new_Mu1 = np.matmul(self.Lambda, self.mu.transpose((0, 2, 1))) + np.matmul(
            X.transpose((0, 2, 1)), data
        )
        new_Mu = np.matmul(new_Mu0, new_Mu1)
        new_Mu = new_Mu.transpose((0, 2, 1))
        muT0 = np.concatenate((self.mu0, new_Mu))
        LambdaT0 = np.concatenate((self.Lambda0, new_Lambda))
        alphaT0 = np.concatenate((self.alpha0, self.alpha + 0.5))
        betaT0 = np.concatenate(
            (
                self.beta0,
                self.beta
                + 0.5
                * (
                    np.matmul(data.transpose((0, 2, 1)), data)
                    + np.matmul(
                        np.matmul(self.mu, self.Lambda), self.mu.transpose((0, 2, 1))
                    )
                    - np.matmul(
                        np.matmul(new_Mu, new_Lambda), new_Mu.transpose((0, 2, 1))
                    )
                ),
            )
        )

        self.mu = muT0
        self.Lambda = LambdaT0
        self.alpha = alphaT0
        self.beta = betaT0

    def all_estimates(self):
        return self.mu, self.Lambda, self.alpha, self.beta


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
        if (
            frequency == "W"
            or frequency == "w"
            or frequency == "week"
            or frequency == "Week"
        ):
            DATA = DATA.asfreq("W-" + DOW[:3])
        elif (
            frequency == "M"
            or frequency == "m"
            or frequency == "month"
            or frequency == "Month"
        ):
            DATA = DATA.asfreq("MS")
        elif (
            frequency == "D"
            or frequency == "d"
            or frequency == "day"
            or frequency == "Day"
        ):
            DATA = DATA.asfreq(frequency)
        else:
            raise Exception(
                "Cannot recognize the specified frequency, acceptable frequency 'week','month','day'"
            )

        mis_indx = np.where(DATA.isna())[0]
        for INDX in mis_indx:
            left = INDX - 1
            right = INDX + 1
            while (left >= 0) and (left in mis_indx):
                left -= 1
            while (right < len(DATA)) and (right in mis_indx):
                right += 1
            if left < 0:
                if right < len(DATA):
                    DATA[INDX] = DATA[right]
                else:
                    DATA[INDX] = 0
            elif right >= len(DATA):
                if left >= 0:
                    DATA[INDX] = DATA[left]
                else:
                    DATA[INDX] = 0
            else:
                DATA[INDX] = (DATA[left] + DATA[right]) / 2

        DATA2 = np.array(DATA)

    else:
        # for discrete data, the input DATA has k+1 columns, one timestamp column and k columns for the k levels
        DATA[time_column] = pd.to_datetime(DATA[time_column])
        DOW = DATA[time_column].dt.day_name()[0]
        DATA = DATA.set_index(time_column)
        DATA = DATA[value_column]  # k columns for the k levels of the variable

        if (
            frequency == "W"
            or frequency == "w"
            or frequency == "week"
            or frequency == "Week"
        ):
            DATA = DATA.asfreq("W-" + DOW[:3])
        elif (
            frequency == "M"
            or frequency == "m"
            or frequency == "month"
            or frequency == "Month"
        ):
            DATA = DATA.asfreq("MS")
        elif (
            frequency == "D"
            or frequency == "d"
            or frequency == "day"
            or frequency == "Day"
        ):
            DATA = DATA.asfreq(frequency)
        else:
            raise Exception(
                "Cannot recognize the specified frequency, acceptable frequency 'week','month','day'"
            )

        NROW = DATA.shape[0]
        for COL in range(DATA.shape[1]):
            mis_indx = np.where(DATA.iloc[:, COL].isna())[0]

            for INDX in mis_indx:
                left = INDX - 1
                right = INDX + 1
                while (left >= 0) and (left in mis_indx):
                    left -= 1
                while (right < NROW) and (right in mis_indx):
                    right += 1
                if left < 0:
                    if right < NROW:
                        DATA.iloc[INDX, COL] = DATA.iloc[right, COL]
                    else:
                        DATA.iloc[INDX, COL] = 0
                elif right >= NROW:
                    if left >= 0:
                        DATA.iloc[INDX, COL] = DATA.iloc[left, COL]
                    else:
                        DATA.iloc[INDX, COL] = 0
                else:
                    DATA.iloc[INDX, COL] = (
                        DATA.iloc[left, COL] + DATA.iloc[right, COL]
                    ) / 2

        DATA2 = np.array(DATA)

    return DATA2, DATA


def hyperparameters(task_method, **kwargs):
    # season
    if task_method == "MSTL":
        periods = kwargs.get(
            "periods", [4]
        )  # can specify multiple periods using tuple (7,30) etc.
        windows = kwargs.get("windows", list(np.repeat(61, len(periods))))
        # lmbda=kwargs.get('lmbda','auto')
        # lmbda_range=kwargs.get('lmbda_range',np.arange(-3,3,0.1))
        hps = {"periods": periods, "windows": windows}

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
        n_bkps = kwargs.get("n_bkps", 5)
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
        const_hazard = kwargs.get("const_hazard", 30)
        ORDER = kwargs.get("ORDER", 1)
        MuInit = kwargs.get("MuInit", [0 for _ in range(ORDER + 1)])
        LambdaInit = kwargs.get("LambdaInit", 1e-3)
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


# -------------------------------------------------------------------------------------------------------------- #
# -------------------------------------------------------------------------------------------------------------- #
# -------------------------------------------------------------------------------------------------------------- #
# -------------------------------------------------------------------------------------------------------------- #


def detect(
    data_map,
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

    data_prep = {"date": data_map.keys(), "value_column": data_map.values()}

    data = pd.DataFrame(data_prep)

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
    ALL_DATES = data_with_date.index.strftime("%m/%d/%Y")
    if isinstance(zoom_in_point, str):
        INDX = np.where(ALL_DATES == zoom_in_point)[0]
        if len(INDX) == 1:
            zoom_in_point = int(np.where(ALL_DATES == zoom_in_point)[0][0])
        else:
            print(
                "Warning: The specified date ",
                zoom_in_point,
                " is not observed in the data.",
            )
            zoom_in_date = pd.to_datetime(zoom_in_point, format="%m/%d/%Y")
            zoom_in_point = int(np.where(data_with_date.index < zoom_in_date)[0][-1])
            print(
                "The fisrt day (Monday) of that week ",
                data_with_date.index[
                    np.where(data_with_date.index < zoom_in_date)[0][-1]
                ].strftime("%m/%d/%Y"),
                " is used.",
            )

    elif isinstance(zoom_in_point, int):
        zoom_in_point = zoom_in_point

    zoom_in_before = kwargs.get("zoom_in_before", 30)
    zoom_in_after = kwargs.get("zoom_in_after", 11)
    if zoom_in_point:  # zoom_in_point start from 0
        if zoom_in_point < zoom_in_before:
            if (zoom_in_point + zoom_in_after) < len(data):
                if data_type == "continuous":
                    data = data[: (zoom_in_point + 1)]
                    data_with_date = data_with_date[: (zoom_in_point + 1)]
                elif data_type == "discrete":
                    data = data[: (zoom_in_point + 1), :]
                    data_with_date = data_with_date.iloc[: (zoom_in_point + 1), :]
            else:
                data = data[:]
                data_with_date = data_with_date[:]
            anchor_point = zoom_in_point
        else:
            if (zoom_in_point + zoom_in_after) < len(data):
                if data_type == "continuous":
                    data = data[
                        (zoom_in_point - zoom_in_before) : (
                            zoom_in_point + zoom_in_after
                        )
                    ]
                    data_with_date = data_with_date[
                        (zoom_in_point - zoom_in_before) : (
                            zoom_in_point + zoom_in_after
                        )
                    ]
                elif data_type == "discrete":
                    data = data[
                        (zoom_in_point - zoom_in_before) : (
                            zoom_in_point + zoom_in_after
                        ),
                        :,
                    ]
                    data_with_date = data_with_date.iloc[
                        (zoom_in_point - zoom_in_before) : (
                            zoom_in_point + zoom_in_after
                        ),
                        :,
                    ]
            else:
                if data_type == "continuous":
                    data = data[(zoom_in_point - zoom_in_before) :]
                    data_with_date = data_with_date[(zoom_in_point - zoom_in_before) :]
                elif data_type == "discrete":
                    data = data[(zoom_in_point - zoom_in_before) :, :]
                    data_with_date = data_with_date.iloc[
                        (zoom_in_point - zoom_in_before) :, :
                    ]
            anchor_point = zoom_in_before
    else:
        anchor_point = None

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
        detected_lag,
    ) = ([], [], [], [], [], [], [], [], [], [])
    INPUT_TASKS = list(task.keys())
    PARAMETERS = {}

    # --------------- #
    # continuous case #
    # --------------- #

    if data_type == "continuous":
        ### algorithm
        if "season" in INPUT_TASKS:
            PARAMETERS[task["season"]] = hyperparameters(
                task_method=task["season"], **kwargs
            )
            season, trend, resid, deseason = detect_season(
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
                    resid,
                    data_with_date,
                    frequency,
                    PARAMETERS[task["outlier"]],
                    method="isoForest",
                    **kwargs
                )
            else:
                outlier_x, outlier_y, impute_data = detect_outlier(
                    data,
                    data_with_date,
                    frequency,
                    PARAMETERS[task["outlier"]],
                    method=task["outlier"],
                    **kwargs
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
                change_x, change_y, detected_lag = detect_trend(
                    trend,
                    data_with_date,
                    frequency,
                    PARAMETERS[task["trend"]],
                    data_type=data_type,
                    method=task["trend"],
                    anchor_point=anchor_point,
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
                change_x, change_y, detected_lag = detect_trend(
                    trend,
                    data_with_date,
                    frequency,
                    PARAMETERS[task["trend"]],
                    data_type=data_type,
                    method=task["trend"],
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
                change_x, change_y, detected_lag = detect_trend(
                    impute_data,
                    data_with_date,
                    frequency,
                    PARAMETERS[task["trend"]],
                    data_type=data_type,
                    method=task["trend"],
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
                change_x, change_y, detected_lag = detect_trend(
                    data,
                    data_with_date,
                    frequency,
                    PARAMETERS[task["trend"]],
                    data_type=data_type,
                    method=task["trend"],
                    anchor_point=anchor_point,
                    **kwargs
                )

        ### plot
        figure = kwargs.get("figure", False)
        figsize = kwargs.get("figsize", [18, 6])

        if figure:
            if "trend" in INPUT_TASKS and (task["trend"] != "BOCPD"):
                if data_type == "continuous":
                    y = copy.deepcopy(data)
                    y = y.reshape(
                        -1,
                    )

                    fig, ax = plt.subplots(figsize=figsize)
                    periods = kwargs.get("periods", [4])
                    if 7 in periods:
                        loc = plticker.MultipleLocator(base=30)  # plot every 30 points
                    else:
                        loc = plticker.MultipleLocator(base=5)  # plot every 5 points

                    ax.xaxis.set_major_locator(loc)

                    ax.tick_params("x", rotation=90, labelsize=8)
                    DATE = data_with_date.index
                    res_data = np.array(data_with_date)
                    plot_x = DATE.strftime("%m/%d/%Y")

                    ax.plot(plot_x, res_data, c="tab:blue")

                    if "outlier" in INPUT_TASKS:
                        ax.scatter(
                            outlier_x,
                            y[outlier_x],
                            color="brown",
                            label="outlier points",
                        )
                    if "trend" in INPUT_TASKS:
                        ax.scatter(
                            change_x,
                            y[change_x],
                            color="red",
                            label="trend segmentation point",
                        )
                    ax.set_title("Original Data and Change Points")
                    ax.legend()

    # ------------- #
    # discrete case #
    # ------------- #
    elif data_type == "discrete":

        col_names = list(data_with_date.columns)
        ### algorithm
        # for each one of the column of the discrete variable, we pass it into season, outlier and trend module
        for COL in range(data_with_date.shape[1]):
            print("========================================================")
            print("Process the " + str(COL) + "-th level of the categorical variable")
            print("========================================================")
            data_C = data_with_date.iloc[:, COL]

            if "season" in INPUT_TASKS:
                PARAMETERS[task["season"]] = hyperparameters(
                    task_method=task["season"], **kwargs
                )
                season, trend, resid, deseason = detect_season(
                    data_C,
                    PARAMETERS[task["season"]],
                    method=task["season"],
                    COL=COL + 1,
                    **kwargs
                )
                # replace each column with the output from season module
                data[:, COL] = trend

                # for outputs
                periods = PARAMETERS[task["season"]]["periods"]
                col_names.extend(
                    [data_with_date.columns[COL] + str(i) for i in periods]
                )
                if COL == 0:
                    NEWdata = np.column_stack([data_with_date, season])
                else:
                    NEWdata = np.column_stack([NEWdata, season])
                season = pd.DataFrame(
                    data=NEWdata, index=data_with_date.index, columns=col_names
                )

            if "outlier" in INPUT_TASKS:
                PARAMETERS[task["outlier"]] = hyperparameters(
                    task_method=task["outlier"], **kwargs
                )
                if "season" in INPUT_TASKS:
                    outlier_x, outlier_y, impute_data = detect_outlier(
                        resid,
                        data_with_date,
                        frequency,
                        PARAMETERS[task["outlier"]],
                        method=task["outlier"],
                        **kwargs
                    )
                    # each column should be replaced by the output trend from season module, already updated, no replacement here
                else:
                    data_C = np.array(data_C)
                    outlier_x, outlier_y, impute_data = detect_outlier(
                        data_C,
                        data_with_date,
                        frequency,
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
            change_x, change_y, detected_lag = detect_trend(
                data.round(),
                data_with_date,
                frequency,
                PARAMETERS[task["trend"]],
                data_type=data_type,
                method=task["trend"],
                anchor_point=anchor_point,
                **kwargs
            )

        ### plot
        figure = kwargs.get("figure", False)
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
    if "season" in INPUT_TASKS:
        if data_type == "continuous":
            periods = PARAMETERS[task["season"]]["periods"]
            col_names = ["value"]
            col_names.extend(periods)
            NEWdata = np.column_stack([data_with_date, season])
            season = pd.DataFrame(
                data=NEWdata, index=data_with_date.index, columns=col_names
            )
            data_with_date = pd.DataFrame(
                data=NEWdata, index=data_with_date.index, columns=col_names
            )

    # return season,trend,resid,deseason,outlier_x,outlier_y,impute_data,change_x,change_y,detected_lag,data_with_date
    # return change_x
    # return_map = {'season': season,
    #               'trend': trend,
    #               'resid': resid,
    #               'deseason': deseason,
    #               'outlier_x': outlier_x,
    #               'outlier_y': outlier_y,
    #               'impute_data': impute_data,
    #               'change_x': change_x,
    #               'change_y': change_y,
    #               'detected_lag': detected_lag,
    #               'data_with_date': data_with_date
    #               }

    # return_map = {'change_x': change_x.tolist()},

    change_x = change_x.tolist()
    change_y = change_y.tolist()
    detected_lag = detected_lag.tolist()
    return_map = dict(
        season=season,
        trend=trend,
        resid=resid,
        deseason=deseason,
        outlier_x=outlier_x,
        outlier_y=outlier_y,
        impute_data=impute_data,
        change_x=change_x,
        change_y=change_y,
        detected_lag=change_y,
    )

    return return_map

    # return change_x.tolist()


def detect_season(
    data, PARAMETERS, method="MSTL", data_type="continuous", COL=None, **kwargs
):

    print("... analyzing seasonal effect ...")
    DATA = copy.deepcopy(data)

    DATE = DATA.index
    DATA = np.array(DATA)

    periods = PARAMETERS[
        "periods"
    ]  # can specify multiple periods using tuple (7,30) etc.
    windows = PARAMETERS["windows"]

    N_Periods = len(periods)

    res = MSTL(DATA, periods=periods, windows=windows).fit()

    figure = kwargs.get("figure", False)
    figsize = kwargs.get("figsize", [18, 10])

    # print(res.seasonal.shape)
    # print(np.sum(res.seasonal,axis=1))

    # plots
    if figure:
        fig, ax = plt.subplots(4, 1, figsize=figsize)
        fig.tight_layout(pad=5)
        if 7 in periods:
            loc = plticker.MultipleLocator(base=30)  # plot every 30 points
        else:
            loc = plticker.MultipleLocator(base=5)  # plot every 5 points

        ax[0].xaxis.set_major_locator(loc)
        ax[0].tick_params("x", rotation=90, labelsize=8)
        ax[0].plot(DATE.strftime("%m/%d/%Y"), res.observed)
        ax[0].set_ylabel("Original Data")
        if COL:
            ax[0].set_title(str(COL - 1) + "-th level of the categorical variable")

        ax[1].xaxis.set_major_locator(loc)
        ax[1].tick_params("x", rotation=90, labelsize=8)
        if N_Periods > 1:
            for col in range(res.seasonal.shape[1]):
                ax[1].plot(
                    DATE.strftime("%m/%d/%Y"),
                    res.seasonal[:, col],
                    label="period: " + str(periods[col]),
                )
            ax[1].legend()
        else:
            ax[1].plot(DATE.strftime("%m/%d/%Y"), res.seasonal)
        ax[1].set_ylabel("Seasonality")

        ax[2].xaxis.set_major_locator(loc)
        ax[2].tick_params("x", rotation=90, labelsize=8)
        ax[2].plot(DATE.strftime("%m/%d/%Y"), res.trend)
        ax[2].set_ylabel("Trend")

        ax[3].xaxis.set_major_locator(loc)
        ax[3].tick_params("x", rotation=90, labelsize=8)
        ax[3].plot(DATE.strftime("%m/%d/%Y"), res.resid)
        ax[3].set_ylabel("Residual")

        # ## zoom in plot for final presentation
        # fig, ax = plt.subplots(2,1,figsize=figsize)
        # fig.tight_layout(pad=5)
        # loc = plticker.MultipleLocator(base=1) # plot every 1 point

        # PLOT_L = len(res.observed) - 45

        # ax[0].xaxis.set_major_locator(loc)
        # ax[0].tick_params("x",rotation=90, labelsize=8)
        # if N_Periods > 1:
        #   for col in range(res.seasonal.shape[1]):
        #     ax[0].plot(DATE.strftime("%m/%d/%Y")[PLOT_L:],res.seasonal[:,col][PLOT_L:],label="period: "+str(periods[col]))
        #   ax[0].legend()
        # else:
        #   ax[0].plot(DATE.strftime("%m/%d/%Y")[PLOT_L:],res.seasonal[PLOT_L:])
        # ax[0].set_ylabel('Seasonality')

        # ax[1].xaxis.set_major_locator(loc)
        # ax[1].tick_params("x",rotation=90, labelsize=8)
        # if N_Periods > 1:
        #   for col in range(res.seasonal.shape[1]):
        #     ax[1].plot(DATE.strftime("%m/%d/%Y")[PLOT_L-52:PLOT_L-7],res.seasonal[:,col][PLOT_L-52:PLOT_L-7],label="period: "+str(periods[col]))
        #   ax[1].legend()
        # else:
        #   ax[1].plot(DATE.strftime("%m/%d/%Y")[PLOT_L-52:PLOT_L-7],res.seasonal[PLOT_L-52:PLOT_L-7])
        # ax[1].set_ylabel('Seasonality')

    if N_Periods > 1:
        return (
            res.seasonal,
            res.trend,
            res.resid,
            res.observed - np.sum(res.seasonal, axis=1),
        )
    else:
        return res.seasonal, res.trend, res.resid, res.observed - res.seasonal


def detect_outlier(
    data, data_with_dates, frequency, PARAMETERS, method="spline_isoForest", **kwargs
):
    print("... detecting outliers ...")

    figsize = kwargs.get("figsize", [18, 6])
    figure = kwargs.get("figure", False)

    if (method == "spline_isoForest") or (method == "isoForest"):
        forest_n_estimators = PARAMETERS["forest_n_estimators"]
        forest_max_samples = PARAMETERS["forest_max_samples"]
        forest_contamination = PARAMETERS["forest_contamination"]
        forest_bootstrap = PARAMETERS["forest_bootstrap"]
        random_state = PARAMETERS["random_state"]

        y = copy.deepcopy(data)
        res_datadate = copy.deepcopy(data_with_date)
        DATE = res_datadate.index

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
        ax.plot(DATE.strftime("%m/%d/%Y"), y)  # original data
        ax.scatter(
            outlier_x, y[outlier_x], color="red"
        )  # labeled outliers on original data

        if (
            frequency == "D"
            or frequency == "d"
            or frequency == "day"
            or frequency == "Day"
        ):
            loc = plticker.MultipleLocator(base=30)  # plot every 30 points
        else:
            loc = plticker.MultipleLocator(base=5)  # plot every 5 points

        ax.xaxis.set_major_locator(loc)
        ax.tick_params("x", rotation=90, labelsize=8)

        if method == "spline_isoForest":
            ax.plot(DATE.strftime("%m/%d/%Y"), fitted)  # spline fitted data
            ax.set_title("Data, Smoothed Fit and Outliers")

            fig, ax = plt.subplots(figsize=figsize)
            ax.xaxis.set_major_locator(loc)
            ax.tick_params("x", rotation=90, labelsize=8)
            ax.plot(DATE.strftime("%m/%d/%Y"), data1)  # residual after spline fit
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


def detect_trend(
    data,
    data_with_date,
    frequency,
    PARAMETERS,
    method="Dynp",
    data_type="continuous",
    anchor_point=None,
    lt_include_red_dots=False,
    **kwargs
):

    print("... detecting trend segmentations ...")
    res_data = copy.deepcopy(data)
    res_datadate = copy.deepcopy(data_with_date)
    DATE = res_datadate.index
    res_data = np.array(res_data)

    figure = kwargs.get("figure", False)
    figsize = kwargs.get("figsize", [18, 6])
    n_bt_c = kwargs.get("n_bt", 10000)
    n_bt_d = kwargs.get("n_bt", 1000)
    significance_level = kwargs.get("significance_level", 0.05)
    detected_lag = []

    if data_type == "continuous":
        if method == "PELT":
            pass
            # model = PARAMETERS["model"]
            # min_size = PARAMETERS["min_size"]
            # jump = PARAMETERS["jump"]
            # penalty = PARAMETERS["penalty"]
            # fit_data = PARAMETERS["fit_data"]

            # if model != "ar":
            #     algo = rpt.detection.Pelt(
            #         model=model, min_size=min_size, jump=jump
            #     ).fit(fit_data)
            # else:
            #     ar_order = PARAMETERS["ar_order"]
            #     algo = rpt.detection.Pelt(
            #         model=model,
            #         min_size=min_size,
            #         jump=jump,
            #         params={"order": ar_order},
            #     ).fit(fit_data)

            # my_bkps = np.array(algo.predict(pen=penalty))
            # my_bkps = my_bkps[:-1]  # the last one is always the last obs. remove

            # # plot
            # if figure:
            #     fig, ax = plt.subplots(figsize=figsize)
            #     if (
            #         frequency == "D"
            #         or frequency == "d"
            #         or frequency == "day"
            #         or frequency == "Day"
            #     ):
            #         loc = plticker.MultipleLocator(base=30)  # plot every 30 points
            #     else:
            #         loc = plticker.MultipleLocator(base=5)  # plot every 5 points

            #     ax.xaxis.set_major_locator(loc)
            #     ax.tick_params("x", rotation=90, labelsize=8)
            #     plot_x = DATE.strftime("%m/%d/%Y")
            #     ax.plot(plot_x, res_data, c="tab:blue")
            #     if lt_include_red_dots:
            #         ax.scatter(
            #             my_bkps, res_data[my_bkps], color="red"
            #         )  # red dots - long term
            #         ax.set_title("Trend and Trend Change Points")
            #     else:
            #         ax.set_title("Trend")

            # change_x = my_bkps
            # change_y = res_data[my_bkps]

        elif method == "Dynp":
            pass
            # model = PARAMETERS["model"]
            # min_size = PARAMETERS["min_size"]
            # jump = PARAMETERS["jump"]
            # n_bkps = PARAMETERS["n_bkps"]
            # fit_data = PARAMETERS["fit_data"]

            # if model != "ar":
            #     algo = rpt.detection.Dynp(
            #         model=model, min_size=min_size, jump=jump
            #     ).fit(fit_data)
            # else:
            #     algo = rpt.detection.Dynp(
            #         model=model,
            #         min_size=min_size,
            #         jump=jump,
            #         params={"order": ar_order},
            #     ).fit(fit_data)

            # my_bkps = np.array(algo.predict(n_bkps=n_bkps))
            # my_bkps = my_bkps[:-1]  # the last one is always the last obs. remove

            # # plot
            # if figure:
            #     fig, ax = plt.subplots(figsize=figsize)
            #     if (
            #         frequency == "D"
            #         or frequency == "d"
            #         or frequency == "day"
            #         or frequency == "Day"
            #     ):
            #         loc = plticker.MultipleLocator(base=30)  # plot every 30 points
            #     else:
            #         loc = plticker.MultipleLocator(base=5)  # plot every 5 points
            #     ax.xaxis.set_major_locator(loc)
            #     ax.tick_params("x", rotation=90, labelsize=8)
            #     plot_x = DATE.strftime("%m/%d/%Y")
            #     ax.plot(plot_x, res_data, c="tab:blue")
            #     if lt_include_red_dots:
            #         ax.scatter(
            #             my_bkps, res_data[my_bkps], color="red"
            #         )  # red dots - long term
            #         ax.set_title("Trend and Trend Change Points")
            #     else:
            #         ax.set_title("Trend")

            # change_x = my_bkps
            # change_y = res_data[my_bkps]

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
                ax.tick_params("x", rotation=90, labelsize=8)
                plot_x = DATE.strftime("%m/%d/%Y")

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
                                    label="detected change points",
                                )
                            else:
                                ax.vlines(
                                    CX,
                                    np.min(res_data),
                                    np.max(res_data),
                                    color="k",
                                    alpha=0.4,
                                    ls="--",
                                    label="detected change points",
                                )
                            ax.scatter(
                                CX,
                                res_data[CX],
                                c="red",
                                label="detected change points",
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
                    "Anomaly Detection using BOCPD (both outlier and trend change points)"
                )

    elif data_type == "discrete":
        if method == "BOCPD":

            print("==========")

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
                if not anchor_point:
                    loc = plticker.MultipleLocator(base=5)  # plot every 5 points
                    ax.xaxis.set_major_locator(loc)
                ax.tick_params("x", rotation=90, labelsize=8)
                plot_x = DATE.strftime("%m/%d/%Y")

                for L in range(levels.shape[1]):
                    ax.plot(plot_x, levels[:, L], label="level " + str(L))
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
                                label="detected change points",
                            )
                            ax.scatter(
                                CX,
                                levels[CX, 0],
                                c="red",
                                label="detected change points",
                            )
                            for L in range(1, levels.shape[1]):
                                ax.vlines(CX, 0, 1, color="k", alpha=0.4, ls="--")
                                ax.scatter(CX, levels[CX, L], c="red")
                            LL += 1
                        else:
                            for L in range(levels.shape[1]):
                                ax.vlines(CdeX, 0, 1, color="k", alpha=0.4, ls="--")
                                ax.scatter(CX, levels[CX, L], c="red")

                if anchor_point:
                    ax.vlines(anchor_point, 0.1, 0.9, color="red", ls="dotted")
                    ax.text(
                        anchor_point, 0.1, "zoom_in_point", color="red", ha="center"
                    )
                ax.set_title("Categorical Data (all levels) and Change Points")
                ax.legend()

                for L in range(levels.shape[1]):
                    fig, ax = plt.subplots(figsize=figsize)
                    if not anchor_point:
                        loc = plticker.MultipleLocator(base=5)  # plot every 5 points
                        ax.xaxis.set_major_locator(loc)
                    ax.tick_params("x", rotation=90, labelsize=8)

                    plot_x = DATE.strftime("%m/%d/%Y")

                    ax.set_ylim(
                        np.max([0, np.min(levels[:, L]) - np.std(levels[:, L])]),
                        np.min([1, np.max(levels[:, L]) + np.std(levels[:, L])]),
                    )

                    ax.plot(
                        plot_x, levels[:, L], label="level " + str(L), color="black"
                    )
                    ax.scatter(plot_x, levels[:, L], color="black")

                    LL = 0
                    for CX in change_x:
                        if CX == 0:
                            continue
                        else:
                            if LL == 0:
                                ax.vlines(
                                    CX,
                                    np.max(
                                        [0, np.min(levels[:, L]) - np.std(levels[:, L])]
                                    ),
                                    np.min(
                                        [1, np.max(levels[:, L]) + np.std(levels[:, L])]
                                    ),
                                    color="k",
                                    alpha=0.4,
                                    ls="--",
                                    label="detected change points",
                                )
                                ax.scatter(
                                    CX,
                                    levels[CX, L],
                                    c="red",
                                    label="detected change points",
                                )
                                LL += 1
                            else:
                                ax.vlines(
                                    CX,
                                    np.max(
                                        [0, np.min(levels[:, L]) - np.std(levels[:, L])]
                                    ),
                                    np.min(
                                        [1, np.max(levels[:, L]) + np.std(levels[:, L])]
                                    ),
                                    color="k",
                                    alpha=0.4,
                                    ls="--",
                                )
                                ax.scatter(CX, levels[CX, L], c="red")
                    if anchor_point:
                        ax.vlines(
                            anchor_point,
                            np.max([0, np.min(levels[:, L]) - np.std(levels[:, L])]),
                            np.min([1, np.max(levels[:, L]) + np.std(levels[:, L])]),
                            color="red",
                            ls="dotted",
                        )
                        ax.text(
                            anchor_point,
                            np.median(levels[:, L]),
                            "zoom_in_point",
                            color="red",
                            ha="center",
                        )

                    # plot bootstrap prediction and CI
                    ax.plot(
                        plot_x[1:],
                        btCIs[L]["M"][1:],
                        color="blue",
                        alpha=0.4,
                        label="level " + str(L) + " bootstrap prediction median",
                    )
                    ax.plot(
                        plot_x[1:], btCIs[L]["L"][1:], color="pink", alpha=0.4, ls="--"
                    )
                    ax.plot(
                        plot_x[1:], btCIs[L]["U"][1:], color="pink", alpha=0.4, ls="--"
                    )
                    ax.fill_between(
                        plot_x[1:],
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
                    ax.legend()

        else:
            raise Exception("Only BOCPD is supported for discrete variables")

    return change_x, change_y, detected_lag


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


def similarity_score(string1: str, string2: str, ratio: float = 1.5) -> float:
    """
    Calculate the similarity between two strings on a per-character basis.

    Missing strings return a value of -1

    If the ratio of the length of the two strings is less than a specific value (1.5 default): use the Overlap coefficient
    Otherwise use Jaccard similarity

    Parameters:
    string1 (str):
    string2 (str):
    ratio (float): absolute ratio in length between the two strings to determine Overlap vs. Jaccard similarity

    Returns:
    float: Similarity score on the range between [0,1]. Values of -1 indicate a missing string.
    """
    if string1 is None or string2 is None:
        # Fail gracefully
        return -1.0

    if max(len(string1), len(string2)) / min(len(string1), len(string2)) < ratio:
        # Overlap similarity [0,1] on a per-character basis
        intersection = set(string1).intersection(set(string2))
        return len(intersection) / min([len(set(string1)), len(set(string2))])
    else:
        # Jaccard similarity [0,1] on a per-character basis
        intersection = set(string1).intersection(set(string2))
        union = set(string1).union(set(string2))
        return len(intersection) / len(union)
