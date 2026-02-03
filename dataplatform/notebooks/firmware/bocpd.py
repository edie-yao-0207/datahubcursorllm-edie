# Copied modified version of data analysis library from Data Science & Services
import numpy as np
import pandas as pd
import scipy.stats as ss

# COMMAND ----------


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


# COMMAND ----------


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


# COMMAND ----------


# COMMAND ----------


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


# COMMAND ----------

# bootstrap CI for post. pred. CI
# default, use 1000 bootstrap samples

# we have a mixture of Dirichlet-multinomial (DM) distribution
# For each bootstrap sample, DO:
# (1) randomly select one Dirichlet-multinomial distribution according to the weight w_l
# (2) generate from that Dirichlet-multinomial distribution
# (3) construct CI using the quantiles of drawn samples


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


# COMMAND ----------

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


# COMMAND ----------

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


# COMMAND ----------


class multinomial_discrete:  # prior is Gaussian with only unknown mean
    def __init__(self, levels):
        self.levels = levels
        self.alpha0 = self.alpha = np.array(
            [[1 / self.levels for _ in range(levels)]]
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


# COMMAND ----------

# hazard_functions
def constant_hazard(lam, r):
    """
    Hazard function for bayesian online learning
    Arguments:
        lam - inital prob
        r - R matrix
    """
    return 1 / lam * np.ones(r.shape)


# COMMAND ----------


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


# COMMAND ----------


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


# COMMAND ----------


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
