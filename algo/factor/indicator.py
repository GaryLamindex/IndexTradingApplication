from __future__ import (absolute_import, division, print_function, unicode_literals)
import numpy as np
import pandas as pd
from sklearn.decomposition import PCA
from pykalman import KalmanFilter
from arch.univariate import arch_model

import algo.factor.modellib.DCC as DCC
import algo.factor.modellib.ARIMA as ARIMA
from algo.factor.modellib.covariance_matrix import covariance_matrix as cm

class Indicator:

    def __init__(self, indices, explained_variance=0.8):
        self.expected_return = np.array([])
        self.expected_cov = np.array([])
        self.indices = indices
        self.explained_variance = explained_variance
        # self.indices_return = indice.pct_change().iloc[1:]  # Percentage return
        self.indices_return = (np.log(indices) - np.log(indices.shift(1))).iloc[1:]  # Maybe use log return idk

    def get_params(self):
        # PCA
        factors = pd.DataFrame()
        data_array = self.indices_return.to_numpy()
        pca = PCA(n_components=self.explained_variance)
        pca.fit(data_array)
        eigenvectors = pca.components_
        j = 0
        for eigenvec in eigenvectors:
            factors[j] = np.dot(self.indices_return, eigenvec)
            j += 1

        # ARMA for 1-period forecast of PCs
        factor_preds = []
        factor_resids = []
        for i in range(factors.shape[1]):
            factor = factors.iloc[:, i].to_frame()
            arima = ARIMA.ARIMA()
            arima.AICnARIMAX(factor)
            factor_pred = arima.pred(factor)
            factor_resid = arima.resid(factor)[0].to_numpy()
            factor_preds.append(factor_pred)
            factor_resids.append(factor_resid)
        factor_resids = np.array(factor_resids)

        # Kalman filter for obtaining betas
        all_beta_past = []
        all_beta_mean = []
        all_beta_cov = []
        for idv_return in self.indices_return.T.values:
            transition_matrix = np.identity(factors.shape[1] + 1)
            observation_matrix = np.concatenate((np.ones((factors.shape[0], 1)), factors.to_numpy()), axis=1)\
                                 .reshape(factors.shape[0], 1, factors.shape[1] + 1)
            transition_offset = np.zeros(factors.shape[1] + 1)
            observation_offset = np.array([0])
            kf = KalmanFilter(transition_matrices=transition_matrix,
                              observation_matrices=observation_matrix,
                              transition_offsets=transition_offset,
                              observation_offsets=observation_offset,
                              em_vars=['transition_covariance',
                                       'observation_covariance',
                                       'initial_state_mean',
                                       'initial_state_covariance'],
                              n_dim_state=factors.shape[1] + 1,
                              n_dim_obs=1)
            beta_mean, _ = kf.em(idv_return, n_iter=5).smooth(idv_return)
            all_beta_past.append(beta_mean)
            all_beta_mean.append(beta_mean[-1])
            beta_cov = np.cov(beta_mean, rowvar=False)
            all_beta_cov.append(beta_cov)
        all_beta_past, all_beta_mean, all_beta_cov = np.array(all_beta_past), np.array(all_beta_mean), np.array(all_beta_cov)

        # DCC-garch for covariance matrix between PCs
        dcc = DCC.DCC()
        dccfit = dcc.fit(factor_resids)
        factor_cov = dccfit.forecast()

        # Variance for residual of returns
        past_expected_returns = []
        adj_factors = np.insert(factors.to_numpy(), 0, 1, axis=1)
        for i in range(all_beta_past.shape[0]):
            past_expected_return = np.sum(all_beta_past[i] * adj_factors, axis=1)
            past_expected_returns.append(past_expected_return)
        past_expected_returns = np.array(past_expected_returns).T
        return_residual = self.indices_return.to_numpy() - past_expected_returns
        predicted_vars = []
        for i in range(return_residual.shape[1]):
            garch = arch_model(return_residual[:, i], vol='GARCH', p=1, o=0, q=1)
            garch_fitted = garch.fit(update_freq=0, disp='off')
            garch_forecast = garch_fitted.forecast(horizon=1)
            predicted_var = garch_forecast.variance['h.1'].iloc[-1]
            predicted_vars.append(predicted_var)
        predicted_vars = np.array(predicted_vars)

        factor_preds = [factor_preds[i][0][0] for i in range(len(factor_preds))]
        factor_preds.insert(0, 1)
        self.expected_return = np.dot(all_beta_mean, factor_preds)
        self.expected_cov = cm(self.expected_return, all_beta_cov, all_beta_mean, factor_cov, factor_preds[1:], predicted_vars)
        print(self.expected_return)