# Import libraries
import warnings
import itertools
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import statsmodels.api as sm

class ARIMA():
    def __init__(self):
        self.AICList=[]
        self.ARIMAX_model_list=[]
        self.predList=[]
        # Define the q parameters to take any value between 0 and 1
        q = range(0, 2)
        d = range(0, 1)
        # Define the p parameters to take any value between 0 and 3
        p = range(0, 4)
        # Generate all different combinations of p, q and q triplets
        self.pdq = list(itertools.product(p, d, q))

    def AICnARIMAX(self,train):
        warnings.filterwarnings("ignore") # specify to ignore warning messages
        self.AICList=[]
        self.ARIMAX_model_list=[]
        for i in range(len(train.columns)):
            train_data_temp=train.iloc[:,i]
            AIC = []
            ARIMAX_model = []
            for param in self.pdq:
                    try:
                        mod = sm.tsa.arima.ARIMA(train_data_temp,
                                                       order=param,
                                                       enforce_stationarity=False,
                                                       enforce_invertibility=False)

                        results = mod.fit() #HERE
                        AIC.append(results.aic)
                        ARIMAX_model.append([param])
                    except:
                        print("Error")
            self.AICList.append(AIC)
            self.ARIMAX_model_list.append(ARIMAX_model)

# Fit this model
    def pred(self,train):
        self.predList=[]
        for i in range(len(train.columns)):
            train_data_temp = train.iloc[:, i].to_frame()
            ARIMAX_model_temp=self.ARIMAX_model_list[i]
            AIC_temp=self.AICList[i]
            mod = sm.tsa.arima.ARIMA(train_data_temp,
                                           order=ARIMAX_model_temp[AIC_temp.index(min(AIC_temp))][0],
                                           enforce_stationarity=True,
                                           enforce_invertibility=False)
            results = mod.fit()
            pred = results.get_prediction(start=-1,dynamic=False) # 1-step ahead forecast
            # pred = results.get_prediction(start='1958-01-01', dynamic=True) # predict last year data
            # pred = results.get_forecast(ForecastTillDate) # forecast
            predList_temp=pred.predicted_mean.values.tolist()
            self.predList.append(predList_temp)
        return self.predList

    def resid(self,train):
        self.predList=[]
        for i in range(len(train.columns)):
            train_data_temp = train.iloc[:, i]
            ARIMAX_model_temp=self.ARIMAX_model_list[i]
            AIC_temp=self.AICList[i]
            mod = sm.tsa.arima.ARIMA(train_data_temp,
                                           order=ARIMAX_model_temp[AIC_temp.index(min(AIC_temp))][0],
                                           enforce_stationarity=False,
                                           enforce_invertibility=False)
            results = mod.fit()
            pred = results.resid # Get residual value
            self.predList.append(pred)
        return self.predList