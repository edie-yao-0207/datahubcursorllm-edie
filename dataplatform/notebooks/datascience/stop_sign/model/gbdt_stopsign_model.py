# Databricks notebook source

# COMMAND ----------

import json

import joblib
from sklearn.ensemble._gb import GradientBoostingClassifier, GradientBoostingRegressor

from lightgbm import LGBMClassifier


class GBDTStopSignModel:
    def __init__(self, model_dir):
        self.model_id = model_dir.split("/")[-2]
        self.model = joblib.load(model_dir + "model.joblib")
        with open(model_dir + "features.json", "r") as f:
            self.features = json.load(f)
        if type(self.model) in [GradientBoostingClassifier, LGBMClassifier]:
            self.model_type = "classifier"
        elif type(self.model) == GradientBoostingRegressor:
            self.model_type = "regressor"
        else:
            raise NotImplementedError("Model type is not supported", type(self.model))

    def predict(self, df):
        if self.model_type == "classifier":
            preds = self.model.predict_proba(df[self.features])[:, 1]
        elif self.model_type == "regressor":
            preds = self.model.predict(df[self.features])
        else:
            raise NotImplementedError(
                "Prediction for model type not supported", self.model_type
            )
        return preds
