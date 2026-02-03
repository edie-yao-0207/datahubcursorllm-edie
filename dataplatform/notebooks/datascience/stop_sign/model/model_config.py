# Databricks notebook source

# COMMAND ----------
import json


class ModelConfig:
    def __init__(self):
        self.MODEL_ID = "1621983188983"
        self.MODEL_DIR = f"/dbfs/mnt/samsara-databricks-playground/stop_sign_detection/models/gbdt/{self.MODEL_ID}/"
        self.MODEL_THRESHOLD = 0.975

    def json(self):
        return json.dumps(self.__dict__)
