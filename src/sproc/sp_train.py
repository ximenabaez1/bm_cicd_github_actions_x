from snowflake.snowpark.session import Session
from snowflake.snowpark import DataFrame
import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F
from snowflake.ml.modeling.xgboost import XGBClassifier

from typing import *
import json
import pandas as pd
import warnings
from typing  import *
import logging
warnings.filterwarnings("ignore")
import configparser
import sys
import os
import joblib
import cachetools

config = configparser.ConfigParser()
config_path = os.path.expanduser("~/.snowsql/config") 
config.read(config_path)

# Access the values using the section and key
# Assuming the values you want are in the "connections.dev" section
dict_creds = {}

#Se comenta esta linea de codigo para usar el json con credenciales dentro del proyecto
dict_creds['account'] = config['connections.dev']['accountname']
dict_creds['user'] = config['connections.dev']['username']
dict_creds['password'] = config['connections.dev']['password']
dict_creds['role'] = config['connections.dev']['rolename']
dict_creds['database'] = config['connections.dev']['dbname']
dict_creds['warehouse'] = config['connections.dev']['warehousename']
dict_creds['schema'] = config['connections.dev']['schemaname']

session = Session.builder.configs(dict_creds).create()
session.use_database(dict_creds['database'])
session.use_schema(dict_creds['schema'])


#Functions
def train_model(session: Session, db_name: str, schema_name: str, train_table: str) -> XGBClassifier:
    session.use_database(db_name)
    session.use_schema(schema_name)
    df_train_banana = session.table(train_table)
    feature_cols = df_train_banana.columns
    feature_cols.remove('QUALITY')
    target_col = 'QUALITY'
    xgbmodel = XGBClassifier(random_state=123, input_cols=feature_cols, label_cols=target_col, output_cols='PREDICTION')
    xgbmodel.fit(df_train_banana)
    return xgbmodel

 

def main(sess: Session) -> T.Variant:

    xgbmodel = train_model(sess, "BANANA_QUALITY", "DEV", "BANANA_TRAIN")
    xgb_file = xgbmodel.to_xgboost()
    MODEL_FILE = 'model.joblib.gz'
    joblib.dump(xgb_file, MODEL_FILE)
    sess.file.put(MODEL_FILE, "@DEV.ML_MODELS", auto_compress=False, overwrite=True)

    return str(f'El entrenamiento se realizó de manera exitosa')


sproc = session.sproc.register(func=main,
                                  name='train_step',
                                  is_permanent=True,
                                  replace=True,
                                  stage_location='@BANANA_QUALITY.DEV.ML_MODELS',
                                  packages=['snowflake-ml-python',
                                            'snowflake-snowpark-python'
                                           ])


    


 
 

    


