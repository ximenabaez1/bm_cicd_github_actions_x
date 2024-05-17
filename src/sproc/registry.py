from snowflake.snowpark.session import Session
import os
import json

my_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
connection_parameters = json.load(open(os.path.join(my_dir, 'creds.json')))
sess = Session.builder.configs(connection_parameters).create()

def model_registry():
    print("STORE PROCEDURE QUE REPRESENTA EL REGISTRO DEL MODELO")


sproc = sess.sproc.register(func=model_registry,
                                  name='model_registry_step',
                                  is_permanent=True,
                                  replace=True,
                                  stage_location='@PYTHON_FILES',
                                  packages=['snowflake-ml-python',
                                            'snowflake-snowpark-python'
                                           ])


