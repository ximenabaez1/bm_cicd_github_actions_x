from snowflake.snowpark.session import Session
import os
import json

my_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
connection_parameters = json.load(open(os.path.join(my_dir, 'creds.json')))
sess = Session.builder.configs(connection_parameters).create()

def do_evaluation():
    print("STORE PROCEDURE QUE REPRESENTA EVALUACION DEL MODELO")


sproc = sess.sproc.register(func=do_evaluation,
                                  name='evaluation_step',
                                  is_permanent=True,
                                  replace=True,
                                  stage_location='@PYTHON_FILES',
                                  packages=['snowflake-ml-python',
                                            'snowflake-snowpark-python'
                                           ])


