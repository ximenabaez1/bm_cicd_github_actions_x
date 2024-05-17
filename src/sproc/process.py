from snowflake.snowpark.session import Session
import os
import json

my_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
connection_parameters = json.load(open(os.path.join(my_dir, 'creds.json')))
sess = Session.builder.configs(connection_parameters).create()

def do_processing():
    print("STORE PROCEDURE QUE REPRESENTA PROCESAMIENTO")


sproc = sess.sproc.register(func=do_processing,
                                  name='processing_step',
                                  is_permanent=True,
                                  replace=True,
                                  stage_location='@PYTHON_FILES',
                                  packages=['snowflake-ml-python',
                                            'snowflake-snowpark-python'
                                           ])


