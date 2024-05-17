from snowflake.snowpark.session import Session
import snowflake.snowpark.types as T
import os
import json
import configparser
import sys
import os

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

def do_inference(session: Session)-> T.Variant:
    print("STORE PROCEDURE QUE REPRESENTA LA REALIZACION DE INFERENCIAS")


sproc = session.sproc.register(func=do_inference,
                                  name='inference_step',
                                  is_permanent=True,
                                  replace=True,
                                  stage_location='@PYTHON_FILES',
                                  packages=['snowflake-ml-python',
                                            'snowflake-snowpark-python'
                                           ])


