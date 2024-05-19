import os

def main():
    # Obtener los valores de los secrets de GitHub
    db_name = os.environ.get('SNOWSQL_DATABASE')
    schema_name = os.environ.get('SNOWFLAKE_SCHEMA_DEV')

    # Leer el script SQL
    with open('query_prueba.sql', 'r') as file:
        sql_script = file.read()

    # Sustituir las variables de entorno en el script SQL
    sql_script = sql_script.replace('$DB_NAME$', db_name)
    sql_script = sql_script.replace('$SCHEMA$', schema_name)

    # Ejecutar el script SQL
    # Aquí puedes usar Snowpark o cualquier otra biblioteca que prefieras para ejecutar el script SQL

if __name__ == "__main__":
    main()