import os
from snowflake import connector

def main():
    # Obtener los valores de los secrets de GitHub
    account = os.environ.get('SNOWSQL_ACCOUNT')
    user = os.environ.get('SNOWSQL_USER')
    password = os.environ.get('SNOWSQL_PWD')
    db_name = os.environ.get('SNOWSQL_DATABASE')
    schema_name = os.environ.get('SNOWFLAKE_SCHEMA_DEV')

    file_path = os.path.join(os.environ['GITHUB_WORKSPACE'], 'src/tasks/query_prueba.sql')
    print(file_path)

    # Leer el script SQL
    with open(file_path, 'r') as file:
        sql_script = file.read()

    # Sustituir las variables de entorno en el script SQL
    sql_script = sql_script.replace('$DB_NAME$', db_name)
    sql_script = sql_script.replace('$SCHEMA$', schema_name)

    # Separar las declaraciones SQL por punto y coma
    sql_statements = sql_script.split(';')

    conn = connector.connect(
        user=user,
        password=password,
        account=account,
        database=db_name,
        schema=schema_name
    )

       # Ejecutar cada declaración SQL por separado
    with conn.cursor() as cursor:
        for statement in sql_statements:
            if statement.strip():  # Ignorar líneas en blanco
                cursor.execute(statement)

    # with conn.cursor() as cursor:
    #     cursor.execute(sql_script)

    # Cerrar la conexión
    conn.close()



    # Ejecutar el script SQL
    # Aquí puedes usar Snowpark o cualquier otra biblioteca que prefieras para ejecutar el script SQL

if __name__ == "__main__":
    main()