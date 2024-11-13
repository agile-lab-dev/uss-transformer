import duckdb
import os
import sqlglot
import sqlparse
import subprocess


def retrieve_tables(statements):
    """
    It allows to retrieve the name of the schema and the structure of each tables by parsing sql statements
    :param statements: 'create table' statements
    :return: the name of the schema and the structure of each tables
    """

    schema = 'default'
    tables = {}

    for statement in statements:
        index = statement.find('create table')

        statement = statement[index:].split('\n')
        statement = list(map(str.strip, statement))

        schema, table = statement[0].split()[2].split('.')

        tables[table] = {}

        for line in statement[1:-1]:
            words = line.split()
            column = words[0]
            datatype = (' '.join(words[1:])).rstrip(',').replace('not null', '').strip()
            tables[table][column] = {'datatype': datatype}

    return schema, tables


def retrieve_schema(sql):
    """
    It allows to retrieve the schema by parsing sql
    :param sql: statements to format
    :return: the name of the schema and the structure of each tables
    """

    statements = []

    sql = sqlparse.format(
        sql,
        keyword_case='lower',
        identifier_case='lower',
        strip_comments=True,
        use_space_around_operators=True
    )

    sql = sqlparse.split(sql, strip_semicolon=True)

    for statement in sql:
        if 'create table' in statement:
            statements.append(statement)

    schema, tables = retrieve_tables(statements)

    return schema, tables


def export_parquet_from_postgres(schema, tables, dbname, host, port, user, password):
    """
    It allows to export the data from a schema in postgres into parquet files
    :param schema: the name of the schema
    :param tables: the structure of all tables into the schema
    :param dbname: the database name for connecting to postgres by duckdb
    :param host: the host name for connecting to postgres by duckdb
    :param port: the port for connecting to postgres by duckdb
    :param user: the username for connecting to postgres by duckdb
    :param password: the password for connecting to postgres by duckdb
    """

    duckdb.sql('INSTALL postgres;')
    duckdb.sql('LOAD postgres;')

    parameters = f'dbname={dbname} host={host} port={port} user={user} password={password} '

    duckdb.sql(f"ATTACH '{parameters}' AS postgres_db (TYPE POSTGRES);")

    for table in tables.keys():
        parquet_path = os.path.join('parquet', 'bronze', schema, table)

        if not os.path.exists(parquet_path):
            command = f'mkdir -p {parquet_path}'
            subprocess.run(command.split())

        filename = table + '.parquet'
        duckdb.sql(f'COPY postgres_db.{schema}.{table} TO {filename} (FORMAT PARQUET)')

        command = f'mv {filename} {parquet_path}'
        subprocess.run(command.split())


def generate_sql(schema, tables, trino_setup_file):
    """
    It generates a sql file which contains statements for recreating the schema retrieved from postgres into trino
    :param schema: the name of the schema
    :param tables: the structure of all tables into the schema
    :param trino_setup_file: output file path
    """

    catalog = 'hive'
    bucket = 'datalake'
    metal = 'bronze'
    schema_path = f's3://{bucket}/{metal}/{schema}'

    with open(trino_setup_file, 'w') as script:
        query = f"CREATE SCHEMA {catalog}.{metal}_{schema} WITH (location = '{schema_path}/');\n\n"
        script.write(query)

        for table, columns in tables.items():
            query = f'CREATE TABLE {catalog}.{metal}_{schema}.{table} ('

            for column, attributes in columns.items():
                query += f"{column} {attributes['datatype']}, "

            query = query.rstrip(', ') + ')'

            translated = sqlglot.transpile(query, read="postgres", write="trino")[0]
            translated += f"\nWITH (external_location='{schema_path}/{table}/', format='PARQUET');\n\n"

            script.write(translated)
