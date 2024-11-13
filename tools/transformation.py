import os
import subprocess
import trino

from tools import functions


def trino_dump(schema, trino_host, trino_port, trino_user, trino_dump_file):
    """
    It creates a dump file of a schema from trino
    :param schema: the name of the schema to be analyzed
    :param trino_host: host for connecting to trino
    :param trino_port: port for connecting to trino
    :param trino_user: username for connecting to trino
    :param trino_dump_file: the output path file
    """

    retry = True

    while retry:
        try:
            with trino.dbapi.connect(host=trino_host, port=trino_port, user=trino_user, catalog='hive') as connection:
                cursor = connection.cursor()

                statement = f'show tables from hive.bronze_{schema}'

                while retry:
                    try:
                        cursor.execute(statement)

                        retry = False
                    except trino.exceptions.TrinoQueryError:
                        retry = True

                rows = cursor.fetchall()

                tables = map(lambda row: row[0], rows)

                rows = []

                for table in tables:
                    statement = f'show create table hive.bronze_{schema}.{table}'

                    retry = True

                    while retry:
                        try:
                            cursor.execute(statement)

                            retry = False
                        except trino.exceptions.TrinoQueryError:
                            retry = True

                    rows.append(cursor.fetchall())

                create_tables = map(lambda row: row[0][0], rows)

                with open(trino_dump_file, 'w') as dump:
                    for create_table in create_tables:
                        dump.write(create_table + ';\n')

        except trino.exceptions.TrinoConnectionError:
            retry = True


def uss_transformation(trino_dump_file, notes_file, trino_transformation_file, trino_full_select_query_file):
    """
    It creates the uss schema from a given trino dump file and a notes file
    :param trino_dump_file: dump of the schema in trino to be analyzed
    :param notes_file: notes about the primary keys and foreign keys of the schema
    :param trino_transformation_file: output path file for the queries to be executed for an effective transformation
    :param trino_full_select_query_file: output path file where the full select query is stored
    """

    with open(trino_dump_file) as dump:
        sql = dump.read()

        with open(notes_file) as notes:
            sql += '\n' + notes.read()

        schema, tables, foreign_keys, links = functions.transformation.retrieve_schema(sql)

        uss_tables = functions.transformation.generate_uss_schema(tables)

        functions.transformation.generate_sql(schema, uss_tables, foreign_keys, links, trino_transformation_file)

        functions.transformation.generate_full_select_query(schema, uss_tables, trino_full_select_query_file)


def transformation(schema, trino_host, trino_port, trino_user, notes_file):
    """
    It executes the effective uss transformation of a schema in trino
    :param schema: the name of the schema
    :param trino_host: host for connecting to trino
    :param trino_port: port for connecting to trino
    :param trino_user: username for connecting to trino
    :param notes_file: notes about the primary keys and foreign keys of the schema
    """

    sql_directory = 'sql'
    trino_dump_file = os.path.join(sql_directory, 'trino_dump.sql')
    trino_transformation_file = os.path.join(sql_directory, 'trino_transformation.sql')
    trino_full_select_query_file = os.path.join(sql_directory, 'trino_full_select_query.sql')

    commands = [
        'sudo -S docker compose up -d',
        f'mkdir -p {sql_directory}'
    ]

    for command in commands:
        subprocess.run(command.split())

    trino_dump(schema, trino_host, trino_port, trino_user, trino_dump_file)

    uss_transformation(trino_dump_file, notes_file, trino_transformation_file, trino_full_select_query_file)

    functions.common.trino_execute(trino_host, trino_port, trino_user, trino_transformation_file)
