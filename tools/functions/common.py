import trino


def trino_execute(trino_host, trino_port, trino_user, trino_sql_file):
    """
    It allows to execute statements in trino
    :param trino_host: host for connecting to trino
    :param trino_port: port for connecting to trino
    :param trino_user: username for connecting to trino
    :param trino_sql_file: statements to be executed
    """
    retry = True

    while retry:
        try:
            with trino.dbapi.connect(host=trino_host, port=trino_port, user=trino_user, catalog='hive') as connection:
                cursor = connection.cursor()

                with open(trino_sql_file) as sql:
                    statements = sql.read().split(';')[:-1]

                    while retry:
                        try:
                            for statement in statements:
                                cursor.execute(statement)

                            retry = False
                        except trino.exceptions.TrinoQueryError:
                            retry = True

        except trino.exceptions.TrinoConnectionError:
            retry = True
