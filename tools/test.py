import trino


def test_bridge(schema, trino_host, trino_port, trino_user, columns, rows):
    """
    It checks the data into the bridge are equal to the expected rows given as input
    :param schema: the name of the schema
    :param trino_host: host for connecting to trino
    :param trino_port: port for connecting to trino
    :param trino_user: username for connecting to trino
    :param columns: the columns to be analyzed (header of the rows parameter)
    :param rows: the rows to be analyzed
    :return: True if each row in rows is into the bridge table and the number of row in rows is equal to number of row
        into the bridge, False otherwise
    """

    retry = True

    while retry:
        try:
            with trino.dbapi.connect(host=trino_host, port=trino_port, user=trino_user, catalog='hive') as connection:
                cursor = connection.cursor()

                statement = f'select count(*) from hive.silver_{schema}.bridge'

                while retry:
                    try:
                        cursor.execute(statement)

                        retry = False
                    except trino.exceptions.TrinoQueryError:
                        retry = True

                counter = cursor.fetchall()[0][0]

                if counter != len(rows):
                    return False

                for row in rows:
                    statement = f'select {", ".join(columns)} from hive.silver_{schema}.bridge where '

                    conditions = []

                    for column, value in zip(columns, row):
                        if value is None:
                            conditions.append(f'{column} is NULL')
                        elif str(value).isdigit():
                            conditions.append(f'{column} = {value}')
                        else:
                            conditions.append(f'{column} = \'{value}\'')

                    statement += ' and '.join(conditions)

                    retry = True

                    while retry:
                        try:
                            cursor.execute(statement)

                            retry = False
                        except trino.exceptions.TrinoQueryError:
                            retry = True

                    result = cursor.fetchall()[0]

                    if result != row:
                        return False

                return True

        except trino.exceptions.TrinoConnectionError:
            retry = True
