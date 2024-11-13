import sqlparse


def retrieve_tables(statements):
    """
    It retrieves the name of the schema and the structure of each tables by parsing sql statements
    :param statements: 'create table' statements
    :return: the name of the schema and the structure of each tables
    """

    schema = 'default'
    tables = {}

    for statement in statements:
        start_index = statement.find('create table')
        end_index = statement.find(')\nwith')

        statement = statement[start_index:end_index].split('\n')
        statement = list(map(str.strip, statement))

        catalog, schema, table = statement[0].split()[2].split('.')

        schema = schema.replace('bronze_', '')

        tables[table] = {}

        for line in statement[1:-1]:
            words = line.split()
            column = words[0]
            datatype = (' '.join(words[1:])).rstrip(',')
            tables[table][column] = {'datatype': datatype, 'primary_key': False}

    return schema, tables


def retrieve_primary_keys(statements, tables):
    """
    It retrieves which columns are primary keys
    :param statements: sql statements to be parsed
    :param tables: the structure of all tables into the schema
    """

    for statement in statements:
        statement = statement.replace('primary key ', '')

        table, columns = statement.split('(')
        columns = columns.rstrip(')')
        columns = list(map(str.strip, columns.split(',')))

        for column in columns:
            tables[table][column]['primary_key'] = True


def retrieve_foreign_keys(statements):
    """
    It retrieves which columns are foreign keys and which columns are referenced
    :param statements: sql statements to be parsed
    :return: a list of dictionaries, where a dictionary describes a foreign key
    """

    foreign_keys = []

    for statement in statements:
        statement = statement.replace('foreign key ', '')

        index = statement.find('references')

        table, columns = statement[:index - 1].split('(')
        columns = columns.rstrip(')')
        columns = list(map(str.strip, columns.split(',')))

        statement = statement[index:].replace('references ', '')
        referenced_table, referenced_columns = statement.split('(')
        referenced_columns = referenced_columns.rstrip(')')
        referenced_columns = list(map(str.strip, referenced_columns.split(',')))

        if table != referenced_table:
            foreign_key = {
                'table': table,
                'columns': columns,
                'referenced_table': referenced_table,
                'referenced_columns': referenced_columns,
            }

            foreign_keys.append(foreign_key)

    return foreign_keys


def retrieve_links(tables, foreign_keys):
    """
    It retrieves the links between tables by analyzing the foreign keys
    :param tables: the structure of the tables into the schema
    :param foreign_keys: the foreign keys into the schema
    :return: a dictionary which contains, for each table, a list of tables which it is linked to
    """

    links = {}

    for table in tables.keys():
        links[table] = set()

        for foreign_key in foreign_keys:
            if foreign_key['table'] == table:
                links[table].add(foreign_key['referenced_table'])

    changes = True

    while changes:
        changes = False

        for table, referenced_tables in links.items():
            new_referenced_tables = set()

            for referenced_table in referenced_tables:
                new_referenced_tables.update(links[referenced_table])

            if not changes and new_referenced_tables - referenced_tables:
                changes = True

            referenced_tables.update(new_referenced_tables)
            referenced_tables.discard(table)

    return links


def retrieve_schema(sql):
    """
    It retrieves details about a schema by parsing a sql dump
    :param sql: statements to be parsed
    :return: the name of the schema, the structure of all tables into the schema, the foreign keys and the links
    """

    statements = {
        'create_table': [],
        'primary_key': [],
        'foreign_key': []
    }

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
            statements['create_table'].append(statement)
        elif 'primary key' in statement:
            statements['primary_key'].append(statement)
        elif 'foreign key' in statement:
            statements['foreign_key'].append(statement)

    schema, tables = retrieve_tables(statements['create_table'])

    retrieve_primary_keys(statements['primary_key'], tables)

    foreign_keys = retrieve_foreign_keys(statements['foreign_key'])

    links = retrieve_links(tables, foreign_keys)

    return schema, tables, foreign_keys, links


def generate_uss_schema(tables):
    """
    It generates the structure of the uss schema by modifying the original structure
    :param tables: the structure of all tables into the original schema
    :return: the structure of the uss schema
    """

    uss_tables = tables.copy()
    max_length = 0

    bridge = {
        'stage': {
            'datatype': '',
            'primary_key': False
        }
    }

    for table, columns in tables.items():
        new_column = '_key_' + table
        primary_keys = []
        max_length = max(max_length, len(table))

        for column, attributes in columns.items():
            if attributes['primary_key']:
                primary_keys.append(column)

        if len(primary_keys) == 0:
            datatype = 'smallint' # to store the value returned by row_number()
            # datatype = 'varbinary' # to store the value returned by cast(uuid() as varbinary)
        elif len(primary_keys) == 1:
            datatype = uss_tables[table][primary_keys[0]]['datatype']
        else:
            datatype = 'bytea'

        bridge[new_column] = {
            'datatype': datatype.replace('not null', '').strip(),
            'primary_key': False
        }

        uss_tables[table][new_column] = {
            'datatype': datatype,
            'primary_key': True
        }

    bridge['stage']['datatype'] = f'character varying({max_length})'

    uss_tables['bridge'] = bridge

    return uss_tables


def create_tables_no_bridge(catalog, schema, uss_tables):
    """
    It creates all uss tables, except the bridge table
    :param catalog: the name of the catalog in trino
    :param schema: the name of the schema
    :param uss_tables: the structure of all tables into the uss schema
    :return: queries which allow to create all the tables, except the bridge, into trino
    """

    queries = ''
    bucket = 'datalake'
    uss_metal = 'silver'
    original_metal = 'bronze'

    schema_path = f's3://{bucket}/{uss_metal}/{schema}'

    query = f"create schema if not exists {catalog}.{uss_metal}_{schema} with (location = '{schema_path}/');\n\n"
    queries += query

    for table, columns in uss_tables.items():
        if table != 'bridge':
            primary_keys = []

            query = f'create table if not exists {catalog}.{uss_metal}_{schema}.{table} ('

            for column, attributes in columns.items():
                query += f'{column}, '

                if '_key_' not in column:
                    if attributes['primary_key']:
                        primary_keys.append(column)

            query = query.rstrip(', ') + ')'
            query += f"\nwith (external_location='{schema_path}/{table}/', format='parquet')"
            query += f'\nas'
            query += f'\nselect *, '

            if len(primary_keys) == 0:
                query += f'row_number() over (order by {list(columns.keys())[0]})' # less efficient, but deterministic
                # query = 'cast(uuid() as varbinary)' # more efficient, but not deterministic
            elif len(primary_keys) == 1:
                query += f'{primary_keys[0]}'
            else:
                array = 'ARRAY['

                for pk in primary_keys:
                    array += f'cast({pk} as varchar), '

                array = array.rstrip(', ') + ']'

                query += f"sha256(cast(array_join({array}, '') as varbinary))"

            query += f'\nfrom {catalog}.{original_metal}_{schema}.{table};\n\n'

            queries += query

    return queries


def get_on_columns(catalog, schema, foreign_key):
    """
    It gets the part of the query about the left join and the on condition
    :param catalog: the name of the catalog in trino
    :param schema: the name of the schema
    :param foreign_key: the dictionary which describes a foreign key
    :return: the part of the query about the left join and the on condition
    """

    metal = 'silver'
    table = f"{catalog}.{metal}_{schema}.{foreign_key['referenced_table']}"
    columns = map(lambda column: foreign_key['table'] + '.' + column, foreign_key['columns'])
    first_column = f"({', '.join(columns)})"
    columns = map(lambda column: foreign_key['referenced_table'] + '.' + column, foreign_key['referenced_columns'])
    second_column = f"({', '.join(columns)})"
    left_join_query = f'\nleft join {table}'
    left_join_query += f'\non {first_column} = {second_column}'

    return left_join_query


def get_left_join_query(foreign_keys, links, catalog, schema, stage):
    """
    It gets all the left joins which build the query for creating the bridge table about a specific table (stage)
    :param foreign_keys: the list of dictionaries, where each dictionary describes a foreign key
    :param links: a dictionary which contains, for each table, a list of tables which it is linked to
    :param catalog: the name of the catalog in trino
    :param schema: the name of the schema
    :param stage: the specific table for which is generated the left join query
    :return: the left join query to add into the query for creating the bridge table
    """

    left_join_query = ''
    already_joined = set()

    for foreign_key in foreign_keys:
        if foreign_key['table'] == stage:
            already_joined.add(foreign_key['referenced_table'])
            left_join_query += get_on_columns(catalog, schema, foreign_key)

    while len(links[stage] - already_joined) > 0:
        joined = set()

        for table in links[stage] - already_joined:
            for foreign_key in foreign_keys:
                if foreign_key['table'] in already_joined:
                    if table == foreign_key['referenced_table']:
                        joined.add(table)
                        left_join_query += get_on_columns(catalog, schema, foreign_key)

        already_joined = already_joined.union(joined)

    return left_join_query


def create_bridge(catalog, schema, uss_tables, foreign_keys, links):
    """
    It generates the query for creating the bridge table
    :param catalog: the name of the catalog in trino
    :param schema: the name of the schema
    :param uss_tables: the structure of all tables into the uss schema
    :param foreign_keys: the foreign keys of the schema
    :param links: the links among the tables
    :return: the query for creating the bridge table
    """

    query = ''
    bucket = 'datalake'
    metal = 'silver'

    table_names = uss_tables.keys()

    for table, columns in uss_tables.items():
        if table == 'bridge':
            query = f'create table if not exists {catalog}.{metal}_{schema}.{table} ('

            for column in columns.keys():
                query += f'{column}, '

            query = query.rstrip(', ') + ')'
            query += f"\nwith (external_location='s3://{bucket}/{metal}/{schema}/{table}/', format='parquet')"
            query += f'\nas'

            for stage in table_names:
                if stage != 'bridge':
                    query += f"\nselect '{stage}', "

                    for column in columns.keys():
                        if column != 'stage':
                            if column == '_key_' + stage:
                                query += f'{column}, '
                            elif column[5:] not in links[stage]:
                                query += 'null, '
                            else:
                                query += f'{column}, '

                    query = query.rstrip(', ')
                    left_join_query = get_left_join_query(foreign_keys, links, catalog, schema, stage)
                    query += f'\nfrom {catalog}.{metal}_{schema}.{stage}' + left_join_query
                    query += f'\nunion'

            query = query.rstrip('\nunion') + ';\n\n'

    return query


def generate_sql(schema, uss_tables, foreign_keys, links, trino_transformation_file):
    """
    It generates a sql file which allows to create the uss schema in trino
    :param schema: the name of the original schema
    :param uss_tables: the structure of all tables into the uss schema
    :param foreign_keys: the foreign keys of the schema
    :param links: the links among tables
    :param trino_transformation_file: the output file path
    """

    catalog = 'hive'

    with open(trino_transformation_file, 'w') as script:
        queries = create_tables_no_bridge(catalog, schema, uss_tables)
        script.write(queries)
        query = create_bridge(catalog, schema, uss_tables, foreign_keys, links)
        script.write(query)


def generate_full_select_query(schema, uss_tables, trino_full_select_query_file):
    """
    It generates the select query which allows to join each table with the bridge
    :param schema: the name of the schema
    :param uss_tables: the structure of all tables into the uss schema
    :param trino_full_select_query_file: the output file path
    :return:
    """

    with open(trino_full_select_query_file, 'w') as script:
        catalog = 'hive'
        metal = 'silver'

        query = 'select *'
        query += f'\nfrom {catalog}.{metal}_{schema}.bridge'

        for table in uss_tables.keys():
            if table != 'bridge':
                column = f'_key_{table}'
                query += f'\nleft join {catalog}.{metal}_{schema}.{table}'
                query += f'\non bridge.{column} = {table}.{column}'

        query += ';\n\n'

        script.write(query)
