import os
import subprocess

from minio import Minio

from tools import functions


def prepare_trino_setup(pg_dump_file, dbname, pg_host, pg_port, pg_user, pg_password, trino_setup_file):
    """
    It retrieves the schema from postgres, exports the data into parquet file, and generates the sql for trino
    :param pg_dump_file: the output file path of the postgres dump
    :param dbname: the name of the database for connecting to postgres
    :param pg_host: the host name for connecting to postgres
    :param pg_port: the port for connecting to postgres
    :param pg_user: the username for connecting to postgres
    :param pg_password: the password for connecting to postgres
    :param trino_setup_file: the output file path where statements to set up trino are written
    """

    with open(pg_dump_file) as dump:
        sql = dump.read()
        schema, tables = functions.setup.retrieve_schema(sql)

        functions.setup.export_parquet_from_postgres(schema, tables, dbname, pg_host, pg_port, pg_user, pg_password)

        functions.setup.generate_sql(schema, tables, trino_setup_file)


def prepare_minio_setup(minio_host, minio_port, minio_user, minio_password):
    """
    It prepares minio by uploading the parquet files into the bucket
    :param minio_host: the hostname for connecting to minio
    :param minio_port: the port for connecting to minio
    :param minio_user: the username for connecting to minio
    :param minio_password: the password for connecting to minio
    """

    minio_address = f'{minio_host}:{minio_port}'

    client = Minio(minio_address, access_key=minio_user, secret_key=minio_password, secure=False)

    bucket_name = 'datalake'

    if client.bucket_exists(bucket_name):
        objects = map(lambda o: o.object_name, client.list_objects(bucket_name, recursive=True))

        for obj in objects:
            client.remove_object(bucket_name, obj)
    else:
        client.make_bucket(bucket_name)

    parquet_directory = 'parquet'

    for path, _, files in os.walk(parquet_directory):
        for name in files:
            path_file = os.path.join(*(path.split(os.path.sep)[1:]), name)
            client.fput_object(bucket_name, path_file, os.path.join(parquet_directory, path_file))


def setup(schema, dbname, pg_host, pg_port, pg_user, pg_password, minio_host, minio_port, minio_user, minio_password,
          trino_host, trino_port, trino_user):
    """
    It sets up trino and minio. It uploads the parquet file exported from postgres into minio's bucket, and recreates
        the schema dumped from postgres in trino
    :param schema: the name of the schema
    :param dbname: the name of the database for connecting to postgres
    :param pg_host: the host name for connecting to postgres
    :param pg_port: the port for connecting to postgres
    :param pg_user: the username for connecting to postgres
    :param pg_password: the password for connecting to postgres
    :param minio_host: the hostname for connecting to minio
    :param minio_port: the port for connecting to minio
    :param minio_user: the username for connecting to minio
    :param minio_password: the password for connecting to minio
    :param trino_host: host for connecting to trino
    :param trino_port: port for connecting to trino
    :param trino_user: username for connecting to trino
    """

    sql_directory = 'sql'
    pg_dump_file = os.path.join(sql_directory, 'pg_dump.sql')
    trino_setup_file = os.path.join(sql_directory, 'trino_setup.sql')

    env = os.environ.copy()
    env['PGPASSWORD'] = pg_password

    first_commands = [
        'sudo -S docker compose down',
        'rm -r -f parquet',
        'mkdir -p parquet',
        'sudo -S docker compose up -d',
        f'mkdir -p {sql_directory}',
        f'pg_dump -f {pg_dump_file} -s -n {schema} --no-comments -d {dbname} -h {pg_host} -p {pg_port} -U {pg_user}'
    ]

    for command in first_commands:
        subprocess.run(command.split(), env=env)

    prepare_trino_setup(pg_dump_file, dbname, pg_host, pg_port, pg_user, pg_password, trino_setup_file)

    prepare_minio_setup(minio_host, minio_port, minio_user, minio_password)

    functions.common.trino_execute(trino_host, trino_port, trino_user, trino_setup_file)
