from config import *
from tools import setup, transformation


SETUP = True
SCHEMA = 'northwind'
NOTES_FILE = 'notes/northwind.sql'


if __name__ == '__main__':
    if SETUP:
        setup(SCHEMA, DBNAME, PG_HOST, PG_PORT, PG_USER, PG_PASSWORD, MINIO_HOST, MINIO_PORT, MINIO_USER,
              MINIO_PASSWORD, TRINO_HOST, TRINO_PORT, TRINO_USER)

    transformation(SCHEMA, TRINO_HOST, TRINO_PORT, TRINO_USER, NOTES_FILE)
