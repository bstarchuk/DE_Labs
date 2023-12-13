import psycopg2
from psycopg2 import sql
from pathlib import Path
import csv
import os

IN_DB_OPTIONS = {
    'host': 'localhost',
    'database': 'postgres',
    'user': 'postgres',
    'password': 'postgres'
}

DB_OPTIONS = {
    'host': 'localhost',
    'database': 'data_engineering',
    'user': 'postgres',
    'password': 'postgres'
}


def check_data(connection):
    db_name = DB_OPTIONS.get("database")
    db_owner = DB_OPTIONS.get("user")

    with connection.cursor() as cursor:
        cursor.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{db_name}'")
        exists = cursor.fetchone()
        if not exists:
            cursor.execute(f"""
                CREATE DATABASE {db_name}
                    WITH
                    OWNER = {db_owner}
                    ENCODING = 'UTF8'
                    CONNECTION LIMIT = -1
                    IS_TEMPLATE = False;
                           """)


def conn():
    in_conn = psycopg2.connect(**IN_DB_OPTIONS)
    check_data(in_conn)
    connection = psycopg2.connect(**DB_OPTIONS)
    return connection


def name_table_set(table_name: str, connection):
    schema_path = f"schema/{table_name}.sql"
    is_schema = os.path.isfile(schema_path)
    if not is_schema:
        print(f"'{table_name}' schema does not exist.")

    ddl_script = Path(schema_path).read_text()
    with connection.cursor() as cursor:
        cursor.execute(ddl_script)
    connection.commit()
    cursor.close()
    print(f"Table '{table_name}' is created'")


def convert_csv_to_table(table_name: str, connection):
    data_path = f"data/{table_name}.csv"
    is_data = os.path.isfile(data_path)
    if not is_data:
        print(f"'{table_name}' data does not exist.")

    with connection.cursor() as cursor:
        with open(data_path, 'r', encoding='utf-8') as csv_file:
            csv_reader = csv.reader(csv_file)
            header = next(csv_reader)

            cursor.execute(sql.SQL("DELETE FROM {}").format(sql.Identifier(table_name)))

            cleaned_up_rows = [[value.strip() for value in row] for row in csv_reader]

            insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                sql.Identifier(table_name),
                sql.SQL(", ").join(map(lambda col: sql.Identifier(col.strip()), header)),
                sql.SQL(", ").join(sql.Placeholder() * len(header)),
            )

            print(insert_query)

            cursor.executemany(insert_query, cleaned_up_rows)

    connection.commit()

    cursor.close()
