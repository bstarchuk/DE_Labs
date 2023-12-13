from converter import conn, name_table_set, convert_csv_to_table

TABLES = ['accounts', 'products', 'transactions']


def main():
    connection = conn()
    for table in TABLES:
        name_table_set(table, connection)
        convert_csv_to_table(table, connection)

    connection.close()


if __name__ == "__main__":
    main()
