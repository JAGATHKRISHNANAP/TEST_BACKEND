from psycopg2 import sql
def table_exists(cur, table_name):
    cur.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)", (table_name,))
    return cur.fetchone()[0]

def create_table(cur, table_name, columns, primary_key_column):
    create_table_query = sql.SQL('CREATE TABLE IF NOT EXISTS {} ({})').format(sql.Identifier(table_name), sql.SQL(columns))
    cur.execute(create_table_query)

    if primary_key_column:
        alter_table_query = sql.SQL('ALTER TABLE {} ADD PRIMARY KEY ({})').format(
            sql.Identifier(table_name), sql.Identifier(primary_key_column))
        cur.execute(alter_table_query)

def update_or_insert_row(cur, table_name, primary_key_column, row, df):
    cur.execute(
        sql.SQL("SELECT EXISTS (SELECT 1 FROM {} WHERE {} = %s)").format(
            sql.Identifier(table_name), sql.Identifier(primary_key_column)),
        (str(row[primary_key_column]),)
    )
    exists = cur.fetchone()[0]

    if exists:
        update_values = [(col, row[col] if row[col] != 'NaN' else None) for col in df.columns if col != primary_key_column]
        update_query = sql.SQL('UPDATE {} SET {} WHERE {} = %s').format(
            sql.Identifier(table_name),
            sql.SQL(', ').join(col + sql.SQL(' = %s') for col, _ in update_values),
            sql.Identifier(primary_key_column)
        )
        cur.execute(update_query, [value for _, value in update_values] + [str(row[primary_key_column])])
    else:
        insert_query = sql.SQL('INSERT INTO {} ({}) VALUES ({})').format(
            sql.Identifier(table_name),
            sql.SQL(', ').join(map(sql.Identifier, df.columns)),
            sql.SQL(', ').join(sql.Placeholder() * len(df.columns))
        )
        cur.execute(insert_query, tuple(row))
