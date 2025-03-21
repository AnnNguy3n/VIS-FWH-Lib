import sys
import sqlite3


def create_checkpoint_PolyMethod(db_path: str, num_operand: int):
    with sqlite3.connect(db_path) as connection:
        cursor = connection.cursor()
        cursor.execute("BEGIN")

        table_name = f"checkpoint_{num_operand}"
        columns = ",".join(map(lambda i: f"E{i} INTEGER NOT NULL", range(num_operand * 2)))
        query_create = f"""
        CREATE TABLE {table_name}(
            id INTEGER NOT NULL,
            num_opr_per_grp INTEGER NOT NULL,
            {columns}
        )
        """
        cursor.execute(query_create)

        placeholders = ("0," * num_operand * 2)[:-1]
        query_insert = f"INSERT INTO {table_name} VALUES (0, 1, {placeholders})"
        cursor.execute(query_insert)

        cursor.execute("COMMIT")


if __name__ == "__main__":
    database_path = sys.argv[1]
    num_operands = int(sys.argv[2])
    create_checkpoint_PolyMethod(database_path, num_operands)
