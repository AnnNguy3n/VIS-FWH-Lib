import sys
import sqlite3


def create_table_PolyMethod(
        db_path: str,
        start_cycle: int,
        end_cycle: int,
        num_operand: int,
        columns: list[str]
):
    with sqlite3.connect(db_path) as connection:
        cursor = connection.cursor()
        cursor.execute("BEGIN")

        for cycle in range(start_cycle, end_cycle + 1):
            table_name = f"T{cycle}_{num_operand}"

            operand_columns = ",".join(map(lambda i: f"E{i} INTEGER NOT NULL", range(num_operand)))
            extra_columns = ",".join(map(lambda col: f"{col} REAL", columns))
            query_create = f"CREATE TABLE IF NOT EXISTS {table_name} ({operand_columns}, {extra_columns})"
            cursor.execute(query_create)

        cursor.execute("COMMIT")


if __name__ == "__main__":
    database_path = sys.argv[1]
    start_cycle = int(sys.argv[2])
    end_cycle = int(sys.argv[3])
    num_operands = int(sys.argv[4])
    extra_columns = sys.argv[5:]
    create_table_PolyMethod(database_path, start_cycle, end_cycle, num_operands, extra_columns)
