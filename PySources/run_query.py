import sys
import sqlite3


def run_query(db_path: str):
    print("Start execute query")
    with open(db_path.replace("f.db", "queries.bin"), "rb") as f:
        queries = f.read().decode("utf-8").split(";")

    with sqlite3.connect(db_path) as connection:
        cursor = connection.cursor()
        cursor.execute("BEGIN")
        for query in queries:
            if query.strip():
                cursor.execute(query)
        cursor.execute("COMMIT")
        print("Done")


if __name__ == "__main__":
    db_path = sys.argv[1]
    run_query(db_path)
