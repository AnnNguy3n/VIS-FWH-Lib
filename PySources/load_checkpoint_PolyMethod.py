import sys
import os
import sqlite3
import numpy as np
from create_checkpoint_PolyMethod import create_checkpoint_PolyMethod


def load_checkpoint_PolyMethod(db_path: str):
    folder_save = db_path.replace("/f.db", "")
    os.makedirs(f"{folder_save}/InputData/", exist_ok=True)
    try:
        os.remove(f"{folder_save}/InputData/checkpoint.bin")
    except: pass

    with sqlite3.connect(db_path) as connection:
        cursor = connection.cursor()

        cursor.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        list_table = [row[0] for row in cursor.fetchall() if row[0].startswith("checkpoint_")]

        if not list_table:
            create_checkpoint_PolyMethod(db_path, 1)
            table_name = "checkpoint_1"
        else:
            max_checkpoint = max(map(lambda name: int(name.replace("checkpoint_", "")), list_table))
            table_name = f"checkpoint_{max_checkpoint}"

        cursor.execute(f"SELECT * FROM {table_name}")
        info = cursor.fetchone()

        current = [
            np.array(info[2:], np.int64),
            np.int64(info[1]),
            np.int64(info[0])
        ]

    with open(f"{folder_save}/InputData/checkpoint.bin", "wb") as f:
        f.write(np.int64(len(current[0]) + 2).tobytes())
        f.write(current[0].tobytes())
        f.write(current[1].tobytes())
        f.write(current[2].tobytes())


if __name__ == "__main__":
    db_path = sys.argv[1]
    load_checkpoint_PolyMethod(db_path)
