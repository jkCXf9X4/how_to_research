
from pathlib import Path
import duckdb
from duckdb.typing import *

import os

# cursor = connection.cursor()

keywords = "simulating;simulator;simulate;model;modeling;intended use;verification;verify;validation;validate;credibility;credible".split(";")

def count_keys(text : str) -> int:
    counter = 0
    for key in keywords:
        if key in text:
            counter += 1
    return counter


def main():
    cwd = Path(".")
    print(f"Pid [{os.getpid()}]")


    folder = cwd /"parquets_2024_04_22"
    db_file = folder / "_hits.db"

    with duckdb.connect(db_file.as_posix()) as con:

        # descibe schema
        con.sql("DESCRIBE db").show()
        # con.sql("SELECT * from db").show()
        con.sql("SELECT count(*) from db").show()


        con.create_function("count_keys", count_keys)
        con.sql("""CREATE OR REPLACE TABLE db AS SELECT *, count_keys(abstract_low) FROM db""")
        con.sql("""ALTER TABLE db RENAME "count_keys(abstract_low)" TO  count_keys """)
        # print(hits)

        con.sql("DESCRIBE db").show()
        con.sql("SELECT count(*) from db").show()

        # hits = con.execute("""SELECT * FROM db ORDER BY count_keys DESC """).fetchone()
        # print(hits)

    print("Done")





if __name__ == "__main__":
    main()