
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


def print_seq(item, level=0):
    if isinstance(item, list) or isinstance(item, tuple):
        for i in item:
            print_seq(i, level=level+1)
    else:
        print(f"{'-'*level} {type(item)} {item}")



def main():
    cwd = Path(".")
    db_file = cwd / "hits.db"
    print(f"Pid [{os.getgid()}]")


    # Remove if it exists previously
    if db_file.exists():
        db_file.unlink()

    folder = "parquets_2024-04-20 11:20:35.683421"

    print("Creating separate file")
    with duckdb.connect(db_file.as_posix()) as con:
        # 
        con.sql(f"""CREATE TABLE db AS SELECT DOI, title, abstract, URL, "is-referenced-by-count" FROM read_parquet('{folder}/*.parquet');""")

    print("db created")
    with duckdb.connect(db_file.as_posix()) as con:
        nr_hits = con.execute("SELECT count(*) FROM db").fetchall()
        print(nr_hits)
        # while(True):
        hits = con.sql("SELECT * FROM db").show()
        # print(hits)

        # descibe schema
        query = f"DESCRIBE db"
        print(f"{query=}")
        con.sql(query).show()
        # print_seq(res)

        con.create_function("count_keys", count_keys)
        con.sql("""CREATE OR REPLACE TABLE db AS SELECT *, count_keys("abstract") FROM db""")
        # print(hits)

        # descibe schema
        query = f"DESCRIBE db"
        print(f"{query=}")
        con.sql(query).show()
        # print_seq(res)

        hits = con.execute("""SELECT * FROM db ORDER BY "count_keys(abstract)" DESC """).fetchone()
        print(hits)









if __name__ == "__main__":
    main()