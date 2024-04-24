
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


    folder = cwd /"json_2024_04_23"
    db_file = folder / "_hits.db"

    # Remove if it exists previously
    if db_file.exists():
        db_file.unlink()
    

    print("Creating separate file")
    with duckdb.connect(db_file.as_posix()) as con:

        con.sql(f"""CREATE TABLE db AS SELECT *, lower(abstract) FROM read_json_auto( '{folder}/*.json.gz', union_by_name=true,  maximum_object_size=224857600);""")
        # con.sql(f"""CREATE TABLE db AS SELECT DOI, title, abstract, URL, "is-referenced-by-count", count_keys("abstract") FROM read_parquet('{folder}/*.parquet', union_by_name=true);""")
        con.sql("""ALTER TABLE db RENAME "lower(abstract)" TO  abstract_low """)

        con.sql("DESCRIBE db").show()

        con.sql("SELECT count(*) from db").show()

    print("db created")
    exit()

    with duckdb.connect(db_file.as_posix()) as con:

        # descibe schema
        con.sql("DESCRIBE db").show()
        # con.sql("SELECT * from db").show()
        con.sql("SELECT count(*) from db").show()


        con.create_function("count_keys", count_keys)
        con.sql("""CREATE OR REPLACE TABLE db AS SELECT *, count_keys(abstract_low)FROM db""")
        con.sql("DESCRIBE db").show()


        con.sql("""ALTER TABLE db RENAME "count_keys(abstract_low)" TO  count_keys """)

        con.sql("DESCRIBE db").show()
        # con.sql("SELECT * from db").show()

        hits = con.execute("""SELECT abstract_low FROM db ORDER BY count_keys DESC """).fetchone()
        print(hits)









if __name__ == "__main__":
    main()