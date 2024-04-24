
import gzip
import os
from pathlib import Path
import re
import traceback
import duckdb
import concurrent.futures
import datetime

from utils.extract.base import CrossrefJson


from utils.abstract import strip_tags




def main():
    cwd = Path(".")
    input_dir = cwd / f"data/2_json_2024_04_23"
    db_file = input_dir / "_hits.db"

    print(f"{input_dir=}")

    keywords = "simulating;simulator;simulate;model;modeling;intended use;verification;verify;validation;validate;credibility;credible".split(";")

    print(f"{keywords=}")



    with duckdb.connect(db_file.as_posix()) as c:
        CrossrefJson.set_defaults(c)

        c.sql("DESCRIBE db").show()
        abstracts = c.sql("SELECT DOI, title, abstract, count_keys FROM db WHERE DOI='10.5194/egusphere-egu21-4935' ORDER BY count_keys DESC ").fetchall()
        for doi, title, abstract, keys in abstracts:
            print("-" * 100)

            print(f"{doi}\n{title}\n{keys}")

            print(strip_tags(abstract))
            print()
            print(abstract)
            # input()



if __name__ == "__main__":
    main()