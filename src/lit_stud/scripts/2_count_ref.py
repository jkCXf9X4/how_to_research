
import gzip
import os
from pathlib import Path
import traceback
import duckdb
import concurrent.futures
import datetime

from utils.extract.base import CrossrefJson


def main():
    cwd = Path(".")
    input_dir = cwd / f"data/1_json_2024_04_23"
    output_dir = cwd / f"data/2_json_2024_04_23"

    print(f"{input_dir=} \n {output_dir=}")

    keywords = "simulating;simulator;simulate;model;modeling;intended use;verification;verify;validation;validate;credibility;credible".split(";")

    print(f"{keywords=}")

    def count_keys(text : str) -> int:
        counter = 0
        for key in keywords:
            if key in text:
                counter += 1
        return counter

    crossref_files = [Path(i.path) for i in os.scandir(input_dir)]
    nr_files = len(crossref_files)
    print(nr_files)

    steps = CrossrefJson.get_patterns(nr_files, step=10)

    # steps = steps[:1]
    print(steps)


    def extract_files(pattern):
        with duckdb.connect() as c:
            CrossrefJson.set_defaults(c)

            file = input_dir / f"{pattern}*.json.gz"
            CrossrefJson.import_jsons(c, file, select = "*, lower(abstract)")
            c.sql("""ALTER TABLE db RENAME "lower(abstract)" TO  abstract_low """)

            # c.sql("DESCRIBE db").show()
            # c.sql("SELECT count(*) from db").show()

            c.create_function("count_keys", count_keys)

            c.sql("""CREATE OR REPLACE TABLE db AS SELECT *, count_keys(abstract_low) FROM db""")
            c.sql("""ALTER TABLE db RENAME "count_keys(abstract_low)" TO  count_keys """)
            # print(hits)

            # c.sql("DESCRIBE db").show()
            # c.sql("SELECT count(*) from db").show()
            # c.sql("SELECT count(*) from db WHERE count_keys>4").show()
            # c.sql("SELECT count(*) from db WHERE count_keys>4").show()


            out_file = output_dir / f"{pattern}.json.gz"
            CrossrefJson.export_json(c, "SELECT * FROM db WHERE count_keys>4", out_file)

    CrossrefJson.pool(extract_files, steps)


if __name__ == "__main__":
    main()