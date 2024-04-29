
import gzip
import os
from pathlib import Path
import traceback
import duckdb
import concurrent.futures
import datetime

from lit_stud.utils.extract.crossref import CrossrefJson

def main():
    cwd = Path(".")
    input_dir = cwd / f"data/2_json_2024_04_23"
    db_file = input_dir / "_hits.db"

    # Remove if it exists previously
    if db_file.exists():
        db_file.unlink()

    print(f"{input_dir=}")


    with duckdb.connect(db_file.as_posix()) as c:
        CrossrefJson.set_defaults(c)

        file = input_dir / f"*.json.gz"
        CrossrefJson.import_jsons(c, file, select = "*")

        # c.sql("DESCRIBE db").show()
        # c.sql("SELECT abstract, count_keys FROM db ORDER BY count_keys DESC ").show()




if __name__ == "__main__":
    main()