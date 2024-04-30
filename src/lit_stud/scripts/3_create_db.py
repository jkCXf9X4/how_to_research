
from pathlib import Path
import duckdb

from lit_stud.utils.crossref_db import CrossrefJson
from lit_stud.utils.duckdb import CrossrefDuckDB

def main():
    cwd = Path(".")
    input_dir = cwd / f"data/2_json_2024_04_29"
    db_file = input_dir / "_hits.db"

    # Remove if it exists previously
    if db_file.exists():
        db_file.unlink()

    print(f"{input_dir=}")

    db = CrossrefDuckDB(db_file)

    with db.get_connection() as c:

        file = input_dir / f"*.json.gz"
        CrossrefJson.import_jsons(c, file, select = "*")

        c.sql("SELECT abstract, count_keys FROM db ORDER BY count_keys DESC ").show()

    db.descibe()
    db.count()

if __name__ == "__main__":
    main()