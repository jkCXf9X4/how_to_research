
from pathlib import Path
import json

from sequence_extensions import list_ext

from lit_stud.utils.config_log import ConfigLogUtils
from lit_stud.utils.crossref_db import CrossrefJson
from lit_stud.utils.duckdb import CrossrefDuckDB

def main():
    cwd = Path(".")
    input_dir = cwd / f"data/2_json_2024_04_29"
    output_dir = cwd / f"data/3_db_2024_04_29"
    log_dir = output_dir/ "logs" 

    output_dir.mkdir(exist_ok=True, parents=True)
    db_file = output_dir / "hits.db"

    logs = {"input": input_dir, "output": output_dir, "db_file": db_file}
    ConfigLogUtils.log_config(log_dir / "args.json", logs)

    # Remove if it exists previously
    if db_file.exists():
        db_file.unlink()

    print(f"{input_dir=}")

    db = CrossrefDuckDB(db_file)

    with db.get_connection() as c:

        file = input_dir / f"*.json.gz"
        CrossrefJson.import_jsons(c, file, select = "*")

        c.sql("SELECT abstract, count_keys FROM db ORDER BY count_keys DESC ").show()

        descriptions = list_ext(c.sql("DESCRIBE db").fetchall())
        descriptions = descriptions.to_string(separator="\n")

        with open(log_dir / "db.log", "w") as f:
            f.write(descriptions)

    db.descibe()
    db.count()

if __name__ == "__main__":
    main()