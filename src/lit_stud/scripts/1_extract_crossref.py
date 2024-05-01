
import os
from pathlib import Path
import duckdb

from lit_stud.utils.config_log import ConfigLogUtils
from lit_stud.utils.crossref_db import CrossrefJson


def main():
    cwd = Path(".")
    crossref_folder = Path("/media/eriro/WD/crossref/April 2023 Public Data File from Crossref/").resolve()
    output_dir = cwd / "data" / f"1_json_2024_04_23"

    print(f"{crossref_folder=} \n {output_dir=}")

    keywords = "simulat;model".split(";")
    print(f"{keywords=}")

    logs = {"crossref_folder": crossref_folder, "output": output_dir, "keywords": keywords}
    ConfigLogUtils.log_config(output_dir/ "logs" / "args.json", logs)

    crossref_files = [Path(i.path) for i in os.scandir(crossref_folder)]
    nr_files = len(crossref_files)
    print(nr_files)

    steps = CrossrefJson.get_patterns(nr_files, step=10)

    steps = steps[:2]

    arguments = [(crossref_folder / f"{p}*.json.gz", output_dir / f"{p}.json.gz") for p in steps]
    print(arguments)

    def extract_files(files):
        with duckdb.connect() as c:

            # file = crossref_folder / f"{pattern}*.json.gz"
            CrossrefJson.import_jsons(c, files[0], select = "unnest(items, recursive:= true)")

            keyword_query = [f"""lower(abstract) LIKE '%{k}%'""" for k in keywords]
            keyword_query = " OR ".join(keyword_query) 
            query = f"""SELECT * FROM db WHERE {keyword_query}"""

            # out_file = output_dir / f"{pattern}.json.gz"
            CrossrefJson.export_json(c, query, files[1])

    CrossrefJson.pool(extract_files, arguments)


if __name__ == "__main__":
    main()