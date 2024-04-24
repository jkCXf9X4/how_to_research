
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
    crossref_folder = Path("/media/eriro/WD/crossref/April 2023 Public Data File from Crossref/").resolve()
    output_dir = Path("./test") / f"json_2024_04_23"

    print(f"{crossref_folder=} \n {output_dir=}")

    # keywords = "simulating, simulator, modeling, intended use, verification, validation, credibility".split(", ") # 200424_0653

    # model { model, modeling}
    # simulat { simulate, simulator, simulating}
    # credib {credibility, credible }
    # validat {validation, validate }
    # verif {verify, verification }

    # keywords = "simulat;model;intended use;verif;validat;credib".split(";")
    keywords = "simulat;model".split(";")
    print(f"{keywords=}")

    crossref_files = [Path(i.path) for i in os.scandir(crossref_folder)]
    nr_files = len(crossref_files)
    print(nr_files)

    steps = CrossrefJson.get_patterns(nr_files, step=100)

    steps = steps[:1]

    def extract_files(pattern):
        with duckdb.connect() as c:
            file = crossref_folder / f"{pattern}*.json.gz"
            CrossrefJson.import_jsons(c, file, select = "unnest(items, recursive:= true)")

            keyword_query = [f"""lower(abstract) LIKE '%{k}%'""" for k in keywords]
            keyword_query = " OR ".join(keyword_query) 
            query = f"""SELECT * FROM db WHERE {keyword_query}"""

            out_file = output_dir / f"{pattern}.json.gz"
            CrossrefJson.export_json(c, query, out_file)

    CrossrefJson.pool(extract_files, steps)


if __name__ == "__main__":
    main()