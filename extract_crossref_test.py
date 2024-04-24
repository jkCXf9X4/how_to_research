
import gzip
import os
from pathlib import Path
import traceback
import duckdb
import concurrent.futures
import datetime
# cursor = connection.cursor()

def print_seq(item, level=0):
    if isinstance(item, list) or isinstance(item, tuple):
        for i in item:
            print_seq(i, level=level+1)
    else:
        print(f"{'-'*level} {type(item)} {item}")


def main():
    cwd = Path(".")
    crossref_folder = Path("/media/eriro/WD/crossref/April 2023 Public Data File from Crossref/").resolve()
    output_dir = Path("/media/eriro/WD") / f"json_2024_04_23"

    print(f"{crossref_folder=} \n {output_dir=}")
    output_type = "json"

    # json to 
    

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

    # read them 10/100/1000 files at a time
    step = 100
    steps = [int(i/step) for i in range(0, nr_files, step)]
    max_nr_of_digits = len(str(steps[-1]))
    print(steps)

    def fill_zero(x):
        fill_nr = max_nr_of_digits-len(str(x))
        return f"{'0'*fill_nr}{x}"

    steps = [fill_zero(i) for i in steps]
    # steps = ["0000"]
    print(steps)
    # exit()


    def extract_files(pattern):
        with duckdb.connect() as c:
            # 
            query = f"CREATE TABLE db AS SELECT unnest(items, recursive:= true) FROM read_json_auto( '{crossref_folder}/{pattern}*.*', union_by_name=true,  maximum_object_size=224857600);"
            # print(f"{query=}")
            c.execute(query)

            keyword_query = [f"""lower(abstract) LIKE '%{k}%'""" for k in keywords]
            keyword_query = " OR ".join(keyword_query) 
            query = f"""SELECT * FROM db WHERE {keyword_query}"""

            output_dir.mkdir(exist_ok=True)
            if output_type == "parquet":
                print(f"{query=}")
                c.sql(query).write_parquet(str(output_dir/f"{pattern}.parquet"))
            elif output_type =="json":
                query = f"""COPY ({query}) TO '{ output_dir/ f"{pattern}.json.gz" }';"""
                print(f"{query=}")
                c.sql(query)
            



    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futures = {executor.submit(extract_files, pattern): pattern for pattern in steps}
        for future in concurrent.futures.as_completed(futures):
            try:
                data = future.result()
                print(f"Completed {futures[future]}")
            except Exception as exc:
                if ("abstract" in str(exc)):
                    print(f'{futures[future]} Did not contain an abstract feild')

                elif ("No files found that match the pattern") in str(exc):
                    print(f"{futures[future]} not found")
                else:
                    print(f'{futures[future]} generated an exception: {exc}')
                    traceback.print_exception(exc)

if __name__ == "__main__":
    main()