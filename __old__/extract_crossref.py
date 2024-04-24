
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
    crossref_folder = Path("/home/eriro/pwa/resources/April_2023_Crossref").resolve()
    output_dir = cwd / f"parquets_{datetime.datetime.now()}"

    print(f"{crossref_folder=} \n {output_dir=}")
    

    # keywords = "simulating, simulator, modeling, intended use, verification, validation, credibility".split(", ") # 200424_0653

    # model { model, modeling}
    # simulat { simulate, simulator, simulating}
    # credib {credibility, credible }
    # validat {validation, validate }
    # verif {verify, verification }

    keywords = "simulat;model;intended use;verif;validat;credib".split(";")
    print(f"{keywords=}")

    def extract_file(file :Path):
        with duckdb.connect() as c:

            query = f"CREATE TABLE db AS SELECT unnest(items, recursive:= true) FROM read_json_auto('{file.as_posix()}', maximum_object_size=124857600);"
            # print(f"{query=}")
            c.execute(query)

            keyword_query = [f"lower(abstract) LIKE '%{k}%'" for k in keywords]
            keyword_query = " OR ".join(keyword_query) 
            # print(f"{keyword_query=}")

            output_dir.mkdir(exist_ok=True)
            query = f"""SELECT * FROM db WHERE {keyword_query}"""
            c.sql(query).write_parquet(str(output_dir/f"{file.name}.parquet"))
            # print(f"Export {file} as parque")

    crossref_files = [Path(i.path) for i in os.scandir(crossref_folder)]
    print(len(crossref_files))

    # run single file
    # crossref_files = [crossref_folder / "999.json.gz"]

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        futures = {executor.submit(extract_file, file): file for file in crossref_files}
        counter = 1
        for future in concurrent.futures.as_completed(futures):
            counter += 1
            try:
                print(f"Completed {counter} {futures[future]}")
                data = future.result()
            except Exception as exc:
                if ("abstract" in str(exc)):
                    print(f'{futures[future]} Did not contain an abstract feild')
                else:
                    print(f'{futures[future]} generated an exception: {exc}')
                    traceback.print_exception(exc)


    # Remove if it exists previously
    # db_file = cwd / "hits.db"
    # if db_file.exists():
    #     db_file.unlink()

    # print("Creating separate file")
    # with duckdb.connect(db_file.as_posix()) as con:
    #     # 
    #     con.sql("""CREATE TABLE db AS SELECT DOI, title, abstract, URL, "is-referenced-by-count" FROM read_parquet('parquets/*.parquet');""")


    # with duckdb.connect(db_file.as_posix()) as con:
    #     nr_hits = con.execute("SELECT count(*) FROM db").fetchall()
    #     print(nr_hits)

if __name__ == "__main__":
    main()