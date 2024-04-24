
import concurrent.futures
import os
from pathlib import Path
import traceback
import duckdb

class CrossrefJson:

    @staticmethod
    def print_seq(item, level=0):
        if isinstance(item, list) or isinstance(item, tuple):
            for i in item:
                CrossrefJson.print_seq(i, level=level+1)
        else:
            print(f"{'-'*level} {type(item)} {item}")


    @staticmethod
    def get_patterns(nr_files, step=100):
        """
        step = 10/100/1000
        
        """
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

    @staticmethod
    def import_jsons(con : duckdb.DuckDBPyConnection, file, select="*"):
        """
        file can contain regex, eg 00*.*
        """

        # unnest(items, recursive:= true)
        query = f"CREATE TABLE db AS SELECT {select} FROM read_json_auto( '{file}', union_by_name=true,  maximum_object_size=224857600);"
        # print(f"{query=}")
        con.sql(query)

    @staticmethod
    def export_json(con : duckdb.DuckDBPyConnection, query, file : Path) :
            duckdb.connect()
            file.mkdir(parents=True, exist_ok=True)

            ext = str(file).split["."][-1]

            if ext == "parquet":
                print(f"{query=}")
                con.sql(query).write_parquet(str(file))
            elif ext =="gz":
                query = f"""COPY ({query}) TO '{file}';"""
                print(f"{query=}")
                con.sql(query)

    
    @staticmethod
    def pool(function, args, max_workers=2):

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(function, arg): arg for arg in args}
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
