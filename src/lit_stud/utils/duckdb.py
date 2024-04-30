

from contextlib import contextmanager
from pathlib import Path

import duckdb

from lit_stud.utils.crossref_db import CrossrefJson


class CrossrefDuckDB:
        
        def __init__(self, db_file : Path) -> None:
                self.db_file : Path = db_file
                
        @contextmanager
        def get_connection(self):
            with duckdb.connect(self.db_file.as_posix()) as c:
                CrossrefJson.set_defaults(c)
                yield c

        def descibe(self):
             with self.get_connection() as c:
                c.sql("DESCRIBE db").show()

        def count(self):
             with self.get_connection() as c:
                c.sql("SELECT count(*) FROM db").show()

        def get_abstracts(self, selection = "DOI, title, abstract, count_keys"):
            with self.get_connection() as c:

                abstracts = c.sql(f"SELECT {selection} FROM db ORDER BY count_keys DESC").fetchall()
                print(len(abstracts))
                return abstracts

        def get_doi(self, doi, selection="DOI, title, abstract, count_keys", connection=None):
            """
            doi eg: 10.1149/ma2022-02391368mtgabs
            """

            if connection:
                 return connection.sql(f"SELECT * FROM db WHERE DOI='{doi}'").fetchone()
            else:
                with self.get_connection() as c:
                    return c.sql(f"SELECT * FROM db WHERE DOI='{doi}'").fetchone()