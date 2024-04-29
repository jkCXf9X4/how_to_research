
import os
from pathlib import Path
import duckdb
import json

from lit_stud.utils.chatgpt import ChatGPTWrapper
from lit_stud.utils.extract.crossref import CrossrefJson


from lit_stud.utils.abstract import Abstract
from lit_stud.utils.os.files import FileUtils


def main():
    cwd = Path(".")
    input_dir = cwd / f"data/2_json_2024_04_23"
    db_file = input_dir / "_hits.db"

    ouput_dir = cwd / f"data/3_chatgpt_2024_04_24"
    ouput_dir.mkdir(parents=True, exist_ok=True)

    print(f"{input_dir=}")

    chatgpt_files = [i.name for i in os.scandir(ouput_dir)]
    print(chatgpt_files)

    with duckdb.connect(db_file.as_posix()) as c:
        CrossrefJson.set_defaults(c)

        c.sql("DESCRIBE db").show()
        where = ""
        # where = "WHERE DOI='10.1149/ma2022-02391368mtgabs'"

        abstracts = c.sql(f"SELECT DOI, title, abstract, count_keys FROM db {where} ORDER BY count_keys DESC ").fetchall()
        print(len(abstracts))
        # abstracts = abstracts[:160]

        for doi, title, abstract, keys in abstracts:
            if FileUtils.doi_filename(doi) in chatgpt_files:
                print(f"Skipping {doi}")
            else:
                print(f"{doi}\n{title}\n{keys}")
                abstract = Abstract(abstract)
                abstract_text = abstract.get_text()

                full_text = f"""I'm writing a article on evaluating model credibility and model intended use in the context of simulating complex systems like aircrafts, cars or telecom networks eith a focus on verification and validation. How well does this abtract fit into this category, can you estimate the fit in percentage in the format "Abstract fit: %"

{title}, {abstract_text}"""
                
                query = ChatGPTWrapper(full_text)
                query.query()
                query.add_info(DOI=doi, title=title, abstract=abstract_text )

                print(query.get_content())

                query.save_query(ouput_dir/  FileUtils.doi_filename(doi))

        
        # print(word_counter)


if __name__ == "__main__":
    main()

    