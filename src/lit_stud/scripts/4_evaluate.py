
import os
from pathlib import Path
import duckdb

from lit_stud.utils.chatgpt import ChatGPTWrapper
from lit_stud.utils.config_log import ConfigLogUtils
from lit_stud.utils.crossref_db import CrossrefJson


from lit_stud.utils.abstract import Abstract
from lit_stud.utils.duckdb import CrossrefDuckDB
from lit_stud.utils.os.files import FileUtils


def main():
    cwd = Path(".")
    input_dir = cwd / f"data/3_db_2024_04_29"
    db_file = input_dir / "hits.db"

    output_dir = cwd / f"data/4_chatgpt_2024_04_29"
    output_dir.mkdir(parents=True, exist_ok=True)

    logs = {"input": input_dir, "output": output_dir, "db_file": db_file}
    ConfigLogUtils.log_config(output_dir / "_log.json", logs)

    chatgpt_files = [i.name for i in os.scandir(output_dir)]
    # print(chatgpt_files)

    db = CrossrefDuckDB(db_file)
    db.descibe()
    db.count()

    abstracts = db.get_abstracts()
    # abstracts = abstracts[:160]

    print(len(abstracts))
    # print(abstracts[1])

    for doi, title, abstract, keys in abstracts:
        if FileUtils.doi_filename(doi) in chatgpt_files:
            print(f"Skipping {doi}")
        else:
            print(f"{doi}\n{title}\n{keys}")
            abstract = Abstract(abstract)
            abstract_text = abstract.get_text()

            full_text = f"""I'm writing an article on evaluating model credibility and model intended use in the context of large scale simulations, cyber-physical systems and model-based system engineering. A focus is on simulating complex systems like aircrafts, cars or telecom networks. How well does this abstract fit into this category, can you estimate the fit in percentage in the format "Abstract fit: %" and provide a 100 word summary of your motivation

Title: {title}
Abstract: {abstract_text}"""
            
            # print(abstract_text)
            
            query = ChatGPTWrapper(full_text)
            query.query()
            query.add_info(DOI=doi, title=title, abstract=abstract_text )

            print(query.get_content())

            query.save_query(output_dir/  FileUtils.doi_filename(doi))
            # exit()


if __name__ == "__main__":
    main()

    