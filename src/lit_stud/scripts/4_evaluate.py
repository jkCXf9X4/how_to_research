
import gzip
import os
from pathlib import Path
import re
import traceback
import duckdb
import concurrent.futures
import datetime
import json

from lit_stud.utils.extract.base import CrossrefJson


from lit_stud.utils.abstract import Abstract
import keyring

from openai import OpenAI

client = OpenAI(
    # This is the default and can be omitted
    api_key=keyring.get_password("openai", "openai")
)

def query_chat_gpt(text):
    query = f"""
im writing a article on evaluating model credibility and model intended use in the context of simulating complex systems like aircrafts, cars or telecom networks eith a focus on verification and validation. How well does this abtract fit into this category, can you estimate the fit in percentage in the format "Abstract fit: %"

{text}"""

    # print(query)

    chat_completion = client.chat.completions.create(
        messages=[
            {"role": "system", "content": "Assistant is a large language model trained by OpenAI."},
            {
                "role": "user",
                "content": query,
                # "content": "Say This is a test",
            }
        ],
        model="gpt-3.5-turbo",
        )

    return chat_completion.to_dict()


def doi_filename(doi):
    return f"{doi.replace("/", "_")}.json"


def main():
    cwd = Path(".")
    input_dir = cwd / f"data/2_json_2024_04_23"
    db_file = input_dir / "_hits.db"

    ouput_dir = cwd / f"data/3_chatgpt_2024_04_24"
    ouput_dir.mkdir(parents=True, exist_ok=True)

    print(f"{input_dir=}")

    keywords = "simulating;simulator;simulate;model;modeling;intended use;verification;verify;validation;validate;credibility;credible".split(";")

    print(f"{keywords=}")

    chatgpt_files = [i.name for i in os.scandir(ouput_dir)]
    print(chatgpt_files)


    with duckdb.connect(db_file.as_posix()) as c:
        CrossrefJson.set_defaults(c)

        c.sql("DESCRIBE db").show()
        where = ""
        # where = "WHERE DOI='10.1149/ma2022-02391368mtgabs'"

        abstracts = c.sql(f"SELECT DOI, title, abstract, count_keys FROM db {where} ORDER BY count_keys DESC ").fetchall()
        abstracts = abstracts[:150]

        for doi, title, abstract, keys in abstracts:
            if doi_filename(doi)  in chatgpt_files:
                print(f"Skipping {doi}")
            else:
                print(f"{doi}\n{title}\n{keys}")
                abstract = Abstract(abstract)

                answer_dict = query_chat_gpt(abstract.get_text())
                answer_dict["DOI"] = doi
                answer_dict["abstract"] = abstract.get_text()
                answer_dict["title"] = title

                # # print(answer_dict)
                # # json_dump = json.dumps(answer_dict, indent=4)
                # # print(json_dump)
                # # print(json.loads(json_dump))

                # print(answer_dict["choices"][0]["message"]["content"])

                # # print(answer_json)

                # print(doi_filename(doi))
                with open(ouput_dir/ doi_filename(doi), "w") as f:
                    json.dump(answer_dict, f, indent=4)
        
        # print(word_counter)


if __name__ == "__main__":
    main()

    