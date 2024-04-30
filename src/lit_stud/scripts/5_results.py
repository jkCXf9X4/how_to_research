
import os
from pathlib import Path
import re
import json
from sequence_extensions import list_ext
import shutil

from lit_stud.utils.config_log import ConfigLogUtils
from lit_stud.utils.duckdb import CrossrefDuckDB
from lit_stud.utils.os.files import FileUtils


def main():
    cwd = Path(".")
    step = "3.5"
    if step == "3.5":
        input_dir = cwd / f"data/4_chatgpt_2024_04_29"
        output_dir = cwd / f"data/5_chatgpt_2024_04_29"
    elif step == "4":
        input_dir = cwd / f"data/6_chatgpt_2024_04_29"
        output_dir = cwd / f"data/7_chatgpt_2024_04_29"

    shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True)

    db_file = cwd / f"data/3_db_2024_04_29" / "hits.db"
    db = CrossrefDuckDB(db_file)

    print(f"{input_dir=}")

    match_re = re.compile('Abstract fit: (\d*)%', re.I) 

    chatgpt_files = list_ext([input_dir / i.name for i in os.scandir(input_dir)])
    print(f"Total nr of files: {len(chatgpt_files)}")

    logs = {"input": input_dir, "output": output_dir, "db_file": db_file, "chatgpt_files" : chatgpt_files.to_strings()}
    ConfigLogUtils.log_config(output_dir / "_log.json", logs)

    results = list_ext()

    for f_path in chatgpt_files:
        with open(f_path, "r") as f:

            result = json.load(f)
            # print(result["DOI"])

            match_result = match_re.match(result["choices"][0]["message"]["content"])
            percentage = 0
            if match_result:
                percentage = int(match_result.group(1))
            else:
                print(f"No match for {result["DOI"]}")

            result["match"] = percentage
            results.append(result)

    results.sort(key =lambda x: x["match"], reverse=True)
    # results.for_each(lambda x: print(x["match"]))

    results = results.filter(lambda x: x["match"] >= 80)
    print(f"After filtering, len:{len(results)}")

    def save(x):
        with open(output_dir / FileUtils.doi_filename(x["DOI"]), "w") as f:
            print(f)
            json.dump(x, f, indent=4)

    results.for_each(lambda x: save(x))

    # this one is quite slow, dont use unness nessesary
    # with db.get_connection() as c:
    #     results.for_each(lambda x : db.get_doi(x["DOI"], connection=c)) 



    def p(x):
        print("-"*100)
        print(x["match"])
        print(x["DOI"])
        print(x["title"])
        print(x["abstract"])
        input()

    # results.for_each(lambda x: p(x))






if __name__ == "__main__":
    main()

    