
from io import TextIOWrapper
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
    data_dir = cwd / "data"
    step = "4"
    if step == "3.5":
        input_dir = data_dir / f"4_chatgpt_2024_04_29"
        output_dir = data_dir / f"5_chatgpt_2024_04_29"
    elif step == "4":
        input_dir = data_dir / f"6_chatgpt_4_2024_04_29"
        output_dir = data_dir / f"7_results_2024_04_29"

    rate_file = data_dir / "rate.json"
    rate_file.touch()

    shutil.rmtree(output_dir, ignore_errors=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    db_file = cwd / f"data/3_db_2024_04_29" / "hits.db"
    db = CrossrefDuckDB(db_file)

    print(f"{input_dir=}")

    match_re = re.compile('Abstract fit: ([0-9]*)%', re.I) 

    chatgpt_files = list_ext([input_dir / i.name for i in os.scandir(input_dir)])
    print(f"Total nr of files: {len(chatgpt_files)}")

    logs = {"input": input_dir, "output": output_dir, "db_file": db_file, "chatgpt_files" : chatgpt_files.to_strings()}
    ConfigLogUtils.log_config(output_dir/ "logs" / "args.json", logs)

    results = list_ext()

    for f_path in chatgpt_files:
        with open(f_path, "r") as f:

            result = json.load(f)
            # print(result)
            try:
                result["DOI"]
            except:
                # usually when trying to open log file
                continue

            match_result = match_re.match(result["choices"][0]["message"]["content"])
            percentage = 0
            if match_result:
                percentage = int(match_result.group(1))
            else:
                print(f"No match for {result["DOI"]}")

            result["match"] = percentage
            results.append(result)

    results.sort(key =lambda x: x["match"], reverse=True)

    results = results.filter(lambda x: x["match"] >= 80)
    print(f"After filtering, len:{len(results)}")

    # results.for_each(lambda x: print(x["match"]))

    def save(x):
        with open(output_dir / FileUtils.doi_filename(x["DOI"]), "w") as f:
            print(f)
            json.dump(x, f, indent=4)

    # results.for_each(lambda x: save(x))

    # this one is quite slow, dont use unness nessesary
    # with db.get_connection() as c:
    #     results.for_each(lambda x : db.get_doi(x["DOI"], connection=c)) 

    def read_rates(file: TextIOWrapper):
        data = list_ext()
        for l in file:
            data.append(json.loads(l))
        return data

    dois = []
    with open(rate_file, "r") as rates_f:
        previous_rated = read_rates(rates_f)
        dois = previous_rated.map(lambda x : x["doi"])
        # print(dois)

    def p(x, file : TextIOWrapper):
        if x["DOI"] in dois:
            print(f"Skipping {x["DOI"]}")
            return

        print("-"*100)

        article = db.get_doi(x["DOI"], selection="*")

        print(x["match"])
        print(x["DOI"])
        print(article[0]) # link
        print(x["title"])
        print(x["abstract"])
        rating = input("Rate from 0-9: ")
        file.write("\n" + json.dumps({"doi" : x["DOI"], "title" :x["title"][0], "rating" : rating}))




    with open(rate_file, "a+") as rates_f:
        results.for_each(lambda x: p(x, rates_f))







if __name__ == "__main__":
    main()

    