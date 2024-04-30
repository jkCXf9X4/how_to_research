
import os
from pathlib import Path
import re
import json
from sequence_extensions import list_ext

from lit_stud.utils.duckdb import CrossrefDuckDB


def main():
    cwd = Path(".")
    input_dir = cwd / f"data/3_chatgpt_2024_04_29"

    db_file = cwd / f"data/2_json_2024_04_29" / "_hits.db"
    db = CrossrefDuckDB(db_file)

    print(f"{input_dir=}")

    match_re = re.compile('Abstract fit: (\d*)%', re.I) 

    chatgpt_files = [input_dir / i.name for i in os.scandir(input_dir)]
    chatgpt_files = chatgpt_files[:]
    print(f"Total nr of files: {len(chatgpt_files)}")

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
    results = results.filter(lambda x: x["match"] >= 85)
    print(f"After filtering, len:{len(results)}")


    # this one is quite slow, dont use unness nessesary
    with db.get_connection() as c:
        results.for_each(lambda x : db.get_doi(x["DOI"], connection=c)) 


    # results.for_each(lambda x: print(x["match"]))

    def p(x):
        print(x["match"])
        print(x["DOI"])
        print(x["title"])
        print(x["abstract"])
        input()

    # results.filter(lambda x: x["match"] == 80).for_each(lambda x: p(x))






if __name__ == "__main__":
    main()

    