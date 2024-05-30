from io import TextIOWrapper
import os
from pathlib import Path
import json
from sequence_extensions import list_ext

from lit_stud.utils.config_log import ConfigLogUtils
from lit_stud.utils.duckdb import CrossrefDuckDB


def main():
    cwd = Path(".")
    data_dir = cwd / "data"
    step = "3.5"
    # step = "4"

    if step == "3.5":
        input_dir = data_dir / "5.0_results_2024_04_29"
    elif step == "4":
        input_dir = data_dir / "5.1_results_4_2024_04_29"

    output_dir = data_dir / "6_rate_2024_04_29"
    output_dir.mkdir(parents=True, exist_ok=True)

    rate_file = output_dir / "rate.json"
    rate_file.touch()

    db_file = cwd / "data/3_db_2024_04_29" / "hits.db"
    db = CrossrefDuckDB(db_file)

    print(f"{input_dir=}")

    # find files to parse
    doi_files = list_ext([input_dir / i.name for i in os.scandir(input_dir)])
    print(f"Total nr of files: {len(doi_files)}")

    logs = {
        "input": input_dir,
        "db_file": db_file,
        "chatgpt_files": doi_files.to_strings(),
    }
    ConfigLogUtils.log_config(output_dir / "logs" / "args.json", logs)



    with open(rate_file, "r") as rates_f:
        previous_rated_items = list_ext([json.loads(l) for l in rates_f])
        previous_rated_dois = previous_rated_items.to_dict_fn(lambda x: x["doi"], lambda x: x["rating"])
        # print(dois)

    # Rate new ones
    results = list_ext()
    for f_path in doi_files:
        f_path : Path
        if not f_path.is_file() or f_path.name == ".log.json":
            continue

        with open(f_path, "r") as f:
            results.append(json.load(f))
            # print(result)


    index = [0]
    def p(x, file: TextIOWrapper, index):
        index[0] += 1
        print(index)
        if x["DOI"] in previous_rated_dois:
            print(f"Skipping {x["DOI"]}")
            return

        print("-" * 100)

        article = db.get_doi(x["DOI"], selection="*")

        print(x["match"])
        print(x["DOI"])
        print(article[0])  # link
        print(x["title"])
        print(x["abstract"])
        rating = input("Rate from 0-9: ")
        file.write(
            "\n"
            + json.dumps({"doi": x["DOI"], "title": x["title"][0], "rating": rating})
        )

    with open(rate_file, "a+") as rates_f:
        results.for_each(lambda x: p(x, rates_f, index))


if __name__ == "__main__":
    main()
