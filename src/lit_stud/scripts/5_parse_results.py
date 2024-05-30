import os
from pathlib import Path
import re
import json
from sequence_extensions import list_ext
import shutil

from lit_stud.utils.config_log import ConfigLogUtils
from lit_stud.utils.os.files import FileUtils


def main():
    cwd = Path(".")
    data_dir = cwd / "data"
    step = "3.5"
    # step = "4"

    if step == "3.5":
        input_dir = data_dir / "4.0_chatgpt_2024_04_29"
        output_dir = data_dir / "5.0_results_2024_04_29"
    elif step == "4":
        input_dir = data_dir / "4.1_chatgpt_4_2024_04_29"
        output_dir = data_dir / "5.1_results_4_2024_04_29"

    shutil.rmtree(output_dir, ignore_errors=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"{input_dir=}")

    match_re = re.compile("Abstract fit: ([0-9]*)%", re.I)

    chatgpt_files = list_ext([input_dir / i.name for i in os.scandir(input_dir)])
    print(f"Total nr of files: {len(chatgpt_files)}")

    logs = {
        "input": input_dir,
        "output": output_dir,
        "chatgpt_files": chatgpt_files.to_strings(),
    }
    ConfigLogUtils.log_config(output_dir / "logs" / "args.json", logs)

    results = list_ext()

    for f_path in chatgpt_files:
        f_path : Path
        # print(f_path)
        if not f_path.is_file():
            continue

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
            # print(percentage)
            results.append(result)

    results.sort(key=lambda x: x["match"], reverse=True)

    results = results.filter(lambda x: x["match"] >= 80)
    print(f"After filtering, len:{len(results)}")

    # results.for_each(lambda x: print(x["match"]))

    def save(x):
        with open(output_dir / FileUtils.doi_filename(x["DOI"]), "w") as f:
            # print(f)
            json.dump(x, f, indent=4)

    results.for_each(lambda x: save(x))


if __name__ == "__main__":
    main()
