
import json
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
    input_dir = cwd / f"data/5_chatgpt_2024_04_29"


    output_dir = cwd / f"data/6_chatgpt_4_2024_04_29"
    output_dir.mkdir(parents=True, exist_ok=True)
    chatgpt_files = [input_dir / i.name for i in os.scandir(input_dir)]
    print(f"Total nr of files: {len(chatgpt_files)}")

    logs = {"input": input_dir, "output": output_dir, "chatgpt_input_files" : chatgpt_files}
    ConfigLogUtils.log_config(output_dir / "_log.json", logs)

    processed_chatgpt_files = [i.name for i in os.scandir(output_dir)]
    print(processed_chatgpt_files)

    for f_path in chatgpt_files:

        with open(f_path, "r") as f:

            result = json.load(f)
            doi = result["DOI"]

            if FileUtils.doi_filename(doi) in processed_chatgpt_files:
                print(f"Skipping {doi}")
            else:
                content = result["query"]["messages"][1]["content"]
                print(content)

                query = ChatGPTWrapper(content, model="gpt-4-turbo-2024-04-09")
                query.query()
                query.add_info(DOI=result["DOI"], title=result["title"], abstract=result["abstract"] )

                print(query.get_content())

                query.save_query(output_dir/  FileUtils.doi_filename(doi))


if __name__ == "__main__":
    main()

    