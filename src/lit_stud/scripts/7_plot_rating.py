import os
from pathlib import Path
import json
from matplotlib import pyplot as plt
import pandas as pd
from sequence_extensions import list_ext

from lit_stud.utils.config_log import ConfigLogUtils
from lit_stud.utils.os.files import FileUtils



cwd = Path("/media/eriro/storage/pwa/2_work/how_to_research/")
data_dir = cwd / "data"

input_dir_3_5 = data_dir / "5.0_results_2024_04_29"
input_dir_4 = data_dir / "5.1_results_4_2024_04_29"

rate_file = data_dir / "6_rate_2024_04_29" / "rate.json"

output_dir = data_dir / "7_rate_2024_04_29"

print(f"{input_dir_3_5=}")
print(f"{input_dir_4=}")



# find files to parse
doi_3_5_files = list_ext(
    [input_dir_3_5 / i.name for i in os.scandir(input_dir_3_5)]
)
print(f"Total nr of chatgpt 3.5 files: {len(doi_3_5_files)}")

doi_4_files = list_ext(
    [input_dir_4 / i.name for i in os.scandir(input_dir_4)]
)
print(f"Total nr of chatgp4 4files: {len(doi_4_files)}")

logs = {
    "rate_file": rate_file,
    "input_dir_3_5" : input_dir_3_5,
    "input_dir_4" : input_dir_4,
    "output_dir": output_dir,
    "chatgpt_3_5_files": doi_3_5_files.to_strings(),
    "chatgpt_4_files": doi_4_files.to_strings(),
}
ConfigLogUtils.log_config(output_dir / "logs" / "args.json", logs)

with open(rate_file, "r") as f:
    df = pd.read_json(f.read(), lines=True)

print(df.head(10))



def get_dois(files):
    dois = {}
    for f_path in files:
        f_path: Path
        if not f_path.is_file() or f_path.name == ".log.json":
            continue

        with open(f_path, "r") as f:
            answer = json.load(f)
            dois[answer["DOI"]] = answer["match"]
    return dois

gpt_3_5_dois = get_dois(doi_3_5_files)
gpt_4_dois = get_dois(doi_4_files)

# print(len(gpt_3_5_dois))
# print(len(gpt_4_dois))

# print(gpt_3_5_dois[:10])
# print(gpt_4_dois[:10])

df["chatgpt35"] = df.apply(lambda x: x["doi"] in gpt_3_5_dois, axis=1)
df["chatgpt35_match"] = df.apply(lambda x: int(gpt_3_5_dois[x["doi"]]), axis=1)

df["chatgpt4"] = df.apply(lambda x: x["doi"] in gpt_4_dois, axis=1)

def extract_match(x):
    if x["chatgpt4"]:
        return int(gpt_4_dois[x["doi"]])
    else:
        return None

df["chatgpt4_match"] = df.apply(lambda x: extract_match(x), axis=1)
print(df.head())

df["only_gpt35"] = df.apply(lambda x: x["chatgpt35"] and not x["chatgpt4"], axis=1)

df["rating"] = df.apply(lambda x: x["rating"]*10, axis=1)

print(df.describe())

# for index, row in df.iterrows():
#     print(row)


# df_pivot = pd.pivot_table(df, values=["chatgpt35", "only_gpt35", "chatgpt4"], columns=["rating"], aggfunc="sum")
df_pivot = pd.pivot_table(df, index="rating", values=["chatgpt4", "only_gpt35"], aggfunc="sum")

print(df_pivot.head())
# print(df_pivot.columns)




ax = df_pivot.plot.barh(figsize=(10,7),title='Rating of articles')

# ax.set_xscale("log")

# fig.show()
plt.savefig(output_dir /"test")