
import os
from pathlib import Path
import duckdb

from lit_stud.utils.config_log import ConfigLogUtils
from lit_stud.utils.crossref_db import CrossrefJson
from lit_stud.utils.keywords import KeywordGroup, KeywordGroups


def main():
    cwd = Path(".")
    input_dir = cwd / f"data/1_json_2024_04_23"
    output_dir = cwd / f"data/2_json_2024_04_29"

    print(f"{input_dir=} \n {output_dir=}")

    groups = KeywordGroups()
    groups.append(KeywordGroup("sim", ["simulating", "simulator", "simulate"], weight=1))
    groups.append(KeywordGroup("model", ["model", "modeling"], weight=1))
    groups.append(KeywordGroup("intended_use", ["intended use", "intended-use"], weight=1))
    groups.append(KeywordGroup("verify", ["verification", "verify", "validation", "validate"], weight=1))
    groups.append(KeywordGroup("uncertain", ["uncertainty", "uncertainty quantification", "sensitivity analysis", "sensitivity"], weight=1))
    groups.append(KeywordGroup("complex", ["complex system"], weight=1))
    groups.append(KeywordGroup("pbs", ["physics-based simulation"], weight=1))
    groups.append(KeywordGroup("cps", ["Cyber-Physical Systems", "cps"], weight=1))
    groups.append(KeywordGroup("cba", ["cba", "certification by analysis"], weight=1))
    groups.append(KeywordGroup("twin", ["digital twin", "digital shadow"], weight=1))
    groups.append(KeywordGroup("cosim", ["co-simulation", "cosimulation"], weight=1))
    groups.append(KeywordGroup("fidelity", ["fidelity"], weight=1))
    groups.append(KeywordGroup("method", ["methodology"], weight=0.5))
    groups.append(KeywordGroup("lls", ["large-scale simulator", "large-scale simulation"], weight=1))
    groups.append(KeywordGroup("mbse", ["mbse", "model-based system engineering"], weight=1))
    groups.append(KeywordGroup("hil", ["hardware-in-the-loop", "hil", "software-in-the-loop", "sil"], weight=1))
    groups.append(KeywordGroup("rts", ["real-time simulators", "rts"], weight=1))

    print(f"{groups=}")

    logs = {"input": input_dir, "output": output_dir, "groups": groups}
    ConfigLogUtils.log_config(output_dir / "_log.json", logs)

    crossref_files = [Path(i.path) for i in os.scandir(input_dir)]
    nr_files = len(crossref_files)
    print(nr_files)

    steps = CrossrefJson.get_patterns(nr_files, step=10)

    steps = steps[16:]
    print(steps)
    # exit()

    def count_keys(text : str) -> int:
        return round(groups.evaluate_keywords(text))

    def extract_files(pattern):
        with duckdb.connect() as c:
            CrossrefJson.set_defaults(c)

            file = input_dir / f"{pattern}*.json.gz"
            CrossrefJson.import_jsons(c, file, select = "*, lower(abstract)")
            c.sql("""ALTER TABLE db RENAME "lower(abstract)" TO  abstract_low """)

            # c.sql("DESCRIBE db").show()
            # c.sql("SELECT count(*) from db").show()

            c.create_function("count_keys", count_keys)

            c.sql("""CREATE OR REPLACE TABLE db AS SELECT *, count_keys(abstract_low) FROM db""")
            c.sql("""ALTER TABLE db RENAME "count_keys(abstract_low)" TO  count_keys """)
            # print(hits)

            # c.sql("DESCRIBE db").show()
            # c.sql("SELECT count(*) from db").show()
            c.sql("SELECT count(*) from db WHERE count_keys>4").show()
            # c.sql("SELECT abstract, count_keys from db ORDER BY count_keys DESC").show()


            out_file = output_dir / f"{pattern}.json.gz"
            CrossrefJson.export_json(c, "SELECT * FROM db WHERE count_keys>4", out_file)

    for s in steps:
        extract_files(s)

    # CrossrefJson.pool(extract_files, steps)


if __name__ == "__main__":
    main()