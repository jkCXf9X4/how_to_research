

import datetime
import os
from pathlib import Path


def main():
    cwd = Path(".")
    crossref_folder = Path("/media/eriro/storage/crossref/April 2023 Public Data File from Crossref/").resolve()

    crossref_files = [Path(i.path) for i in os.scandir(crossref_folder)]
    print(len(crossref_files))

    digits = 5
    for file in crossref_files:
        names = file.name.split(".")
        name = names[0]
        ext = ".".join(names[1:])
        nr_numbers = len(name)
        if nr_numbers < digits:
            new_name = f"{'0'*(digits-nr_numbers)}{name}"

            new_file = crossref_folder / f"{new_name}.{ext}"
            print(f"{name=} {new_name=} {ext=} {new_file}")
            # print (new_file)

            file.rename(crossref_folder / f"{new_name}.{ext}")





if __name__ == "__main__":
    main()