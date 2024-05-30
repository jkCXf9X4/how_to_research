

Use Crossref offline db to find abstracts

1. rename all files in crossref using ./src/lit_stud/utils/script/rename_crossref.py

it will add zeros in the beginning of all names so that they are all equal length

2. run ./scripts/create_env.sh
3. activate the env, using ./scripts/set_env.sh

Then find the src/lit_stud/scripts folder and start going thru the steps.


# src/lit_stud/scripts/1_extract_crossref.py
broad search using a few keywords, about 10h

# src/lit_stud/scripts/2_count_ref.py 
narrow search using multiple keywords, about 1h

# src/lit_stud/scripts/3_create_db.py
create a db file of the results -  about 2min

# src/lit_stud/scripts/4.0_evaluate.py 
evaluate using ChatGPT 3.5 - 0.5-2h

# src/lit_stud/scripts/4.1_evaluate_with_4.py
evaluate using ChatGPT 4 - 1-4h

# src/lit_stud/scripts/5_parse_results.py
Extract match from chatgpt answers and save the results

# src/lit_stud/scripts/6_rate_abstractions.py
Read shit and rate the answers