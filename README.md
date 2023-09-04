## Corrputed Json to Postgres

This project convert `corropted-file.json` to valid json file and make flatten the json and write the flatten json to postgres with Spark and native python code.

The project include two Jupyter Notebooks and 2 python files. First Jupyter Notebook(`solution_1.ipynb`) use pyspark and second Jupyter Notebook(`solution_2.ipynb`) use native python codes. First python script(`solution_1.py`) is export of `solution_1.ipynb` and second python script(`solution_2.py`) is export of `solution_2.ipynb`.

Jupyter Notebooks: [solution_1.ipynb](./solution_1.ipynb), [solution_2.ipynb](./solution_2.ipynb)

Python scripts: [solution_1.py](./solution_1.py), [solution_2.py](./solution_2.py)

## Installation

For postgreSQL run the following command in root project directory:

```
docker compose up -d
```

install the python modules:

```
pip install -r requirenmets.txt
```
