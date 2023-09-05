## Corrputed Json to Postgres

This project convert `corropted-file.json` to valid json file and make flatten the json and write the flatten json to postgres with Spark and native python code.

The project include two Jupyter Notebooks. First one(`solution_1.ipynb`) use pyspark and second one(`solution_2.ipynb`) use native python codes. 

First Jupyter Notebooks: [solution_1.ipynb](./solution_1.ipynb)

Second Jupyter Notebooks: [solution_2.ipynb](./solution_2.ipynb)

> `scripts` folder includes python exports of solution_1, solution_2 jupyter notebooks.
> Python scripts: [solution_1](./scripts/solution_1.py), [solution_2](./scripts/solution_2.py)

## Installation

For postgreSQL run the following command in root project directory:

```
docker compose up -d
```

install the python modules:

```
pip install -r requirenmets.txt
```
