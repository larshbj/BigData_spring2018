# BigData_spring2018

## Setup

```bash
#python3 setup.py install

pip3 install pyspark


spark-submit task1.py
```

### Jupyter notebook

Fix git
```bash
echo *.ipynb  filter=clean_ipynb > .git/info/attributes

#Add to .git/config
[filter "clean_ipynb"]
    clean = jq '{ cells: [.cells[] | . + { metadata: {} } + if .cell_type == \"code\" then { outputs: [], execution_count: null } else {} end ] } + delpaths([[\"cells\"]])'
    smudge = cat
```

```bash
pip install jupyter

# To run
jupyter-notebook
```
