
##### To activate the virtual environment

* Open the candidate-data-pipeline-engineer-evonne-cho folder in VSCode
* Open the terminal in VSCode
* Run:
```conda deactivate```
```python3 -m venv nuceli``` -- to create the python virtual environment
```source nuclei/bin/activate``` -- to activate the python virtual environment

```pip install -e .```  -- to install the dependencies from pyproject.toml
  
- `-e` is editable. This is so that we can do `import nuclei_serverless_app`

- Note: this does not support historic tweets from long before we started

##### To run tests

pytest -s tests/example.py

##### Or for a web app...

pytest -s example_flask.py
