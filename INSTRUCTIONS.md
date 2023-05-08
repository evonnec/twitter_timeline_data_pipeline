
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

## Deploy the App
Use AWS Chalice as it supports YAML Templates
https://aws.amazon.com/blogs/developer/aws-chalice-now-supports-yaml-templates/


### Considerations and Options
To expand on this, there are a number of options to consider:
- AWS Lambda Layers with AWS Chalice: https://aws.amazon.com/blogs/developer/using-aws-lambda-layers-with-aws-chalice/
    - reports of some bugginess with respect to overwriting definitions and configurations.
- Deployment Options:
    - AWS SAM: Serverless Application Model https://github.com/aws/serverless-application-model and https://aws.amazon.com/serverless/sam/
    - Terraform
    - AWS CDK with Chalice https://aws.amazon.com/blogs/developer/aws-chalice-adds-support-for-the-aws-cdk/
- To extend Chalice behavior and customize how it processes requests by adding middleware: https://aws.amazon.com/blogs/developer/following-serverless-best-practices-with-aws-chalice-and-lambda-powertools/
- AWS Glue as an alternative: https://github.com/aws-samples/aws-glue-samples
- AWS CodeCatalyst as a CI build tool for the app: https://docs.aws.amazon.com/codecatalyst/latest/userguide/welcome.html