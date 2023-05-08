Nuclei Take home
================

Status
------

* What is working
    * If we follow the instructions below, we have a lambda service which creates a   DynamoDB table, and provides endpoints for modifying and retrieving data from that table
    * A local script which can collect tweet information 
    * Secrets management - we use the AWS secrets management service to store and provide the Twitter Bearer token
* What is work in progress
    * Push tweet data and media to AWS
* What is not yet started
    * Get all historic tweet information (not clear if this is a requirement)
    * Creating an SQS queue
    * Put tweet media into S3
    * Add tests
    * Some way of making sure that we don't add duplicate tweet information

Decisions made
--------------

Tooling:

* Chalice
    * I am familiar with Python and Flask, and this is a small and simple tool.
    * Chose to use ``chalice package`` as it seems like the simplest way to add resources using CloudFormation

## Deploy the App

### Create a virtual environment

* Run:
- ```python3 -m venv nuceli```  -- to create the python virtual environment  
- ```source nuclei/bin/activate```  -- to activate the python virtual environment  
  
Do ```pip install -e .```

### Create an S3 bucket

* Create an S3 bucket
    * I did this in the AWS CLI but if I had some time I would automate that in a different way

    ```
    aws s3 mb s3://twitter-media.nuclei-serverless-app
    ```

### Create the AWS stack

This currently brings up a Lambda service and a DyanmoDB table.

- ```chmod +x deploy.sh``` in `src/nuclei_serverless_app/nuclei-serverless-app-dynamo/`
- ```./deploy.sh``` to run the deploy
    - this will declare the S3 bucket
    - create `sam.yaml` and additional artifacts in out/ folder
    - change to out/ directory
    - create a `packaged.yaml` from `sam.yaml`
    - deploy `packaged.yaml` as a stack
- the stack is now live. 
- to bring down the stack do `chalice destroy`

### Create the AWS secret for the Twitter API token

We use the AWS Secrets Manager

```
TODO
```

### Run a script to fetch tweet data

Currently this is just a Python script that we run locally and prints data.
This is a proof of concept for the finished application which would send the tweet data to SQS/DynamoDB/S3.

```
python3 src/
```

## Test the App

In production I would like to use unit tests, maybe with a mock of AWS.
I used the `httpie` CLI for testing the REST endpoints.

### Considerations and Options

If I were to deploy this to production, I would look at the following options:

* AWS Lambda Layers
    * Simple deployment of multiple lambdas
* Deploy with an alternative to CloudFormation, e.g. using AWS CDK, AWS Glue or Terraform
* Use a CI system (e.g. CodeCatalyst) for testing and deployment
