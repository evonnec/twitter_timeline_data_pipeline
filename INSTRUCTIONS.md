Take Home: Serverless App
================

Status
------

* What is working
    * If we follow the instructions below, we have a lambda service which 
        * creates a DynamoDB Table with a schema of `id: metadata`
        * provides endpoints for modifying and retrieving data from that table
    * A local python script which can use the bearer token provided to 
        * call the Twitter API, 
        * request the last tweets from a user's timeline
        * collect tweet information from each of those last tweets from a user's timeline
        * take in a user as an argument
        * Push tweet media if it exists to S3
    * Provides a way to store the Twitter API bearer token in AWS Secrets Manager such that it can be pulled by the service created
* What is currently work in progress
    * Push tweet metadata
    * Push to SQS queue
* What is not yet started
    * Get all historic tweet information (unclear if this is a requirement)
    * Put Tweet Media into S3 from SQS Queue
    * Add tests
    * Some way of making sure that we don't add duplicate tweet information

Decisions made
--------------

Tooling:

* Chalice
    * Chose to use ``chalice package`` as it seems like the simplest way to add resources using CloudFormation
* python: requests and python-twitter-v2 used. not enough time to figure out attachments, includes, media from python-twitter-v2.
* toml for easy virtual env set up

# Deploy the App

## Create a virtual environment

* Run:
- ```python -m venv twitterpipeline```  -- to create the python virtual environment  
- ```source twitterpipeline/bin/activate```  -- to activate the python virtual environment  
  
Do ```pip install -e .```

Do ```aws configure``` to set up AWS credentials

## Create the AWS secret for the Twitter API token

We do this by creating the bearer token secret in AWS Secrets Manager.  
Go to `create_bearer_token_secret.sh` and export the bearer token in the terminal or enter into the script prior to running.

## Create the AWS stack

This currently:
- brings up a Lambda service
- a DyanmoDB table with GET and PUT endpoints
- makes a S3 bucket

- ```chmod +x deploy.sh``` in `src/serverless_app/serverless-app-dynamo/`
- ```./deploy.sh``` to run the deploy of the app
    - this will declare the S3 bucket
    - make bucket for the declared S3 bucket
    - create `sam.yaml` and additional artifacts in `out/` folder
    - create a `packaged.yaml` from `sam.yaml`
    - deploy `packaged.yaml` as a stack
- the stack is now live. 

- to bring down the stack do:
```chmod +x delete.sh``` in `src/serverless_app/serverless-app-dynamo/`
- ```./delete.sh``` to delete resources of the app

TODO:
- logs can be used to set up CloudWatch

## Run the script to fetch tweet data

Currently this is a simple Python script that we run locally and prints data.

This is a proof of concept for the finished application which would send the tweet data to SQS/DynamoDB/S3.

TODO: To separate out the SQS Queue, the Dynamo DB PUT, the S3 upload object, so that each lambda has its Python instructions

```
python3 src/serverless_app/get_twitter_info.py --username 'who'
```

## Test the App

To develop, I manually tested the endpoints. Next, write these as well as other functional tests as unit tests, with a mock deploy of AWS.

I used the library `httpie` as a CLI for testing the REST endpoints.

## Considerations and Options

If I were to deploy this to production, I would look at the following options:

* AWS Lambda Layers
    * Simple deployment of multiple lambdas
* Deploy with an alternative to CloudFormation, e.g. using AWS CDK, AWS Glue or Terraform
* Use a CI system (e.g. CodeCatalyst) for testing and deployment
