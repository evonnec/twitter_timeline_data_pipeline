#!/bin/bash

BEARER_TOKEN=""

aws secretsmanager create-secret --name prod/Twitter/Api
--description "Twitter API Bearer Token"
--secret-string '{"twitter_access_token":"$(BEARER_TOKEN)"}' --region us-east-1