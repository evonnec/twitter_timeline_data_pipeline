#!/bin/bash

aws cloudformation delete-stack --stack-name serverless-app-dynamo
printf "========delete out/packaged.yaml and cloudformation stack resources========\n"