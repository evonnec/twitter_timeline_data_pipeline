#!/bin/bash

S3_BUCKET=twitter-media.nuclei-serverless-app
#declare S3 bucket
printf "========S3_BUCKET declared as $S3_BUCKET========\n"

aws s3 mb s3://$S3_BUCKET
#make a S3 bucket in the same name of the variable set
printf "========aws make bucket command $S3_BUCKET========\n"

chalice package --merge-template resources.yaml out/
#create sam.yaml and additional artifacts in out/ folder
printf "========create sam.yaml in out/ folder========\n"

aws cloudformation package --template-file out/sam.yaml --s3-bucket $S3_BUCKET \
    --output-template-file out/packaged.yaml
#create a packaged.yaml from sam.yaml
printf "========create packaged.yaml from sam.yaml========\n"

aws cloudformation deploy --template-file out/packaged.yaml \
    --stack-name nuclei-serverless-app-dynamo --capabilities CAPABILITY_IAM
#deploy packaged.yaml as a stack
printf "========deploy out/packaged.yaml========\n"
