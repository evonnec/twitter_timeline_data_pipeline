#!/bin/bash
S3_BUCKET=twitter-media.nuclei-serverless-app
aws s3 mb s3://$S3_BUCKET
#declare S3 bucket
chalice package --merge-template resources.yaml out/
#create sam.yaml and additional artifacts in out/ folder
cd out/
#change to out/ director
aws cloudformation package --template-file sam.yaml --s3-bucket $S3_BUCKET \
    --output-template-file packaged.yaml
#create a packaged.yaml from sam.yaml
aws cloudformation deploy --template-file packaged.yaml \
    --stack-name nuclei-serverless-app-dynamo --capabilities CAPABILITY_IAM
#deploy packaged.yaml as a stack
