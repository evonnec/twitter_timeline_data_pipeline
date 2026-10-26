#!/bin/bash

API_URL=$(aws cloudformation describe-stacks --stack-name serverless-app-dynamo \
   --query "Stacks[0].Outputs[?OutputKey=='EndpointURL'].OutputValue | [0]")
#get the API URL created by the deploy of the stack
#ID=

#from Twitter API call, retrieve the tweet ID
#VALUE=

#from Twitter API call, retrieve the tweet metadata json blob

# echo '{"metadata": $VALUE}' | http PUT $API_URL/item/$ID
#put the information into the dynamo DB table

