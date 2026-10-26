from chalice import Chalice
import boto3

app = Chalice(app_name='serverless-app-sqs')


"""LAMBDA TO SQS SENSOR
Returns:
    _type_: _description_
"""
# Invoke this lambda function whenever a message
# is sent to the ``tweets-queue_serverless-app`` SQS queue.

@app.on_sqs_message(queue='tweets-queue_serverless-app')
def handler_sqs(event):
    for record in event:
        print("Message body: %s" % record.body)
        
        
# https://boto3.amazonaws.com/v1/documentation/api/latest/guide/sqs.html