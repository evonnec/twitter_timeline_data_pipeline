from chalice import Chalice

app = Chalice(app_name='nuclei-serverless-app-sqs')


"""LAMBDA TO SQS SENSOR
Returns:
    _type_: _description_
"""
# Invoke this lambda function whenever a message
# is sent to the ``tweets-queue_nuclei-serverless-app`` SQS queue.

@app.on_sqs_message(queue='tweets-queue_nuclei-serverless-app')
def handler_sqs(event):
    for record in event:
        print("Message body: %s" % record.body)