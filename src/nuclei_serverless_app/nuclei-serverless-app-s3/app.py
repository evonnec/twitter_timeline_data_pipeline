from chalice import Chalice

app = Chalice(app_name='nuclei-serverless-app-s3')

"""LAMBDA TO S3 SENSOR
Returns:
    _type_: _description_
"""

# Whenever an object is uploaded to S3 bucket: 'twitter-media.nuclei-serverless-app'
# this lambda function will be invoked.

@app.on_s3_event(bucket='twitter-media.nuclei-serverless-app')
def handler_s3(event):
    print("Object uploaded for bucket: %s, key: %s"
          % (event.bucket, event.key))