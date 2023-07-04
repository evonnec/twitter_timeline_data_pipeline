CloudWatch Events
You can configure a lambda function to subscribe to any CloudWatch Event <https://amzn.to/2SCgWA6>.

To subscribe to a CloudWatch Event in chalice, you use the @app.on_cw_event() decorator. Let’s look at an example.

app = chalice.Chalice(app_name='foo')

@app.on_cw_event({"source": ["aws.codecommit"]})
def on_code_commit_changes(event):
    print(event.to_dict())
In this example, we have a single lambda function that we subscribe to all events from the AWS Code Commit service. The first parameter to the decorator is the event pattern that will be used to filter the events sent to the function.

See the CloudWatch Event pattern docs for additional syntax and examples https://amzn.to/2OlqZso

The function you decorate must accept a single argument, which will be of type CloudWatchEvent.