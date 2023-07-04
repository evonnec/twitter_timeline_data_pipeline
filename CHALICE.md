from chalice import Chalice, Rate

"""REST

Returns:
    _type_: _description_
"""
app = Chalice(app_name="restapi")

@app.route("/")
def index():
    return {"hello": "world"}



"""SCHEDULE -- uses Rate

Returns:
    _type_: _description_
"""

app = Chalice(app_name="scheduler5min")

# Automatically runs every 5 minutes
@app.schedule(Rate(5, unit=Rate.MINUTES))
def periodic_task(event):
    return {"hello": "world"}

# @app.route('/')
# def index():
#     return {'hello': 'world'}


# The view function above will return {"hello": "world"}
# whenever you make an HTTP GET request to '/'.
#
# Here are a few more examples:
#
# @app.route('/hello/{name}')
# def hello_name(name):
#    # '/hello/james' -> {"hello": "james"}
#    return {'hello': name}
#
# @app.route('/users', methods=['POST'])
# def create_user():
#     # This is the JSON body the user sent in their POST request.
#     user_as_json = app.current_request.json_body
#     # We'll echo the json body back to the user in a 'user' key.
#     return {'user': user_as_json}
#
# See the README documentation for more examples.
#


botocore.errorfactory.InvalidParameterValueException:
 An error occurred (InvalidParameterValueException) when calling the 
 CreateEventSourceMapping operation: Queue visibility timeout: 30 seconds is 
 less than Function timeout: 60 seconds

    raise ChaliceDeploymentError(e)
chalice.deploy.deployer.ChaliceDeploymentError: ERROR - While deploying your chalice application, received the following error:

 An error occurred (InvalidArgument) when calling the 
 PutBucketNotificationConfiguration operation: Configurations overlap. 
 Configurations on the same bucket cannot share a common event type.


 


          